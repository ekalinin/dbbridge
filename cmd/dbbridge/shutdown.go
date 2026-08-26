package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ekalinin/dbbridge/internal/lifecycle"
)

// awaitSignals blocks until the process is asked to stop. SIGHUP reloads the
// configuration and keeps serving; SIGINT and SIGTERM run the shutdown
// sequence described in docs/readiness-and-draining.md.
func (a *app) awaitSignals() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

	for {
		sig := <-sigCh
		if sig == syscall.SIGHUP {
			a.reload()
			continue
		}
		a.runShutdown(sig)
		return
	}
}

func (a *app) reload() {
	log.Println("Received SIGHUP, reloading configuration...")
	report, err := a.qm.Reload()
	if err != nil {
		log.Printf("ERROR: Failed to reload config: %v", err)
		return
	}
	log.Printf("Configuration reloaded successfully (added=%v removed=%v updated=%v)",
		report.Added, report.Removed, report.Updated)
}

// runShutdown is what a SIGINT or a SIGTERM starts: stop admitting queries,
// wait for the ones already running, then close the listeners.
func (a *app) runShutdown(sig os.Signal) {
	log.Printf("Received signal %v, starting graceful shutdown / draining...", sig)
	a.drain()
	a.stopServers()
	log.Println("dbbridge stopped.")
}

// drain closes admission and waits up to 30s for the queries this node owns.
func (a *app) drain() {
	a.lm.SetState(lifecycle.StateDraining)
	// Close admission in the manager as well: the lifecycle flag is checked
	// by the service before it calls SubmitQuery, which leaves a window in
	// which a query can still register after the loop below sees zero (I5).
	a.qm.Drain()

	// Wait until all queries on this node are finished
	shutdownDeadline := time.Now().Add(30 * time.Second)
	for {
		inFlight := a.qm.CountInFlight(context.Background())
		// Publishes the DRAINING -> STOPPABLE transition, which is what
		// /v1/admin/can-stop reports to the orchestrator (spec §10).
		a.lm.Advance(inFlight)
		if inFlight == 0 {
			log.Println("0 owned active queries remaining. Safe to stop.")
			return
		}
		if time.Now().After(shutdownDeadline) {
			log.Printf("Shutdown deadline exceeded, forcing stop with %d queries still active", inFlight)
			return
		}
		log.Printf("Waiting for %d active queries to complete...", inFlight)
		time.Sleep(1 * time.Second)
	}
}

// stopServers closes the listeners, sharing a 10s budget between them.
func (a *app) stopServers() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, srv := range []struct {
		name string
		http *http.Server
	}{
		{"REST", a.restHTTP},
		{"gRPC", a.grpcHTTP},
		{"admin", a.adminHTTP},
	} {
		if srv.http == nil {
			continue
		}
		if err := srv.http.Shutdown(ctx); err != nil {
			log.Printf("ERROR: %s server shutdown failed: %v", srv.name, err)
		}
	}
}
