package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	v1 "github.com/ekalinin/dbbridge/internal/gen/dbbridge/v1"
	"github.com/ekalinin/dbbridge/internal/gen/dbbridge/v1/dbbridgev1connect"
	"github.com/ekalinin/dbbridge/internal/lifecycle"
	"github.com/ekalinin/dbbridge/internal/state"

	"connectrpc.com/connect"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "dbbridge.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("failed to write the config: %v", err)
	}
	return path
}

func closeStepNames(a *app) []string {
	names := make([]string, 0, len(a.closeSteps()))
	for _, step := range a.closeSteps() {
		names = append(names, step.what)
	}
	return names
}

// The storage registry is process-wide and panics on a second registration of
// the same backend, so exactly one test in this package may build the app all
// the way through. This one covers the whole run: what newApp wires, what the
// shutdown sequence does and in which order Close releases it.
func TestApp_WiresAndShutsDown(t *testing.T) {
	cfgPath := writeConfig(t, `
instance:
  id: test-instance
  metastore: memory
  default_storage: fs
server:
  rest_addr: 127.0.0.1:0
  grpc_addr: 127.0.0.1:0
  admin_addr: 127.0.0.1:0
storage:
  fs:
    root: `+t.TempDir()+`
`)

	a, err := newApp(cfgPath)
	if err != nil {
		t.Fatalf("newApp failed: %v", err)
	}
	defer a.Close()

	if a.metaStore == nil || a.qm == nil || a.svc == nil {
		t.Fatal("newApp left a core component unwired")
	}
	if a.restHTTP == nil || a.grpcHTTP == nil {
		t.Fatal("newApp left a listener unwired")
	}
	// server.admin_addr is set, so /metrics and /v1/admin/* get their own server.
	if a.adminHTTP == nil {
		t.Fatal("newApp did not build the separate admin server")
	}
	if a.tlsCerts != nil {
		t.Fatal("newApp loaded a TLS key pair that was not configured")
	}

	// Telemetry is set up even with no OTLP endpoint - the exporter is the
	// no-op one - so its shutdown is part of the cleanup either way, last.
	want := []string{"QueryManager close", "FS storage close", "MetaStore close", "OpenTelemetry shutdown"}
	if got := closeStepNames(a); !slices.Equal(got, want) {
		t.Errorf("cleanup order = %v, want %v", got, want)
	}

	// Every configured listener actually binds and serves its own handler. The
	// addresses are ":0" in the config, so the ones srv.Addr holds after serve()
	// are what the kernel assigned.
	if err := a.serve(); err != nil {
		t.Fatalf("serve: %v", err)
	}
	assertServes(t, "REST", "http://"+a.restHTTP.Addr+"/healthz")
	assertServes(t, "admin", "http://"+a.adminHTTP.Addr+"/metrics")
	assertConnectServes(t, "http://"+a.grpcHTTP.Addr)

	// The signal loop, driven by real signals: SIGHUP reloads and keeps serving,
	// SIGTERM runs the drain and returns. Registering a guard channel first is
	// what makes this safe - once any handler exists for a signal Go stops
	// applying the default action, so a SIGHUP that arrives before awaitSignals
	// has called signal.Notify cannot kill the test binary.
	guard := make(chan os.Signal, 2)
	signal.Notify(guard, syscall.SIGHUP, syscall.SIGTERM)
	defer signal.Stop(guard)

	returned := make(chan struct{})
	go func() {
		a.awaitSignals()
		close(returned)
	}()

	// A change the reload can actually apply: defaults are re-read from the file,
	// and unlike a database it needs no reachable backend.
	if err := os.WriteFile(cfgPath, []byte(`
instance:
  id: test-instance
  metastore: memory
  default_storage: fs
server:
  rest_addr: 127.0.0.1:0
  grpc_addr: 127.0.0.1:0
  admin_addr: 127.0.0.1:0
defaults:
  result_ttl: 72h
storage:
  fs:
    root: `+t.TempDir()+`
`), 0o600); err != nil {
		t.Fatalf("rewrite the config: %v", err)
	}

	sendSignal(t, syscall.SIGHUP)
	waitFor(t, "the reloaded result_ttl", func() bool {
		return a.qm.GetConfig().Defaults.ResultTTL == 72*time.Hour
	})
	if got := a.lm.GetState(); got != lifecycle.StateServing {
		t.Errorf("lifecycle state after SIGHUP = %q, want %q: a reload must not drain", got, lifecycle.StateServing)
	}

	sendSignal(t, syscall.SIGTERM)
	select {
	case <-returned:
	case <-time.After(45 * time.Second):
		t.Fatal("awaitSignals did not return after SIGTERM")
	}
	if got := a.lm.GetState(); got != lifecycle.StateStoppable {
		t.Errorf("lifecycle state after shutdown = %q, want %q", got, lifecycle.StateStoppable)
	}
}

// sendSignal delivers a signal to this process. Handlers are already registered
// by the caller, so the default action never applies.
func sendSignal(t *testing.T, sig syscall.Signal) {
	t.Helper()
	if err := syscall.Kill(syscall.Getpid(), sig); err != nil {
		t.Fatalf("send %v: %v", sig, err)
	}
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestNewApp_ReportsAConfigFailure(t *testing.T) {
	a, err := newApp(filepath.Join(t.TempDir(), "missing.yaml"))
	if err == nil {
		a.Close()
		t.Fatal("newApp accepted a config file that does not exist")
	}
	if !strings.Contains(err.Error(), "failed to initialize config manager") {
		t.Errorf("error = %v, want it to name the config manager", err)
	}
}

// A storage backend that cannot be built fails the start instead of exiting
// from inside the constructor, and what was already wired is released.
func TestNewApp_ReportsAStorageFailure(t *testing.T) {
	notADir := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(notADir, nil, 0o600); err != nil {
		t.Fatalf("failed to write the file: %v", err)
	}
	cfgPath := writeConfig(t, `
instance:
  id: test-instance
  metastore: memory
  default_storage: fs
storage:
  fs:
    root: `+filepath.Join(notADir, "results")+`
`)

	a, err := newApp(cfgPath)
	if err == nil {
		a.Close()
		t.Fatal("newApp accepted an unusable FS storage root")
	}
	if !strings.Contains(err.Error(), "failed to initialize FS storage") {
		t.Errorf("error = %v, want it to name the FS storage", err)
	}
}

func TestApp_CloseSkipsWhatWasNeverBuilt(t *testing.T) {
	a := &app{metaStore: state.NewMemoryMetaStore()}

	want := []string{"MetaStore close"}
	if got := closeStepNames(a); !slices.Equal(got, want) {
		t.Fatalf("cleanup order = %v, want %v", got, want)
	}
	a.Close()
}

// assertServes checks that a listener answers 200 on a plain GET.
func assertServes(t *testing.T, name, url string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s (%s listener): %v", url, name, err)
	}
	defer resp.Body.Close()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		t.Fatalf("read %s body: %v", name, err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("%s listener: GET %s = %d, want 200", name, url, resp.StatusCode)
	}
}

// assertConnectServes checks that the gRPC listener is serving the Connect
// handler over cleartext HTTP/2, which is what it falls back to without TLS.
func assertConnectServes(t *testing.T, baseURL string) {
	t.Helper()
	tr := &http.Transport{}
	tr.Protocols = new(http.Protocols)
	tr.Protocols.SetUnencryptedHTTP2(true)
	client := dbbridgev1connect.NewQueryServiceClient(&http.Client{Transport: tr}, baseURL)

	resp, err := client.CanIBeStopped(context.Background(), connect.NewRequest(&v1.CanIBeStoppedRequest{}))
	if err != nil {
		t.Fatalf("CanIBeStopped over the gRPC listener: %v", err)
	}
	if !resp.Msg.CanBeStopped {
		t.Errorf("can_be_stopped = false on an idle instance, want true")
	}
}

// TestServeOne_ReportsABindFailure covers the synchronous bind. A port already
// in use used to surface from inside the serving goroutine, where log.Fatalf
// takes the process down without running Close - the same failure mode
// loadTLSCerts was moved out of the goroutine to avoid.
func TestServeOne_ReportsABindFailure(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer busy.Close()

	a := &app{}
	err = a.serveOne("REST API", &http.Server{Addr: busy.Addr().String(), ReadHeaderTimeout: time.Second})
	if err == nil {
		t.Fatal("serveOne accepted an address that is already in use")
	}
	if !strings.Contains(err.Error(), "REST API") {
		t.Errorf("error = %v, want it to name the listener", err)
	}
}
