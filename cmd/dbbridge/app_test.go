package main

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"

	"github.com/ekalinin/dbbridge/internal/lifecycle"
	"github.com/ekalinin/dbbridge/internal/state"
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

	// The drain sequence, without the signal that normally starts it.
	a.runShutdown(syscall.SIGTERM)
	if got := a.lm.GetState(); got != lifecycle.StateStoppable {
		t.Errorf("lifecycle state after shutdown = %q, want %q", got, lifecycle.StateStoppable)
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
