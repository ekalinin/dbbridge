package db

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// stubDriver records the arguments Open was called with, so a test can check
// that OpenPool passes the DSN and the connection limit through untouched.
type stubDriver struct {
	dsn      string
	maxConns int
	pool     Pool
}

func (d *stubDriver) Open(_ context.Context, dsn string, maxConns int) (Pool, error) {
	d.dsn, d.maxConns = dsn, maxConns
	return d.pool, nil
}

// constDriver is the stateless variant, for the concurrency test: its readers
// would otherwise race on stubDriver's recorder fields rather than on the
// registry under test.
type constDriver struct{}

func (constDriver) Open(context.Context, string, int) (Pool, error) { return stubPool{}, nil }

// stubPool is comparable on purpose: a test asserts that OpenPool hands back
// exactly the pool its driver returned, once the tracing wrapper is off it.
type stubPool struct{}

func (stubPool) Exec(context.Context, string) (RowStream, error) { return nil, nil }
func (stubPool) Ping(context.Context) error                      { return nil }
func (stubPool) Stat() PoolStat                                  { return PoolStat{} }
func (stubPool) Close() error                                    { return nil }

func TestOpenPoolUsesTheRegisteredDriver(t *testing.T) {
	want := stubPool{}
	d := &stubDriver{pool: want}
	Register("stub", d)

	got, err := OpenPool(context.Background(), "stub", "stub://host/db", 7)
	if err != nil {
		t.Fatalf("OpenPool: %v", err)
	}
	if Unwrap(got) != Pool(want) {
		t.Errorf("OpenPool returned %#v, want the pool the driver produced", got)
	}
	if d.dsn != "stub://host/db" || d.maxConns != 7 {
		t.Errorf("driver.Open got (%q, %d), want (\"stub://host/db\", 7)", d.dsn, d.maxConns)
	}
}

// TestOpenPoolUnknownEngine: an engine no driver registered is a configuration
// mistake, and the message has to name it - it is what an operator sees when a
// blank import is missing from main.go.
func TestOpenPoolUnknownEngine(t *testing.T) {
	pool, err := OpenPool(context.Background(), "nosuchengine", "dsn", 1)
	if err == nil {
		t.Fatal("OpenPool with an unregistered engine returned no error")
	}
	if pool != nil {
		t.Errorf("OpenPool returned pool %#v alongside the error", pool)
	}
	if !strings.Contains(err.Error(), `"nosuchengine"`) {
		t.Errorf("error = %q, want the engine name quoted in it", err)
	}
}

func TestRegisterNilDriverPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("Register with a nil driver did not panic")
		}
	}()
	Register("nil-driver", nil)
}

// TestRegisterDuplicatePanics: drivers register from init, so a second
// registration under the same name means two packages disagree about which
// implementation an engine maps to. Failing at startup beats resolving it by
// import order.
func TestRegisterDuplicatePanics(t *testing.T) {
	Register("duplicate", &stubDriver{})

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("registering the same engine twice did not panic")
		}
		if msg, _ := r.(string); !strings.Contains(msg, "duplicate") {
			t.Errorf("panic = %v, want the engine name in it", r)
		}
	}()
	Register("duplicate", &stubDriver{})
}

// TestRegistryIsConcurrencySafe covers the mutex the registry is built around:
// drivers are registered from init while OpenPool reads the map from every
// query goroutine. Only meaningful under -race (make test-race).
func TestRegistryIsConcurrencySafe(t *testing.T) {
	Register("concurrent-read", constDriver{})

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			Register(fmt.Sprintf("concurrent-write-%d", i), constDriver{})
		}()
		go func() {
			defer wg.Done()
			if _, err := OpenPool(context.Background(), "concurrent-read", "dsn", 1); err != nil {
				t.Errorf("OpenPool: %v", err)
			}
		}()
	}
	wg.Wait()
}
