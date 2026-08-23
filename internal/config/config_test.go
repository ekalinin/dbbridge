package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestConfigLoadAndValidate(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "dbbridge-config-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	validYAML := `
instance:
  id: test-instance
  metastore: memory
server:
  rest_addr: ":18080"
  grpc_addr: ":19090"
defaults:
  result_ttl: 1h
databases:
  - id: test_db
    engine: postgres
    dsn: "postgres://test"
`
	configPath := filepath.Join(tempDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(validYAML), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	mgr, err := NewManager(configPath)
	if err != nil {
		t.Fatalf("expected no error loading valid config; got %v", err)
	}

	cfg := mgr.Get()
	if cfg.Instance.ID != "test-instance" {
		t.Errorf("expected instance ID 'test-instance'; got %q", cfg.Instance.ID)
	}
	if cfg.Defaults.ResultTTL != 1*3600*1e9 { // 1h in nanoseconds
		t.Errorf("expected default ResultTTL 1h; got %v", cfg.Defaults.ResultTTL)
	}
}

func TestConfigValidationErrors(t *testing.T) {
	invalidYAML := `
instance:
  id: ""
  metastore: invalid
`
	tempDir, err := os.MkdirTemp("", "dbbridge-config-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	configPath := filepath.Join(tempDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(invalidYAML), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	_, err = NewManager(configPath)
	if err == nil {
		t.Error("expected error loading invalid config; got nil")
	}
}

// TestConfigRejectsEmptyAuthTokens: an auth section that resolves to no tokens
// would start the process with the API open while the operator believes it is
// protected.
func TestConfigRejectsEmptyAuthTokens(t *testing.T) {
	const body = `
instance:
  id: test
  metastore: memory
auth:
  tokens: []
databases: []
`
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if _, err := NewManager(path); err == nil {
		t.Error("an empty auth.tokens list was accepted")
	}
}

func TestConfigDiffDatabases(t *testing.T) {
	oldCfg := &Config{
		Databases: []DatabaseConfig{
			{ID: "db1", Engine: "postgres", DSN: "dsn1", MaxConns: 5},
			{ID: "db2", Engine: "mysql", DSN: "dsn2", MaxConns: 5},
		},
	}

	newCfg := &Config{
		Databases: []DatabaseConfig{
			{ID: "db1", Engine: "postgres", DSN: "dsn1", MaxConns: 10},  // updated
			{ID: "db3", Engine: "clickhouse", DSN: "dsn3", MaxConns: 5}, // added
		}, // db2 was removed
	}

	diff := DiffDatabases(oldCfg, newCfg)

	if len(diff.Added) != 1 || diff.Added[0].ID != "db3" {
		t.Errorf("expected Added database to contain only db3; got %v", diff.Added)
	}
	if len(diff.Removed) != 1 || diff.Removed[0].ID != "db2" {
		t.Errorf("expected Removed database to contain only db2; got %v", diff.Removed)
	}
	if len(diff.Updated) != 1 || diff.Updated[0].ID != "db1" || diff.Updated[0].MaxConns != 10 {
		t.Errorf("expected Updated database to contain only db1 with 10 max conns; got %v", diff.Updated)
	}
}

func TestNonReloadableChanges(t *testing.T) {
	base := &Config{
		Instance: InstanceConfig{ID: "a", MetaStore: "memory"},
		Server:   ServerConfig{RESTAddr: ":8080"},
	}

	if got := NonReloadableChanges(base, base); len(got) != 0 {
		t.Errorf("NonReloadableChanges on an unchanged config = %v, want none", got)
	}

	changed := *base
	changed.Instance.ID = "b"
	changed.Server.RESTAddr = ":9999"
	// Removing a leaked token has to be reported as ignored: the Authenticator
	// is built once, so the token keeps working until the process restarts.
	changed.Auth = &AuthConfig{Tokens: []AuthTokenConfig{{Subject: "a", Value: "v", Scopes: []string{"read"}}}}
	changed.Defaults.MaxConcurrentQueries = 4

	got := NonReloadableChanges(base, &changed)
	want := []string{"instance", "server", "auth", "defaults.max_concurrent_queries"}
	if len(got) != len(want) {
		t.Fatalf("NonReloadableChanges = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("NonReloadableChanges[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestExpandEnv covers the fix for the Kubernetes instance ID: the loader had
// no substitution at all, so `dbbridge-$(POD_NAME)` stayed a literal and both
// replicas reported the same owner.
func TestExpandEnv(t *testing.T) {
	t.Setenv("DBBRIDGE_TEST_POD", "pod-7")

	expand := func(t *testing.T, body string) (map[string]any, error) {
		t.Helper()
		var doc yaml.Node
		if err := yaml.Unmarshal([]byte(body), &doc); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if err := expandEnv(&doc); err != nil {
			return nil, err
		}
		out := map[string]any{}
		if err := doc.Decode(&out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return out, nil
	}

	got, err := expand(t, "id: dbbridge-${DBBRIDGE_TEST_POD}")
	if err != nil {
		t.Fatalf("expandEnv: %v", err)
	}
	if got["id"] != "dbbridge-pod-7" {
		t.Errorf("id = %v, want dbbridge-pod-7", got["id"])
	}

	// Bare $VAR is left alone: DSNs and passwords contain dollar signs.
	got, err = expand(t, `dsn: "postgres://u:p$word@host/db"`)
	if err != nil {
		t.Fatalf("expandEnv: %v", err)
	}
	if got["dsn"] != "postgres://u:p$word@host/db" {
		t.Errorf("expandEnv rewrote a bare dollar sign: %v", got["dsn"])
	}

	// A reference inside a comment is not a reference. Substituting into the
	// raw text made a commented-out example fail the load.
	if _, err := expand(t, "id: x\n# example: redis_password: \"${DBBRIDGE_TEST_UNSET}\"\n"); err != nil {
		t.Errorf("a commented-out reference was expanded: %v", err)
	}

	// An unset variable is an error: silently substituting an empty string is
	// how every replica ended up with the same instance ID.
	if _, err := expand(t, "id: ${DBBRIDGE_TEST_UNSET}"); err == nil {
		t.Error("expandEnv accepted an unset variable")
	}
}

// TestExpandEnvKeepsSecretsIntact: substitution used to run over the raw YAML,
// so the value became part of the markup. A password with a quote or a
// backslash failed the parse with an error naming neither the variable nor the
// real line, and one with a trailing newline - which is what `kubectl create
// secret --from-file` stores - silently turned into a different string.
func TestExpandEnvKeepsSecretsIntact(t *testing.T) {
	cases := map[string]string{
		"quote":           `pa"ss`,
		"backslash":       `pa\ss`,
		"trailing space":  "secret ",
		"newline":         "s3cret\n",
		"yaml injection":  "x\"\ninstance2: injected",
		"leading zeros":   "0012",
		"looks like bool": "yes",
	}

	for name, secret := range cases {
		t.Run(name, func(t *testing.T) {
			t.Setenv("DBBRIDGE_TEST_SECRET", secret)

			path := filepath.Join(t.TempDir(), "cfg.yaml")
			body := `
instance:
  id: test
  metastore: redis
  redis_addr: "redis:6379"
  redis_password: "${DBBRIDGE_TEST_SECRET}"
databases: []
`
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}
			mgr, err := NewManager(path)
			if err != nil {
				t.Fatalf("NewManager: %v", err)
			}
			if got := mgr.Get().Instance.RedisPassword; got != secret {
				t.Errorf("redis_password = %q, want %q", got, secret)
			}
		})
	}
}

// TestExpandEnvResolvesNumbers: a substituted value still has to decode into
// the type of the field it lands in.
func TestExpandEnvResolvesNumbers(t *testing.T) {
	t.Setenv("DBBRIDGE_TEST_REDIS_DB", "3")

	path := filepath.Join(t.TempDir(), "cfg.yaml")
	body := `
instance:
  id: test
  metastore: redis
  redis_addr: "redis:6379"
  redis_db: ${DBBRIDGE_TEST_REDIS_DB}
databases: []
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	mgr, err := NewManager(path)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if got := mgr.Get().Instance.RedisDB; got != 3 {
		t.Errorf("redis_db = %d, want 3", got)
	}
}

func TestLoadExpandsEnvironment(t *testing.T) {
	t.Setenv("DBBRIDGE_TEST_POD", "pod-9")
	t.Setenv("DBBRIDGE_TEST_REDIS_PASSWORD", "s3cret")

	path := filepath.Join(t.TempDir(), "cfg.yaml")
	body := `
instance:
  id: dbbridge-${DBBRIDGE_TEST_POD}
  metastore: redis
  redis_addr: "redis:6379"
  redis_password: "${DBBRIDGE_TEST_REDIS_PASSWORD}"
databases: []
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	mgr, err := NewManager(path)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	cfg := mgr.Get()
	if cfg.Instance.ID != "dbbridge-pod-9" {
		t.Errorf("instance.id = %q, want dbbridge-pod-9", cfg.Instance.ID)
	}
	if cfg.Instance.RedisPassword != "s3cret" {
		t.Errorf("instance.redis_password was not expanded: %q", cfg.Instance.RedisPassword)
	}
}

// TestConfigTLSPair: a certificate without its key, or the other way round,
// would leave TLS silently off on listeners the operator believes are encrypted.
func TestConfigTLSPair(t *testing.T) {
	cases := []struct {
		name    string
		section string
		wantErr bool
		enabled bool
	}{
		{"absent", "", false, false},
		{"both", "  tls:\n    cert_file: /tmp/a.crt\n    key_file: /tmp/a.key\n", false, true},
		{"cert only", "  tls:\n    cert_file: /tmp/a.crt\n", true, false},
		{"key only", "  tls:\n    key_file: /tmp/a.key\n", true, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := "instance:\n  id: test\n  metastore: memory\nserver:\n" + tc.section + "databases: []\n"
			path := filepath.Join(t.TempDir(), "cfg.yaml")
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}
			mgr, err := NewManager(path)
			if tc.wantErr {
				if err == nil {
					t.Fatal("a half-configured TLS section was accepted")
				}
				return
			}
			if err != nil {
				t.Fatalf("NewManager: %v", err)
			}
			if got := mgr.Get().Server.TLS.Enabled(); got != tc.enabled {
				t.Errorf("TLS.Enabled() = %v, want %v", got, tc.enabled)
			}
		})
	}
}

func TestValidate_GCIntervalDefaultsAndOverrides(t *testing.T) {
	write := func(t *testing.T, body string) *Manager {
		t.Helper()
		path := filepath.Join(t.TempDir(), "dbbridge.yaml")
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatalf("write config: %v", err)
		}
		m, err := NewManager(path)
		if err != nil {
			t.Fatalf("NewManager: %v", err)
		}
		return m
	}

	const base = `
instance:
  id: test
  metastore: memory
`
	if got := write(t, base).Get().Instance.GCInterval; got != time.Minute {
		t.Errorf("default gc_interval = %v, want 1m", got)
	}

	if got := write(t, base+"  gc_interval: 250ms\n").Get().Instance.GCInterval; got != 250*time.Millisecond {
		t.Errorf("gc_interval = %v, want 250ms", got)
	}
}
