package config

import (
	"os"
	"path/filepath"
	"testing"
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
