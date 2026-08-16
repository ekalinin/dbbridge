package config

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"gopkg.in/yaml.v3"
)

// InstanceConfig holds config specific to this node.
type InstanceConfig struct {
	ID             string        `yaml:"id"`
	MetaStore      string        `yaml:"metastore"` // "redis" or "memory"
	RedisAddr      string        `yaml:"redis_addr"`
	RedisPassword  string        `yaml:"redis_password"`
	RedisDB        int           `yaml:"redis_db"`
	DefaultStorage string        `yaml:"default_storage"` // "fs", "s3", "clickhouse"
	HeartbeatTTL   time.Duration `yaml:"heartbeat_ttl"`   // default 5s
	OTLPEndpoint   string        `yaml:"otlp_endpoint"`   // OTLP gRPC endpoint; empty disables OTLP export
}

// ServerConfig configures REST, gRPC and WS endpoints.
type ServerConfig struct {
	RESTAddr string `yaml:"rest_addr"`
	GRPCAddr string `yaml:"grpc_addr"`
	// MaxRequestBytes caps a JSON request body; default 1 MiB. Without it the
	// SQL text of a submission is unbounded.
	MaxRequestBytes int64 `yaml:"max_request_bytes"`
	// RequestTimeout bounds ordinary HTTP operations; default 60s. Streaming
	// routes (result download, sync submission, WebSocket) are exempt, they
	// legitimately outlive it.
	RequestTimeout time.Duration `yaml:"request_timeout"`
	// ReadHeaderTimeout and IdleTimeout protect both listeners from connections
	// that open and then stall.
	ReadHeaderTimeout time.Duration `yaml:"read_header_timeout"`
	IdleTimeout       time.Duration `yaml:"idle_timeout"`
	// WSAllowedOrigins lists the browser origins allowed to open a WebSocket.
	// Empty means same-origin only.
	WSAllowedOrigins []string `yaml:"ws_allowed_origins"`
	// TrustedProxyCount is how many reverse proxies sit in front of the
	// service. It decides which X-Forwarded-For entry is the real client; the
	// header is forgeable everywhere else.
	TrustedProxyCount int `yaml:"trusted_proxy_count"`
	// AdminAddr, when set, moves /metrics and /v1/admin/* to their own
	// listener. /metrics enumerates every configured db_id and the admin
	// routes reload the process, so neither belongs on the public port.
	AdminAddr string `yaml:"admin_addr"`
}

// AuthConfig configures API authentication. A nil pointer means the section is
// absent and the API is unauthenticated; an empty token list is a mistake and
// fails validation rather than starting up open.
type AuthConfig struct {
	Tokens []AuthTokenConfig `yaml:"tokens"`
}

// AuthTokenConfig is one static bearer token.
type AuthTokenConfig struct {
	Subject string `yaml:"subject"`
	// Value holds the token inline. Prefer ValueEnv: a value here ends up in
	// the config file, and in Kubernetes that means a ConfigMap.
	Value    string   `yaml:"value"`
	ValueEnv string   `yaml:"value_env"`
	Scopes   []string `yaml:"scopes"` // read | write | admin
}

// DefaultsConfig defines global defaults for query execution parameters.
type DefaultsConfig struct {
	ResultTTL    time.Duration `yaml:"result_ttl"`    // default 24h
	QueryTimeout time.Duration `yaml:"query_timeout"` // default 0 (unlimited)
	// MaxConcurrentQueries caps executions running on this instance at once;
	// 0 means unlimited. Fixed at startup, see NonReloadableChanges.
	MaxConcurrentQueries int `yaml:"max_concurrent_queries"`
	// AllowWrites disables the read-only statement guard. It defaults to false:
	// a proxy that materializes results has no reason to run DML or DDL, and an
	// exposed endpoint that does is remote code execution against the database.
	AllowWrites bool `yaml:"allow_writes"`
}

// StorageFSConfig configures local filesystem storage.
type StorageFSConfig struct {
	Root string `yaml:"root"`
}

// StorageS3Config configures S3-compatible object storage.
type StorageS3Config struct {
	Bucket   string `yaml:"bucket"`
	Region   string `yaml:"region"`
	Endpoint string `yaml:"endpoint"` // for local minio testing
	KeyID    string `yaml:"access_key_id"`
	Secret   string `yaml:"secret_access_key"`
}

// StorageClickHouseConfig configures storage of query results directly inside a ClickHouse table.
type StorageClickHouseConfig struct {
	DSN   string `yaml:"dsn"`
	Table string `yaml:"table"`
}

// StorageConfig wraps configurations for all supported storage backends.
type StorageConfig struct {
	FS         StorageFSConfig         `yaml:"fs"`
	S3         StorageS3Config         `yaml:"s3"`
	ClickHouse StorageClickHouseConfig `yaml:"clickhouse"`
}

// DatabaseConfig contains configuration to connect to target relational databases.
type DatabaseConfig struct {
	ID          string `yaml:"id"`
	Engine      string `yaml:"engine"` // postgres, mysql, clickhouse, oracle
	DSN         string `yaml:"dsn"`
	DisplayName string `yaml:"display_name"`
	MaxConns    int    `yaml:"max_conns"`
}

// Config is the main application configuration struct.
type Config struct {
	Instance  InstanceConfig   `yaml:"instance"`
	Server    ServerConfig     `yaml:"server"`
	Auth      *AuthConfig      `yaml:"auth"`
	Defaults  DefaultsConfig   `yaml:"defaults"`
	Storage   StorageConfig    `yaml:"storage"`
	Databases []DatabaseConfig `yaml:"databases"`
}

// Manager manages a thread-safe hot-reloadable atomic configuration pointer.
type Manager struct {
	configPath string
	ptr        atomic.Pointer[Config]
}

// NewManager creates a new config manager.
func NewManager(path string) (*Manager, error) {
	m := &Manager{configPath: path}
	if err := m.Reload(); err != nil {
		return nil, fmt.Errorf("initial config load failed: %w", err)
	}
	return m, nil
}

// Get returns the latest thread-safe snapshot of the config.
func (m *Manager) Get() *Config {
	return m.ptr.Load()
}

// Reload re-reads the config file from disk, parses it and performs atomic swap.
func (m *Manager) Reload() error {
	data, err := os.ReadFile(m.configPath)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("failed to unmarshal yaml: %w", err)
	}

	if err := validate(&cfg); err != nil {
		return fmt.Errorf("config validation failed: %w", err)
	}

	m.ptr.Store(&cfg)
	return nil
}

func validate(cfg *Config) error {
	if cfg.Instance.ID == "" {
		return fmt.Errorf("instance.id must not be empty")
	}
	if cfg.Instance.MetaStore != "redis" && cfg.Instance.MetaStore != "memory" {
		return fmt.Errorf("instance.metastore must be 'redis' or 'memory'")
	}
	if cfg.Instance.MetaStore == "redis" && cfg.Instance.RedisAddr == "" {
		return fmt.Errorf("instance.redis_addr must be specified when metastore is redis")
	}
	if cfg.Instance.DefaultStorage == "" {
		cfg.Instance.DefaultStorage = "fs"
	}
	// Catch a default backend that can never be built at load time rather than
	// after a query has already run against the database.
	switch cfg.Instance.DefaultStorage {
	case "fs":
	case "s3":
		if cfg.Storage.S3.Bucket == "" {
			return fmt.Errorf("storage.s3.bucket must be set when default_storage is s3")
		}
	case "clickhouse":
		if cfg.Storage.ClickHouse.DSN == "" {
			return fmt.Errorf("storage.clickhouse.dsn must be set when default_storage is clickhouse")
		}
	default:
		return fmt.Errorf("unsupported instance.default_storage: %s", cfg.Instance.DefaultStorage)
	}
	if cfg.Instance.HeartbeatTTL == 0 {
		cfg.Instance.HeartbeatTTL = 5 * time.Second
	}
	if cfg.Defaults.ResultTTL == 0 {
		cfg.Defaults.ResultTTL = 24 * time.Hour
	}
	if cfg.Server.RESTAddr == "" {
		cfg.Server.RESTAddr = ":8080"
	}
	if cfg.Server.GRPCAddr == "" {
		cfg.Server.GRPCAddr = ":9090"
	}
	if cfg.Server.MaxRequestBytes == 0 {
		cfg.Server.MaxRequestBytes = 1 << 20
	}
	if cfg.Server.RequestTimeout == 0 {
		cfg.Server.RequestTimeout = 60 * time.Second
	}
	if cfg.Server.ReadHeaderTimeout == 0 {
		cfg.Server.ReadHeaderTimeout = 10 * time.Second
	}
	if cfg.Server.IdleTimeout == 0 {
		cfg.Server.IdleTimeout = 120 * time.Second
	}
	// An auth section that resolves to no tokens would start the process with
	// the API wide open while the operator believes it is protected.
	if cfg.Auth != nil && len(cfg.Auth.Tokens) == 0 {
		return fmt.Errorf("auth is configured but auth.tokens is empty")
	}
	// Check databases
	seen := make(map[string]bool)
	for _, db := range cfg.Databases {
		if db.ID == "" {
			return fmt.Errorf("database ID cannot be empty")
		}
		if seen[db.ID] {
			return fmt.Errorf("duplicate database ID: %s", db.ID)
		}
		seen[db.ID] = true
		if db.Engine != "postgres" && db.Engine != "mysql" && db.Engine != "clickhouse" && db.Engine != "oracle" {
			return fmt.Errorf("unsupported database engine: %s for db %s", db.Engine, db.ID)
		}
		if db.DSN == "" {
			return fmt.Errorf("dsn must be provided for database %s", db.ID)
		}
	}
	return nil
}
