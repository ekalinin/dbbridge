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
}

// DefaultsConfig defines global defaults for query execution parameters.
type DefaultsConfig struct {
	ResultTTL    time.Duration `yaml:"result_ttl"`    // default 24h
	QueryTimeout time.Duration `yaml:"query_timeout"` // default 0 (unlimited)
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
