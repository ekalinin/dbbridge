package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ekalinin/dbbridge/internal/authn"
	"github.com/ekalinin/dbbridge/internal/config"
	"github.com/ekalinin/dbbridge/internal/core/manager"
	"github.com/ekalinin/dbbridge/internal/core/service"
	"github.com/ekalinin/dbbridge/internal/lifecycle"
	"github.com/ekalinin/dbbridge/internal/state"
	"github.com/ekalinin/dbbridge/internal/storage"
	clickhousestore "github.com/ekalinin/dbbridge/internal/storage/backends/clickhouse"
	"github.com/ekalinin/dbbridge/internal/storage/backends/fs"
	"github.com/ekalinin/dbbridge/internal/storage/backends/s3"
	"github.com/ekalinin/dbbridge/internal/telemetry"
	"github.com/ekalinin/dbbridge/internal/transport/certs"
	"github.com/ekalinin/dbbridge/internal/transport/grpcconnect"
	"github.com/ekalinin/dbbridge/internal/transport/rest"

	v1connect "github.com/ekalinin/dbbridge/internal/gen/dbbridge/v1/dbbridgev1connect"

	"connectrpc.com/connect"

	// Register drivers statically
	_ "github.com/ekalinin/dbbridge/internal/db/drivers/clickhouse"
	_ "github.com/ekalinin/dbbridge/internal/db/drivers/mysql"
	_ "github.com/ekalinin/dbbridge/internal/db/drivers/oracle"
	_ "github.com/ekalinin/dbbridge/internal/db/drivers/postgres"
)

func main() {
	configPath := flag.String("config", "configs/dbbridge.yaml", "Path to config file")
	flag.Parse()

	log.Printf("Starting dbbridge with config: %s", *configPath)

	// 1. Initialize Configuration
	cfgMgr, err := config.NewManager(*configPath)
	if err != nil {
		log.Fatalf("Failed to initialize config manager: %v", err)
	}
	cfg := cfgMgr.Get()

	// 1b. Initialize Telemetry (OTLP traces + metrics). Empty endpoint = no-op.
	otelShutdown, err := telemetry.InitOTel(context.Background(), "dbbridge", cfg.Instance.OTLPEndpoint)
	if err != nil {
		log.Printf("WARNING: Failed to initialize OpenTelemetry: %v", err)
	} else {
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := otelShutdown(ctx); err != nil {
				log.Printf("ERROR: OpenTelemetry shutdown failed: %v", err)
			}
		}()
	}

	// 2. Initialize MetaStore
	var metaStore state.MetaStore
	if cfg.Instance.MetaStore == "redis" {
		log.Printf("Using Redis MetaStore at %s", cfg.Instance.RedisAddr)
		metaStore = state.NewRedisMetaStore(cfg.Instance.RedisAddr, cfg.Instance.RedisPassword, cfg.Instance.RedisDB)
	} else {
		log.Println("Using In-Memory MetaStore (single-node only)")
		metaStore = state.NewMemoryMetaStore()
	}
	defer func() {
		if err := metaStore.Close(); err != nil {
			log.Printf("ERROR: MetaStore close failed: %v", err)
		}
	}()

	// 3. Initialize Storage backends. Only the ones the configuration actually
	// asks for: creating the FS store unconditionally means MkdirAll on every
	// start, which fails outright under a read-only root filesystem even when
	// results go to S3. A backend the configuration does ask for is fatal when
	// it cannot be built, for all three alike - starting without it left the
	// process answering 400 for that backend for the rest of its life, long
	// after the dependency came back.
	if cfg.Storage.FS.Root != "" || cfg.Instance.DefaultStorage == "fs" {
		fsStore, err := fs.NewFSResultStore(cfg.Storage.FS.Root)
		if err != nil {
			log.Fatalf("Failed to initialize FS storage: %v", err)
		}
		storage.Register("fs", fsStore)
		defer func() {
			if err := fsStore.Close(); err != nil {
				log.Printf("ERROR: FS storage close failed: %v", err)
			}
		}()
	}

	if cfg.Storage.S3.Bucket != "" {
		s3Store, err := s3.NewS3ResultStore(
			context.Background(),
			cfg.Storage.S3.Bucket,
			cfg.Storage.S3.Region,
			cfg.Storage.S3.Endpoint,
			cfg.Storage.S3.KeyID,
			cfg.Storage.S3.Secret,
		)
		if err != nil {
			log.Fatalf("Failed to initialize S3 storage: %v", err)
		}
		storage.Register("s3", s3Store)
		log.Println("S3 storage registered successfully")
	}

	// The ClickHouse backend existed but was never registered, so
	// storage_backend: clickhouse failed with "unknown storage backend" only
	// after the SQL had already run.
	if cfg.Storage.ClickHouse.DSN != "" {
		chStore, err := clickhousestore.NewClickHouseResultStore(
			cfg.Storage.ClickHouse.DSN,
			cfg.Storage.ClickHouse.Table,
		)
		if err != nil {
			log.Fatalf("Failed to initialize ClickHouse storage: %v", err)
		}
		storage.Register("clickhouse", chStore)
		log.Println("ClickHouse storage registered successfully")
		defer func() {
			if err := chStore.Close(); err != nil {
				log.Printf("ERROR: ClickHouse storage close failed: %v", err)
			}
		}()
	}

	// A default backend that was never registered would only surface as a
	// failure after a query had already executed.
	if _, err := storage.GetStore(cfg.Instance.DefaultStorage); err != nil {
		log.Fatalf("Default storage backend %q is not available: %v", cfg.Instance.DefaultStorage, err)
	}

	// 4. Initialize Lifecycle and Managers
	lm := lifecycle.NewManager()
	qm, err := manager.NewQueryManager(cfgMgr, metaStore)
	if err != nil {
		log.Fatalf("Failed to initialize QueryManager: %v", err)
	}
	defer func() {
		if err := qm.Close(); err != nil {
			log.Printf("ERROR: QueryManager close failed: %v", err)
		}
	}()

	svc := service.NewQueryService(qm, lm)

	// 5. Initialize authentication. A configured but unusable token list is a
	// startup failure: coming up with the API open while the operator believes
	// it is protected is the worst of the two outcomes.
	var authenticator *authn.Authenticator
	if cfg.Auth != nil {
		specs := make([]authn.TokenSpec, 0, len(cfg.Auth.Tokens))
		for _, t := range cfg.Auth.Tokens {
			specs = append(specs, authn.TokenSpec{
				Subject:  t.Subject,
				Value:    t.Value,
				ValueEnv: t.ValueEnv,
				Scopes:   t.Scopes,
			})
		}
		authenticator, err = authn.New(specs)
		if err != nil {
			log.Fatalf("Failed to initialize authentication: %v", err)
		}
		log.Printf("Authentication enabled with %d token(s)", len(specs))
	}
	svc.SetAuthRequired(authenticator != nil)

	// 6. Initialize Servers
	restServer := rest.NewServer(svc, rest.Options{
		MaxRequestBytes:   cfg.Server.MaxRequestBytes,
		RequestTimeout:    cfg.Server.RequestTimeout,
		WSAllowedOrigins:  cfg.Server.WSAllowedOrigins,
		TrustedProxyCount: cfg.Server.TrustedProxyCount,
		Auth:              authenticator,
		SeparateAdmin:     cfg.Server.AdminAddr != "",
	})
	restHTTP := &http.Server{
		Addr:    cfg.Server.RESTAddr,
		Handler: restServer.Handler(),
		// No ReadTimeout or WriteTimeout: they would cut off long result
		// downloads and WebSocket connections. The header and idle timeouts are
		// what close Slowloris-style connections that open and then stall.
		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout,
		IdleTimeout:       cfg.Server.IdleTimeout,
		MaxHeaderBytes:    1 << 20,
	}

	// Setup gRPC Connect server
	grpcHandler := grpcconnect.NewQueryHandler(svc)
	grpcMux := http.NewServeMux()
	var connectOpts []connect.HandlerOption
	if authenticator != nil {
		connectOpts = append(connectOpts, connect.WithInterceptors(grpcconnect.NewAuthInterceptor(authenticator)))
	}
	path, handler := v1connect.NewQueryServiceHandler(grpcHandler, connectOpts...)
	grpcMux.Handle(path, handler)

	grpcHTTP := &http.Server{
		Addr:              cfg.Server.GRPCAddr,
		Handler:           grpcMux,
		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout,
		IdleTimeout:       cfg.Server.IdleTimeout,
		MaxHeaderBytes:    1 << 20,
	}
	grpcHTTP.Protocols = new(http.Protocols)
	grpcHTTP.Protocols.SetHTTP1(true)
	if cfg.Server.TLS.Enabled() {
		// TLS carries HTTP/2 through ALPN; cleartext HTTP/2 is not needed.
		grpcHTTP.Protocols.SetHTTP2(true)
	} else {
		if !cfg.Server.TLS.AllowH2C {
			log.Print("WARNING: gRPC is served as cleartext HTTP/2 (h2c); configure server.tls or set server.tls.allow_h2c to acknowledge this")
		}
		grpcHTTP.Protocols.SetUnencryptedHTTP2(true)
	}

	var adminHTTP *http.Server
	if h := restServer.AdminHandler(); h != nil {
		adminHTTP = &http.Server{
			Addr:              cfg.Server.AdminAddr,
			Handler:           h,
			ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout,
			IdleTimeout:       cfg.Server.IdleTimeout,
			MaxHeaderBytes:    1 << 20,
		}
	}

	// 7. Start Servers
	tlsCfg := cfg.Server.TLS
	// The pair is loaded here rather than inside ListenAndServeTLS, so a wrong
	// path fails the start with a clear message instead of killing a listener
	// goroutine once the other listeners, the MetaStore and the pools are
	// already up - a log.Fatalf there skips every deferred cleanup.
	var tlsCerts *certs.Reloader
	if tlsCfg.Enabled() {
		tlsCerts, err = certs.NewReloader(tlsCfg.CertFile, tlsCfg.KeyFile)
		if err != nil {
			log.Fatalf("Failed to load the TLS key pair: %v", err)
		}
	}
	serve := func(name string, srv *http.Server) {
		go func() {
			scheme := "http"
			if tlsCerts != nil {
				scheme = "https"
				srv.TLSConfig = tlsCerts.TLSConfig()
			}
			log.Printf("Starting %s on %s (%s)", name, srv.Addr, scheme)
			var err error
			if tlsCerts != nil {
				// The paths are empty on purpose: the certificate comes from
				// TLSConfig.GetCertificate, which re-reads the files when they
				// change, so a rotated certificate does not wait for a restart.
				err = srv.ListenAndServeTLS("", "")
			} else {
				err = srv.ListenAndServe()
			}
			if err != nil && err != http.ErrServerClosed {
				log.Fatalf("%s failed: %v", name, err)
			}
		}()
	}

	serve("REST API", restHTTP)
	serve("gRPC / Connect API", grpcHTTP)
	if adminHTTP != nil {
		serve("metrics / admin API", adminHTTP)
	}

	// 8. Handle OS Signals (Graceful Reload & Shutdown)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

	for {
		sig := <-sigCh
		if sig == syscall.SIGHUP {
			log.Println("Received SIGHUP, reloading configuration...")
			if report, err := qm.Reload(); err != nil {
				log.Printf("ERROR: Failed to reload config: %v", err)
			} else {
				log.Printf("Configuration reloaded successfully (added=%v removed=%v updated=%v)",
					report.Added, report.Removed, report.Updated)
			}
			continue
		}

		// SIGINT or SIGTERM -> Draining & Shutdown
		log.Printf("Received signal %v, starting graceful shutdown / draining...", sig)
		lm.SetState(lifecycle.StateDraining)
		// Close admission in the manager as well: the lifecycle flag is checked
		// by the service before it calls SubmitQuery, which leaves a window in
		// which a query can still register after the loop below sees zero (I5).
		qm.Drain()

		// Wait until all queries on this node are finished
		shutdownDeadline := time.Now().Add(30 * time.Second)
		for {
			inFlight := qm.CountInFlight(context.Background())
			// Publishes the DRAINING -> STOPPABLE transition, which is what
			// /v1/admin/can-stop reports to the orchestrator (spec §9).
			lm.Advance(inFlight)
			if inFlight == 0 {
				log.Println("0 owned active queries remaining. Safe to stop.")
				break
			}
			if time.Now().After(shutdownDeadline) {
				log.Printf("Shutdown deadline exceeded, forcing stop with %d queries still active", inFlight)
				break
			}
			log.Printf("Waiting for %d active queries to complete...", inFlight)
			time.Sleep(1 * time.Second)
		}

		// Shutdown HTTP Servers
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := restHTTP.Shutdown(ctx); err != nil {
			log.Printf("ERROR: REST server shutdown failed: %v", err)
		}
		if err := grpcHTTP.Shutdown(ctx); err != nil {
			log.Printf("ERROR: gRPC server shutdown failed: %v", err)
		}
		if adminHTTP != nil {
			if err := adminHTTP.Shutdown(ctx); err != nil {
				log.Printf("ERROR: admin server shutdown failed: %v", err)
			}
		}
		cancel()

		log.Println("dbbridge stopped.")
		break
	}
}
