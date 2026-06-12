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

	"dbbridge/internal/config"
	"dbbridge/internal/core/manager"
	"dbbridge/internal/core/service"
	"dbbridge/internal/lifecycle"
	"dbbridge/internal/state"
	"dbbridge/internal/storage"
	"dbbridge/internal/storage/backends/fs"
	"dbbridge/internal/storage/backends/s3"
	"dbbridge/internal/telemetry"
	"dbbridge/internal/transport/grpcconnect"
	"dbbridge/internal/transport/rest"

	v1connect "dbbridge/internal/gen/api/proto/dbbridge/v1/dbbridgev1connect"

	// Register drivers statically
	_ "dbbridge/internal/db/drivers/clickhouse"
	_ "dbbridge/internal/db/drivers/mysql"
	_ "dbbridge/internal/db/drivers/oracle"
	_ "dbbridge/internal/db/drivers/postgres"
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
			_ = otelShutdown(ctx)
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
	defer metaStore.Close()

	// 3. Initialize Storage backends
	fsStore, err := fs.NewFSResultStore(cfg.Storage.FS.Root)
	if err != nil {
		log.Fatalf("Failed to initialize FS storage: %v", err)
	}
	storage.Register("fs", fsStore)

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
			log.Printf("WARNING: Failed to initialize S3 storage: %v", err)
		} else {
			storage.Register("s3", s3Store)
			log.Println("S3 storage registered successfully")
		}
	}

	// 4. Initialize Lifecycle and Managers
	lm := lifecycle.NewManager()
	qm, err := manager.NewQueryManager(cfgMgr, metaStore)
	if err != nil {
		log.Fatalf("Failed to initialize QueryManager: %v", err)
	}
	defer qm.Close()

	svc := service.NewQueryService(qm, lm)

	// 5. Initialize Servers
	restServer := rest.NewServer(svc)
	restHTTP := &http.Server{
		Addr:    cfg.Server.RESTAddr,
		Handler: restServer.Handler(),
	}

	// Setup gRPC Connect server
	grpcHandler := grpcconnect.NewQueryHandler(svc)
	grpcMux := http.NewServeMux()
	path, handler := v1connect.NewQueryServiceHandler(grpcHandler)
	grpcMux.Handle(path, handler)

	grpcHTTP := &http.Server{
		Addr:    cfg.Server.GRPCAddr,
		Handler: grpcMux,
	}
	grpcHTTP.Protocols = new(http.Protocols)
	grpcHTTP.Protocols.SetHTTP1(true)
	grpcHTTP.Protocols.SetUnencryptedHTTP2(true)

	// 6. Start Servers
	go func() {
		log.Printf("Starting REST API on %s", cfg.Server.RESTAddr)
		if err := restHTTP.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("REST API server failed: %v", err)
		}
	}()

	go func() {
		log.Printf("Starting gRPC / Connect API on %s", cfg.Server.GRPCAddr)
		if err := grpcHTTP.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("gRPC Connect server failed: %v", err)
		}
	}()

	// 7. Handle OS Signals (Graceful Reload & Shutdown)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

	for {
		sig := <-sigCh
		if sig == syscall.SIGHUP {
			log.Println("Received SIGHUP, reloading configuration...")
			if err := qm.Reload(); err != nil {
				log.Printf("ERROR: Failed to reload config: %v", err)
			} else {
				log.Println("Configuration reloaded successfully")
			}
			continue
		}

		// SIGINT or SIGTERM -> Draining & Shutdown
		log.Printf("Received signal %v, starting graceful shutdown / draining...", sig)
		lm.SetState(lifecycle.StateDraining)

		// Wait until all queries on this node are finished
		shutdownDeadline := time.Now().Add(30 * time.Second)
		for {
			inFlight := qm.CountInFlight()
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
		_ = restHTTP.Shutdown(ctx)
		_ = grpcHTTP.Shutdown(ctx)
		cancel()

		log.Println("dbbridge stopped.")
		break
	}
}
