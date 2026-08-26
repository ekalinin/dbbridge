package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/ekalinin/dbbridge/internal/authn"
	"github.com/ekalinin/dbbridge/internal/config"
	"github.com/ekalinin/dbbridge/internal/core/manager"
	"github.com/ekalinin/dbbridge/internal/core/service"
	"github.com/ekalinin/dbbridge/internal/lifecycle"
	"github.com/ekalinin/dbbridge/internal/ratelimit"
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
)

// app holds the components the process is wired from. Building them is
// newApp's job and releasing them is Close's, so a new component is added by
// naming it in one of those two lists rather than by finding the right place
// among a stack of defers in main.
type app struct {
	cfgMgr *config.Manager
	cfg    *config.Config

	otelShutdown func(context.Context) error
	metaStore    state.MetaStore
	fsStore      *fs.FSResultStore
	chStore      *clickhousestore.ClickHouseResultStore

	lm            *lifecycle.Manager
	qm            *manager.QueryManager
	svc           *service.QueryService
	authenticator *authn.Authenticator
	limiter       *ratelimit.Limiter

	restServer *rest.Server
	restHTTP   *http.Server
	grpcHTTP   *http.Server
	adminHTTP  *http.Server
	tlsCerts   *certs.Reloader
}

// newApp loads the configuration and wires the process from it. Every step
// returns its failure instead of exiting, so what a failed start means is the
// caller's decision: main exits, a test inspects the error.
func newApp(configPath string) (*app, error) {
	cfgMgr, err := config.NewManager(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize config manager: %w", err)
	}
	a := &app{cfgMgr: cfgMgr, cfg: cfgMgr.Get()}

	// The startup sequence, in order. A step that fails leaves the ones before
	// it holding a Redis connection, a directory or a pool, so the half-built
	// app is released through the same Close that ends a normal run.
	steps := []func() error{
		a.initTelemetry,
		a.buildMetaStore,
		a.registerStorage,
		a.buildManagers,
		a.buildAuthenticator,
		a.buildRateLimiter,
		a.buildServers,
		a.loadTLSCerts,
	}
	for _, step := range steps {
		if err := step(); err != nil {
			a.Close()
			return nil, err
		}
	}
	return a, nil
}

// initTelemetry sets up OTLP traces and metrics. An empty endpoint makes it a
// no-op, and a collector that cannot be reached is not a reason not to serve.
func (a *app) initTelemetry() error {
	shutdown, err := telemetry.InitOTel(context.Background(), "dbbridge", a.cfg.Instance.OTLPEndpoint)
	if err != nil {
		log.Printf("WARNING: Failed to initialize OpenTelemetry: %v", err)
		return nil
	}
	a.otelShutdown = shutdown
	return nil
}

func (a *app) buildMetaStore() error {
	if a.cfg.Instance.MetaStore == "redis" {
		log.Printf("Using Redis MetaStore at %s", a.cfg.Instance.RedisAddr)
		a.metaStore = state.NewRedisMetaStore(a.cfg.Instance.RedisAddr, a.cfg.Instance.RedisPassword, a.cfg.Instance.RedisDB)
	} else {
		log.Println("Using In-Memory MetaStore (single-node only)")
		a.metaStore = state.NewMemoryMetaStore()
	}
	return nil
}

// registerStorage builds the storage backends. Only the ones the configuration
// actually asks for: creating the FS store unconditionally means MkdirAll on
// every start, which fails outright under a read-only root filesystem even when
// results go to S3. A backend the configuration does ask for is fatal when it
// cannot be built, for all three alike - starting without it left the process
// answering 400 for that backend for the rest of its life, long after the
// dependency came back.
func (a *app) registerStorage() error {
	cfg := a.cfg

	if cfg.Storage.FS.Root != "" || cfg.Instance.DefaultStorage == "fs" {
		fsStore, err := fs.NewFSResultStore(cfg.Storage.FS.Root)
		if err != nil {
			return fmt.Errorf("failed to initialize FS storage: %w", err)
		}
		a.fsStore = fsStore
		storage.Register("fs", fsStore)
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
			return fmt.Errorf("failed to initialize S3 storage: %w", err)
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
			return fmt.Errorf("failed to initialize ClickHouse storage: %w", err)
		}
		a.chStore = chStore
		storage.Register("clickhouse", chStore)
		log.Println("ClickHouse storage registered successfully")
	}

	// A default backend that was never registered would only surface as a
	// failure after a query had already executed.
	if _, err := storage.GetStore(cfg.Instance.DefaultStorage); err != nil {
		return fmt.Errorf("default storage backend %q is not available: %w", cfg.Instance.DefaultStorage, err)
	}
	return nil
}

func (a *app) buildManagers() error {
	a.lm = lifecycle.NewManager()
	qm, err := manager.NewQueryManager(a.cfgMgr, a.metaStore)
	if err != nil {
		return fmt.Errorf("failed to initialize QueryManager: %w", err)
	}
	a.qm = qm
	a.svc = service.NewQueryService(qm, a.lm)
	return nil
}

// buildAuthenticator reads the token list. A configured but unusable token list
// is a startup failure: coming up with the API open while the operator believes
// it is protected is the worst of the two outcomes.
func (a *app) buildAuthenticator() error {
	if a.cfg.Auth != nil {
		specs := make([]authn.TokenSpec, 0, len(a.cfg.Auth.Tokens))
		for _, t := range a.cfg.Auth.Tokens {
			specs = append(specs, authn.TokenSpec{
				Subject:  t.Subject,
				Value:    t.Value,
				ValueEnv: t.ValueEnv,
				Scopes:   t.Scopes,
			})
		}
		authenticator, err := authn.New(specs)
		if err != nil {
			return fmt.Errorf("failed to initialize authentication: %w", err)
		}
		a.authenticator = authenticator
		log.Printf("Authentication enabled with %d token(s)", len(specs))
	}
	a.svc.SetAuthRequired(a.authenticator != nil)
	return nil
}

func (a *app) buildRateLimiter() error {
	a.limiter = ratelimit.New(a.cfg.Server.RateLimit.RequestsPerSecond, a.cfg.Server.RateLimit.Burst)
	if a.limiter == nil {
		log.Print("WARNING: no request rate limit configured (server.rate_limit.requests_per_second)")
	}
	return nil
}

func (a *app) buildServers() error {
	a.restServer = rest.NewServer(a.svc, rest.Options{
		MaxRequestBytes:   a.cfg.Server.MaxRequestBytes,
		RequestTimeout:    a.cfg.Server.RequestTimeout,
		WSAllowedOrigins:  a.cfg.Server.WSAllowedOrigins,
		TrustedProxyCount: a.cfg.Server.TrustedProxyCount,
		Auth:              a.authenticator,
		SeparateAdmin:     a.cfg.Server.AdminAddr != "",
		RateLimit:         a.limiter,
	})
	a.restHTTP = &http.Server{
		Addr:    a.cfg.Server.RESTAddr,
		Handler: a.restServer.Handler(),
		// No ReadTimeout or WriteTimeout: they would cut off long result
		// downloads and WebSocket connections. The header and idle timeouts are
		// what close Slowloris-style connections that open and then stall.
		ReadHeaderTimeout: a.cfg.Server.ReadHeaderTimeout,
		IdleTimeout:       a.cfg.Server.IdleTimeout,
		MaxHeaderBytes:    1 << 20,
	}

	a.grpcHTTP = a.newGRPCServer()

	if h := a.restServer.AdminHandler(); h != nil {
		a.adminHTTP = &http.Server{
			Addr:              a.cfg.Server.AdminAddr,
			Handler:           h,
			ReadHeaderTimeout: a.cfg.Server.ReadHeaderTimeout,
			IdleTimeout:       a.cfg.Server.IdleTimeout,
			MaxHeaderBytes:    1 << 20,
		}
	}
	return nil
}

// newGRPCServer mounts the Connect handler with the interceptors the
// configuration asks for.
func (a *app) newGRPCServer() *http.Server {
	grpcHandler := grpcconnect.NewQueryHandler(a.svc)
	grpcMux := http.NewServeMux()
	var interceptors []connect.Interceptor
	// First in the chain, so the rate limiter and the authenticator below run
	// inside the span. Failing to instrument is not a reason not to serve.
	if traceInterceptor, err := grpcconnect.TracingInterceptor(); err != nil {
		log.Printf("WARNING: Connect tracing is disabled: %v", err)
	} else {
		interceptors = append(interceptors, traceInterceptor)
	}
	if a.limiter != nil {
		interceptors = append(interceptors, grpcconnect.RateLimitInterceptor(a.limiter))
	}
	if a.authenticator != nil {
		interceptors = append(interceptors, grpcconnect.NewAuthInterceptor(a.authenticator))
	}
	var connectOpts []connect.HandlerOption
	if len(interceptors) > 0 {
		connectOpts = append(connectOpts, connect.WithInterceptors(interceptors...))
	}
	path, handler := v1connect.NewQueryServiceHandler(grpcHandler, connectOpts...)
	grpcMux.Handle(path, handler)

	srv := &http.Server{
		Addr:              a.cfg.Server.GRPCAddr,
		Handler:           grpcMux,
		ReadHeaderTimeout: a.cfg.Server.ReadHeaderTimeout,
		IdleTimeout:       a.cfg.Server.IdleTimeout,
		MaxHeaderBytes:    1 << 20,
	}
	srv.Protocols = new(http.Protocols)
	srv.Protocols.SetHTTP1(true)
	if a.cfg.Server.TLS.Enabled() {
		// TLS carries HTTP/2 through ALPN; cleartext HTTP/2 is not needed.
		srv.Protocols.SetHTTP2(true)
	} else {
		if !a.cfg.Server.TLS.AllowH2C {
			log.Print("WARNING: gRPC is served as cleartext HTTP/2 (h2c); configure server.tls or set server.tls.allow_h2c to acknowledge this")
		}
		srv.Protocols.SetUnencryptedHTTP2(true)
	}
	return srv
}

// loadTLSCerts reads the key pair before any listener starts, so a wrong path
// fails the start with a clear message instead of killing a listener goroutine
// once the other listeners, the MetaStore and the pools are already up - a
// log.Fatalf there skips the cleanup Close does.
func (a *app) loadTLSCerts() error {
	tlsCfg := a.cfg.Server.TLS
	if !tlsCfg.Enabled() {
		return nil
	}
	tlsCerts, err := certs.NewReloader(tlsCfg.CertFile, tlsCfg.KeyFile)
	if err != nil {
		return fmt.Errorf("failed to load the TLS key pair: %w", err)
	}
	a.tlsCerts = tlsCerts
	return nil
}

// serve starts every configured listener in its own goroutine.
func (a *app) serve() {
	a.serveOne("REST API", a.restHTTP)
	a.serveOne("gRPC / Connect API", a.grpcHTTP)
	if a.adminHTTP != nil {
		a.serveOne("metrics / admin API", a.adminHTTP)
	}
}

func (a *app) serveOne(name string, srv *http.Server) {
	go func() {
		scheme := "http"
		if a.tlsCerts != nil {
			scheme = "https"
			srv.TLSConfig = a.tlsCerts.TLSConfig()
		}
		log.Printf("Starting %s on %s (%s)", name, srv.Addr, scheme)
		var err error
		if a.tlsCerts != nil {
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

// closeStep is one entry of the cleanup order, named after what it releases so
// a failure reports the component rather than the position in a defer stack.
type closeStep struct {
	what  string
	close func() error
}

// closeSteps lists the cleanup in the order it runs: what uses a dependency
// goes before the dependency itself, and telemetry last so the shutdown of the
// steps before it is still traced. A component that was never built - because
// the configuration did not ask for it, or because a later step failed the
// start - is simply not in the list.
func (a *app) closeSteps() []closeStep {
	var steps []closeStep
	if a.qm != nil {
		steps = append(steps, closeStep{"QueryManager close", a.qm.Close})
	}
	if a.chStore != nil {
		steps = append(steps, closeStep{"ClickHouse storage close", a.chStore.Close})
	}
	if a.fsStore != nil {
		steps = append(steps, closeStep{"FS storage close", a.fsStore.Close})
	}
	if a.metaStore != nil {
		steps = append(steps, closeStep{"MetaStore close", a.metaStore.Close})
	}
	if a.otelShutdown != nil {
		steps = append(steps, closeStep{"OpenTelemetry shutdown", a.shutdownTelemetry})
	}
	return steps
}

// Close releases the components newApp built. A failing step is logged and the
// remaining ones still run: the process is going away either way.
func (a *app) Close() {
	for _, step := range a.closeSteps() {
		if err := step.close(); err != nil {
			log.Printf("ERROR: %s failed: %v", step.what, err)
		}
	}
}

// shutdownTelemetry flushes what the exporter still holds, bounded so an
// unreachable collector cannot hold the process open.
func (a *app) shutdownTelemetry() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return a.otelShutdown(ctx)
}
