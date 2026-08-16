package rest

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ekalinin/dbbridge/internal/authn"
	"github.com/ekalinin/dbbridge/internal/core/domain"
	"github.com/ekalinin/dbbridge/internal/core/service"
	"github.com/ekalinin/dbbridge/internal/telemetry"
	"github.com/ekalinin/dbbridge/internal/transport/ws"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// Options carries the transport limits. Zero values fall back to the defaults
// below, so tests and embedders can pass Options{}.
type Options struct {
	// MaxRequestBytes caps a JSON request body.
	MaxRequestBytes int64
	// RequestTimeout bounds ordinary HTTP operations. It is deliberately not
	// applied to the streaming routes.
	RequestTimeout time.Duration
	// WSAllowedOrigins lists browser origins allowed to open a WebSocket.
	WSAllowedOrigins []string
	// TrustedProxyCount is the number of reverse proxies in front of this
	// service; it decides which X-Forwarded-For entry is the real client.
	TrustedProxyCount int
	// Auth enforces bearer tokens on the /v1 routes. A nil Authenticator
	// leaves the API open, which NewServer logs about loudly.
	Auth *authn.Authenticator
	// SeparateAdmin moves /metrics and /v1/admin/* off the public router and
	// onto AdminHandler.
	SeparateAdmin bool
}

const (
	defaultMaxRequestBytes = 1 << 20
	defaultRequestTimeout  = 60 * time.Second
)

func (o Options) withDefaults() Options {
	if o.MaxRequestBytes <= 0 {
		o.MaxRequestBytes = defaultMaxRequestBytes
	}
	if o.RequestTimeout <= 0 {
		o.RequestTimeout = defaultRequestTimeout
	}
	return o
}

type Server struct {
	svc         *service.QueryService
	wsHub       *ws.Hub
	router      chi.Router
	adminRouter chi.Router
	opts        Options
}

func NewServer(svc *service.QueryService, opts Options) *Server {
	opts = opts.withDefaults()
	s := &Server{
		svc:    svc,
		wsHub:  ws.NewHub(svc, ws.Options{AllowedOrigins: opts.WSAllowedOrigins}),
		router: chi.NewRouter(),
		opts:   opts,
	}
	if opts.Auth == nil {
		log.Print("WARNING: no auth tokens configured, the API accepts unauthenticated requests")
	}
	s.setupRoutes()
	return s
}

func (s *Server) Handler() http.Handler {
	return s.router
}

// AdminHandler serves /metrics and /v1/admin/*. It is non-nil only when
// Options.SeparateAdmin is set; otherwise those routes live on Handler.
func (s *Server) AdminHandler() http.Handler {
	if s.adminRouter == nil {
		return nil
	}
	return s.adminRouter
}

// useCommonMiddleware applies the middleware every router needs. The admin
// router used to go without it, so admin errors carried no request ID, admin
// requests were not logged at all, and a panic there reached the client as a
// dropped connection instead of a 500.
func (s *Server) useCommonMiddleware(r chi.Router) {
	r.Use(middleware.RequestID)
	// Without a hop count chi takes the right-most X-Forwarded-For entry, which
	// is only the real client when exactly one trusted proxy sits in front of
	// the service; in any other topology the header is attacker-controlled.
	if s.opts.TrustedProxyCount > 0 {
		r.Use(middleware.ClientIPFromXFFTrustedProxies(s.opts.TrustedProxyCount))
	} else {
		r.Use(middleware.ClientIPFromXFF())
	}
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
}

func (s *Server) setupRoutes() {
	s.useCommonMiddleware(s.router)

	// The probes stay unauthenticated: kubelet and the load balancer call them
	// and they expose nothing beyond serving state.
	s.router.Get("/healthz", s.handleHealthz)
	s.router.Get("/readyz", s.handleReadyz)

	s.router.Route("/v1", func(r chi.Router) {
		// Default deny for the whole subtree, so a route added later cannot
		// come out unauthenticated by omission. Groups below raise the bar
		// where a route needs more than read; a write token satisfies read.
		r.Use(s.require(authn.ScopeRead))

		// Long-lived routes. A blanket middleware.Timeout used to cover these
		// too: it cut result downloads and sync submissions off after a minute
		// and killed every WebSocket connection at the same age.
		r.Group(func(r chi.Router) {
			r.Get("/queries/{id}/result", s.handleDownloadResult)
			r.Get("/ws", s.wsHub.ServeHTTP)
		})
		r.Group(func(r chi.Router) {
			r.Use(s.require(authn.ScopeWrite))
			r.Post("/queries", s.handleStartQuery)
		})

		// Ordinary request/response operations keep the timeout.
		r.Group(func(r chi.Router) {
			r.Use(middleware.Timeout(s.opts.RequestTimeout))

			r.Get("/databases", s.handleListDatabases)
			r.Get("/queries/{id}", s.handleGetQueryStatus)
			r.Get("/queries/{id}/stats", s.handleGetQueryStats)

			r.Group(func(r chi.Router) {
				r.Use(s.require(authn.ScopeWrite))
				r.Post("/queries/{id}:stop", s.handleStopQuery)
			})
		})
	})

	// /metrics enumerates every configured db_id and the admin routes reload
	// the process, so they are gated on the admin scope wherever they live and
	// can be moved off the public listener entirely.
	if s.opts.SeparateAdmin {
		s.adminRouter = chi.NewRouter()
		s.useCommonMiddleware(s.adminRouter)
		s.mountAdmin(s.adminRouter)
		return
	}
	s.mountAdmin(s.router)
}

func (s *Server) mountAdmin(r chi.Router) {
	r.Group(func(r chi.Router) {
		r.Use(s.require(authn.ScopeAdmin))
		// admin_addr is optional, so the default deployment has /metrics on the
		// public listener. Leaving it open there hands out the full list of
		// configured db_ids and the traffic volume per database.
		r.Handle("/metrics", telemetry.Handler())
	})
	r.Route("/v1/admin", func(r chi.Router) {
		r.Use(middleware.Timeout(s.opts.RequestTimeout))
		r.Use(s.require(authn.ScopeAdmin))
		r.Post("/reload", s.handleReloadConfig)
		r.Get("/can-stop", s.handleCanIBeStopped)
	})
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	writeText(w, "OK")
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	// Report not-ready when the service isn't wired or the instance is draining,
	// so the load balancer / k8s readiness probe removes this node from rotation
	// and stops routing new traffic while in-flight queries finish.
	if s.svc == nil || s.svc.IsDraining() {
		w.WriteHeader(http.StatusServiceUnavailable)
		writeText(w, "NOT READY")
		return
	}
	w.WriteHeader(http.StatusOK)
	writeText(w, "READY")
}

type StartQueryPayload struct {
	DatabaseID string `json:"database_id"`
	SQL        string `json:"sql"`
	Options    struct {
		TimeoutMs        int64  `json:"timeout_ms"`
		Mode             string `json:"mode"`
		ResultTTLSeconds int64  `json:"result_ttl_seconds"`
		ResultFormat     string `json:"result_format"`
		StorageBackend   string `json:"storage_backend"`
	} `json:"options"`
}

func (s *Server) handleStartQuery(w http.ResponseWriter, r *http.Request) {
	// The SQL text is unbounded without this: a single request could otherwise
	// buffer as much memory as the client cares to send.
	r.Body = http.MaxBytesReader(w, r.Body, s.opts.MaxRequestBytes)

	var payload StartQueryPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeStatus(w, r, http.StatusRequestEntityTooLarge,
				fmt.Sprintf("request body exceeds %d bytes", s.opts.MaxRequestBytes))
			return
		}
		writeValidationError(w, r, "body", "malformed JSON")
		return
	}

	if payload.DatabaseID == "" {
		writeValidationError(w, r, "database_id", "is required")
		return
	}
	if payload.SQL == "" {
		writeValidationError(w, r, "sql", "is required")
		return
	}

	opts := domain.QueryOptions{
		Timeout:        time.Duration(payload.Options.TimeoutMs) * time.Millisecond,
		Mode:           payload.Options.Mode,
		ResultTTL:      time.Duration(payload.Options.ResultTTLSeconds) * time.Second,
		ResultFormat:   payload.Options.ResultFormat,
		StorageBackend: payload.Options.StorageBackend,
	}

	// Idempotency key from header
	if idemKey := r.Header.Get("Idempotency-Key"); idemKey != "" {
		opts.IdempotencyKey = idemKey
	}

	record, err := s.svc.StartQuery(r.Context(), payload.DatabaseID, payload.SQL, opts)
	if err != nil {
		writeError(w, r, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if opts.Mode == "sync" {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusAccepted)
	}
	writeJSON(w, toRecordDTO(record))
}

func (s *Server) handleGetQueryStatus(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		writeValidationError(w, r, "id", "is required")
		return
	}

	record, err := s.svc.GetQueryStatus(r.Context(), id)
	if err != nil {
		writeError(w, r, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	writeJSON(w, toRecordDTO(record))
}

func (s *Server) handleStopQuery(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		writeValidationError(w, r, "id", "is required")
		return
	}

	if err := s.svc.StopQuery(r.Context(), id); err != nil {
		writeError(w, r, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	writeJSON(w, map[string]string{
		"query_id": id,
		"status":   "STOPPED",
	})
}

func (s *Server) handleGetQueryStats(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		writeValidationError(w, r, "id", "is required")
		return
	}

	stats, err := s.svc.GetQueryStats(r.Context(), id)
	if err != nil {
		writeError(w, r, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	writeJSON(w, toStatsDTO(stats))
}

func (s *Server) handleDownloadResult(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		writeValidationError(w, r, "id", "is required")
		return
	}

	var offset, limit int64
	useRange := false
	rangeStart, rangeEnd := int64(0), int64(0)

	if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
		start, end, ok := parseByteRange(rangeHeader)
		if !ok {
			writeStatus(w, r, http.StatusRequestedRangeNotSatisfiable, "invalid Range header")
			return
		}
		offset = start
		rangeStart = start
		rangeEnd = end
		if end >= 0 {
			limit = end - start + 1
		}
		useRange = true
	} else {
		var err error
		if offsetStr := r.URL.Query().Get("offset"); offsetStr != "" {
			if offset, err = strconv.ParseInt(offsetStr, 10, 64); err != nil || offset < 0 {
				writeValidationError(w, r, "offset", "must be a non-negative integer")
				return
			}
		}
		if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
			if limit, err = strconv.ParseInt(limitStr, 10, 64); err != nil || limit < 0 {
				writeValidationError(w, r, "limit", "must be a non-negative integer")
				return
			}
		}
	}

	reader, ref, err := s.svc.DownloadResult(r.Context(), id, offset, limit)
	if err != nil {
		writeError(w, r, err)
		return
	}
	defer func() {
		if cerr := reader.Close(); cerr != nil {
			log.Printf("ERROR: failed to close result reader: %v", cerr)
		}
	}()

	contentType := "application/octet-stream"
	switch ref.Format {
	case "csv":
		contentType = "text/csv"
	case "jsonl":
		contentType = "application/x-jsonlines"
	}

	w.Header().Set("Content-Type", contentType)

	if useRange {
		total := ref.SizeBytes
		// A range that starts past the end is unsatisfiable, not a 206 with an
		// empty body, and RFC 9110 requires the total in Content-Range here.
		if total >= 0 && rangeStart >= total {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", total))
			writeStatus(w, r, http.StatusRequestedRangeNotSatisfiable, "range not satisfiable")
			return
		}
		end := rangeEnd
		if end < 0 || end >= total {
			end = total - 1
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", rangeStart, end, total))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	if _, err = io.Copy(w, reader); err != nil {
		log.Printf("ERROR: Failed during result streaming download: %v", err)
	}
}

// parseByteRange parses an HTTP "Range: bytes=N-M" header.
// Returns (start, end, ok). end == -1 means open-ended (bytes=N-).
func parseByteRange(header string) (start, end int64, ok bool) {
	if !strings.HasPrefix(header, "bytes=") {
		return 0, 0, false
	}
	spec := strings.TrimPrefix(header, "bytes=")
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	s, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || s < 0 {
		return 0, 0, false
	}
	if parts[1] == "" {
		return s, -1, true
	}
	e, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || e < s {
		return 0, 0, false
	}
	return s, e, true
}

func (s *Server) handleListDatabases(w http.ResponseWriter, r *http.Request) {
	dbs, err := s.svc.ListDatabases(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	writeJSON(w, dbs)
}

func (s *Server) handleReloadConfig(w http.ResponseWriter, r *http.Request) {
	report, err := s.svc.ReloadConfig(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	writeJSON(w, map[string]any{
		"success": true,
		"message": "Config reloaded successfully",
		"report":  report,
	})
}

func (s *Server) handleCanIBeStopped(w http.ResponseWriter, r *http.Request) {
	canStop, inFlight := s.svc.CanIBeStopped(r.Context())
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	writeJSON(w, map[string]any{
		"can_be_stopped": canStop,
		"in_flight":      inFlight,
	})
}

// writeJSON encodes v into the response body. The status line is already sent
// by the time encoding runs, so a failure can only be logged.
func writeJSON(w http.ResponseWriter, v any) {
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("ERROR: failed to encode JSON response: %v", err)
	}
}

// writeText writes a plain-text body, used by the health and readiness probes.
func writeText(w http.ResponseWriter, body string) {
	if _, err := io.WriteString(w, body); err != nil {
		log.Printf("ERROR: failed to write response body: %v", err)
	}
}
