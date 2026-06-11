package rest

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"dbbridge/internal/core/domain"
	"dbbridge/internal/core/service"
	"dbbridge/internal/telemetry"
	"dbbridge/internal/transport/ws"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

type Server struct {
	svc    *service.QueryService
	wsHub  *ws.Hub
	router chi.Router
}

func NewServer(svc *service.QueryService) *Server {
	s := &Server{
		svc:    svc,
		wsHub:  ws.NewHub(svc),
		router: chi.NewRouter(),
	}
	s.setupRoutes()
	return s
}

func (s *Server) Handler() http.Handler {
	return s.router
}

func (s *Server) setupRoutes() {
	s.router.Use(middleware.RequestID)
	s.router.Use(middleware.ClientIPFromXFF())
	s.router.Use(middleware.Logger)
	s.router.Use(middleware.Recoverer)
	s.router.Use(middleware.Timeout(60 * time.Second))

	s.router.Get("/healthz", s.handleHealthz)
	s.router.Get("/readyz", s.handleReadyz)
	s.router.Handle("/metrics", telemetry.Handler())

	s.router.Route("/v1", func(r chi.Router) {
		r.Get("/databases", s.handleListDatabases)
		r.Post("/queries", s.handleStartQuery)
		r.Get("/queries/{id}", s.handleGetQueryStatus)
		r.Post("/queries/{id}:stop", s.handleStopQuery)
		r.Get("/queries/{id}/stats", s.handleGetQueryStats)
		r.Get("/queries/{id}/result", s.handleDownloadResult)
		r.Get("/ws", s.wsHub.ServeHTTP)

		r.Post("/admin/reload", s.handleReloadConfig)
		r.Get("/admin/can-stop", s.handleCanIBeStopped)
	})
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	// Simple readiness: verify service is loaded
	if s.svc != nil {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("READY"))
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
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
	var payload StartQueryPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	if payload.DatabaseID == "" {
		http.Error(w, "database_id is required", http.StatusBadRequest)
		return
	}
	if payload.SQL == "" {
		http.Error(w, "sql query is required", http.StatusBadRequest)
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
		http.Error(w, "failed to start query: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if opts.Mode == "sync" {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusAccepted)
	}
	_ = json.NewEncoder(w).Encode(record)
}

func (s *Server) handleGetQueryStatus(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		http.Error(w, "query id is required", http.StatusBadRequest)
		return
	}

	record, err := s.svc.GetQueryStatus(r.Context(), id)
	if err != nil {
		http.Error(w, "query not found: "+err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(record)
}

func (s *Server) handleStopQuery(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		http.Error(w, "query id is required", http.StatusBadRequest)
		return
	}

	if err := s.svc.StopQuery(r.Context(), id); err != nil {
		http.Error(w, "failed to stop query: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"query_id": id,
		"status":   "STOPPED",
	})
}

func (s *Server) handleGetQueryStats(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		http.Error(w, "query id is required", http.StatusBadRequest)
		return
	}

	stats, err := s.svc.GetQueryStats(r.Context(), id)
	if err != nil {
		http.Error(w, "query stats not found: "+err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(stats)
}

func (s *Server) handleDownloadResult(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		http.Error(w, "query id is required", http.StatusBadRequest)
		return
	}

	var offset, limit int64
	useRange := false
	rangeStart, rangeEnd := int64(0), int64(0)

	if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
		start, end, ok := parseByteRange(rangeHeader)
		if !ok {
			http.Error(w, "invalid Range header", http.StatusRequestedRangeNotSatisfiable)
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
			if offset, err = strconv.ParseInt(offsetStr, 10, 64); err != nil {
				http.Error(w, "invalid offset: "+err.Error(), http.StatusBadRequest)
				return
			}
		}
		if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
			if limit, err = strconv.ParseInt(limitStr, 10, 64); err != nil {
				http.Error(w, "invalid limit: "+err.Error(), http.StatusBadRequest)
				return
			}
		}
	}

	reader, ref, err := s.svc.DownloadResult(r.Context(), id, offset, limit)
	if err != nil {
		http.Error(w, "failed to download results: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer reader.Close()

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
		totalStr := "*"
		if total > 0 {
			totalStr = strconv.FormatInt(total, 10)
		}
		end := rangeEnd
		if end < 0 || (total > 0 && end >= total) {
			end = total - 1
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%s", rangeStart, end, totalStr))
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
		http.Error(w, "failed to list databases: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(dbs)
}

func (s *Server) handleReloadConfig(w http.ResponseWriter, r *http.Request) {
	err := s.svc.ReloadConfig(r.Context())
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": false,
			"message": err.Error(),
		})
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"message": "Config reloaded successfully",
	})
}

func (s *Server) handleCanIBeStopped(w http.ResponseWriter, r *http.Request) {
	canStop, inFlight := s.svc.CanIBeStopped(r.Context())
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"can_be_stopped": canStop,
		"in_flight":      inFlight,
	})
}
