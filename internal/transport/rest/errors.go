package rest

import (
	"errors"
	"log"
	"net/http"

	"github.com/ekalinin/dbbridge/internal/core/domain"
	"github.com/ekalinin/dbbridge/internal/state"

	"github.com/go-chi/chi/v5/middleware"
)

// errorResponse is the single error shape the API returns.
type errorResponse struct {
	Error     string `json:"error"`
	RequestID string `json:"request_id,omitempty"`
}

// writeError maps a domain error to a status code and a response body that
// carries no internal detail.
//
// Everything except draining used to come back as 500 with err.Error() in the
// body. That text included the wrapped driver error, and a pgx or mysql
// connection failure spells out the host, the user and the connection
// parameters. The full error goes to the log, keyed by request ID; the client
// gets the category and, for input errors, the reason.
func writeError(w http.ResponseWriter, r *http.Request, err error) {
	status, message := classify(err)
	if status >= http.StatusInternalServerError {
		log.Printf("ERROR: request_id=%s %s %s: %v", middleware.GetReqID(r.Context()), r.Method, r.URL.Path, err)
	}
	writeStatus(w, r, status, message)
}

// writeStatus emits the error envelope with an explicit status.
func writeStatus(w http.ResponseWriter, r *http.Request, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	writeJSON(w, errorResponse{Error: message, RequestID: middleware.GetReqID(r.Context())})
}

func classify(err error) (int, string) {
	switch {
	case err == nil:
		return http.StatusOK, ""
	case isType[domain.ValidationError](err):
		return http.StatusBadRequest, err.Error()
	case isType[domain.NotFoundError](err), errors.Is(err, state.ErrNotFound):
		return http.StatusNotFound, "not found"
	case isType[domain.DrainingError](err):
		return http.StatusServiceUnavailable, err.Error()
	case isType[domain.UnavailableError](err):
		return http.StatusServiceUnavailable, err.Error()
	case isType[domain.ResourceExhaustedError](err):
		return http.StatusTooManyRequests, err.Error()
	default:
		return http.StatusInternalServerError, "internal error"
	}
}

func isType[T error](err error) bool {
	_, ok := errors.AsType[T](err)
	return ok
}

// writeValidationError is the shorthand for input rejected by the transport
// itself, before the request reaches the service.
func writeValidationError(w http.ResponseWriter, r *http.Request, field, reason string) {
	writeError(w, r, domain.ValidationError{Field: field, Reason: reason})
}
