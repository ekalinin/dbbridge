package rest

import (
	"net/http"
	"strings"

	"github.com/ekalinin/dbbridge/internal/authn"
	"github.com/ekalinin/dbbridge/internal/transport/ws"
)

// credential extracts the caller's bearer token. A browser cannot set the
// Authorization header on a WebSocket handshake, so the subprotocol list is
// consulted as well; without it /v1/ws would be gated on a header exactly the
// clients the Origin check exists for cannot send. The query string is
// deliberately not consulted: it ends up in access logs and proxy history.
func credential(r *http.Request) string {
	if v := authn.BearerToken(r.Header.Get("Authorization")); v != "" {
		return v
	}
	return subprotocolCredential(r.Header.Values("Sec-WebSocket-Protocol"))
}

// subprotocolCredential returns the entry that follows the bearer marker in the
// offered subprotocol list.
func subprotocolCredential(values []string) string {
	var offered []string
	for _, v := range values {
		for part := range strings.SplitSeq(v, ",") {
			if part = strings.TrimSpace(part); part != "" {
				offered = append(offered, part)
			}
		}
	}
	for i, p := range offered {
		if p == ws.BearerSubprotocol && i+1 < len(offered) {
			return offered[i+1]
		}
	}
	return ""
}

// require returns the middleware that gates a route on a scope, or a
// pass-through when authentication is disabled.
//
// It is mounted with r.Use on a group rather than r.With on a route: a route
// registered without a gate would not merely skip the 401, it would read the
// records of every subject, because AuthorizeSubject treats a missing identity
// as "this deployment runs without credentials".
func (s *Server) require(scope authn.Scope) func(http.Handler) http.Handler {
	if s.opts.Auth == nil {
		return func(next http.Handler) http.Handler { return next }
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Responses depend on the caller now, so no intermediary may reuse
			// one between callers.
			w.Header().Add("Vary", "Authorization")

			id, err := s.opts.Auth.Authenticate(credential(r))
			if err != nil {
				w.Header().Set("WWW-Authenticate", `Bearer realm="dbbridge"`)
				// The same envelope as every other error: a client parses a 401
				// with the code it already has, and the request ID is kept.
				writeStatus(w, r, http.StatusUnauthorized, "unauthorized")
				return
			}
			if !id.Has(scope) {
				writeStatus(w, r, http.StatusForbidden, "forbidden")
				return
			}
			next.ServeHTTP(w, r.WithContext(authn.WithIdentity(r.Context(), id)))
		})
	}
}
