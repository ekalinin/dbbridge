package grpcconnect

import (
	"context"
	"net/http"
	"strings"

	"github.com/ekalinin/dbbridge/internal/authn"

	"connectrpc.com/connect"
)

// procedureScopes maps a Connect procedure name to the scope it requires.
var procedureScopes = map[string]authn.Scope{
	"StartQuery":     authn.ScopeWrite,
	"StopQuery":      authn.ScopeWrite,
	"GetQueryStatus": authn.ScopeRead,
	"GetQueryStats":  authn.ScopeRead,
	"DownloadResult": authn.ScopeRead,
	"ListDatabases":  authn.ScopeRead,
	"WatchQuery":     authn.ScopeRead,
	"ReloadConfig":   authn.ScopeAdmin,
	"CanIBeStopped":  authn.ScopeAdmin,
}

// ScopeForProcedure resolves the scope a Connect procedure requires. Unknown
// procedures default to admin, so a newly added RPC is never accidentally
// reachable with a read token.
func ScopeForProcedure(procedure string) authn.Scope {
	if idx := strings.LastIndex(procedure, "/"); idx >= 0 {
		procedure = procedure[idx+1:]
	}
	if s, ok := procedureScopes[procedure]; ok {
		return s
	}
	return authn.ScopeAdmin
}

// NewAuthInterceptor returns a Connect interceptor that enforces the same rules
// as the REST middleware. Streaming handlers are covered too: DownloadResult
// and WatchQuery are server streams and would otherwise stay open to anyone.
func NewAuthInterceptor(a *authn.Authenticator) connect.Interceptor {
	return interceptor{a: a}
}

type interceptor struct{ a *authn.Authenticator }

func (i interceptor) check(header http.Header, procedure string) (authn.Identity, error) {
	id, err := i.a.Authenticate(authn.BearerToken(header.Get("Authorization")))
	if err != nil {
		return authn.Identity{}, connect.NewError(connect.CodeUnauthenticated, authn.ErrUnauthenticated)
	}
	if !id.Has(ScopeForProcedure(procedure)) {
		return authn.Identity{}, connect.NewError(connect.CodePermissionDenied, authn.ErrForbidden)
	}
	return id, nil
}

func (i interceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		// Only inbound calls are authenticated; on the client side this
		// interceptor is a pass-through.
		if req.Spec().IsClient {
			return next(ctx, req)
		}
		id, err := i.check(req.Header(), req.Spec().Procedure)
		if err != nil {
			return nil, err
		}
		return next(authn.WithIdentity(ctx, id), req)
	}
}

func (i interceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (i interceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		id, err := i.check(conn.RequestHeader(), conn.Spec().Procedure)
		if err != nil {
			return err
		}
		return next(authn.WithIdentity(ctx, id), conn)
	}
}
