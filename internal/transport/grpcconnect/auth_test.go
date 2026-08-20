package grpcconnect_test

import (
	"testing"

	"github.com/ekalinin/dbbridge/internal/authn"
	"github.com/ekalinin/dbbridge/internal/transport/grpcconnect"
)

func TestScopeForProcedure(t *testing.T) {
	cases := map[string]authn.Scope{
		"/dbbridge.v1.QueryService/StartQuery":     authn.ScopeWrite,
		"/dbbridge.v1.QueryService/GetQueryStatus": authn.ScopeRead,
		"/dbbridge.v1.QueryService/ReloadConfig":   authn.ScopeAdmin,
		// An RPC nobody mapped must not be reachable with a read token.
		"/dbbridge.v1.QueryService/SomethingNew": authn.ScopeAdmin,
	}
	for procedure, want := range cases {
		if got := grpcconnect.ScopeForProcedure(procedure); got != want {
			t.Errorf("ScopeForProcedure(%q) = %q, want %q", procedure, got, want)
		}
	}
}
