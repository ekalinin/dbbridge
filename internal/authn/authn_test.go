package authn

import (
	"context"
	"errors"
	"testing"
)

func mustNew(t *testing.T, specs ...TokenSpec) *Authenticator {
	t.Helper()
	a, err := New(specs)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return a
}

func TestNewRejectsUnusableConfigurations(t *testing.T) {
	cases := []struct {
		name  string
		specs []TokenSpec
	}{
		{"no tokens", nil},
		{"no value", []TokenSpec{{Subject: "a", Scopes: []string{"read"}}}},
		{"no subject", []TokenSpec{{Value: "t", Scopes: []string{"read"}}}},
		{"no scopes", []TokenSpec{{Subject: "a", Value: "t"}}},
		{"unknown scope", []TokenSpec{{Subject: "a", Value: "t", Scopes: []string{"root"}}}},
		{"empty env", []TokenSpec{{Subject: "a", ValueEnv: "DBBRIDGE_TEST_UNSET_TOKEN", Scopes: []string{"read"}}}},
		{"duplicate values", []TokenSpec{
			{Subject: "a", Value: "t", Scopes: []string{"read"}},
			{Subject: "b", Value: "t", Scopes: []string{"read"}},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(tc.specs); err == nil {
				t.Fatal("New accepted an unusable configuration")
			}
		})
	}
}

func TestNewReadsValueFromEnv(t *testing.T) {
	t.Setenv("DBBRIDGE_TEST_TOKEN", "s3cret")
	a := mustNew(t, TokenSpec{Subject: "svc", ValueEnv: "DBBRIDGE_TEST_TOKEN", Scopes: []string{"read"}})

	id, err := a.Authenticate("s3cret")
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if id.Subject != "svc" {
		t.Errorf("subject = %q, want svc", id.Subject)
	}
}

func TestAuthenticate(t *testing.T) {
	a := mustNew(t,
		TokenSpec{Subject: "reader", Value: "r-token", Scopes: []string{"read"}},
		TokenSpec{Subject: "root", Value: "a-token", Scopes: []string{"admin"}},
	)

	if _, err := a.Authenticate("nope"); !errors.Is(err, ErrUnauthenticated) {
		t.Errorf("Authenticate(bad) error = %v, want ErrUnauthenticated", err)
	}
	if _, err := a.Authenticate(""); !errors.Is(err, ErrUnauthenticated) {
		t.Errorf("Authenticate(empty) error = %v, want ErrUnauthenticated", err)
	}

	reader, err := a.Authenticate("r-token")
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if !reader.Has(ScopeRead) {
		t.Error("reader lacks the read scope")
	}
	if reader.Has(ScopeWrite) || reader.Has(ScopeAdmin) {
		t.Error("reader gained a scope it was not granted")
	}

	root, err := a.Authenticate("a-token")
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	// Admin implies the rest, so an admin token does not need read and write
	// spelled out in the config.
	if !root.Has(ScopeRead) || !root.Has(ScopeWrite) || !root.IsAdmin() {
		t.Error("admin token does not imply the other scopes")
	}
}

func TestBearerToken(t *testing.T) {
	cases := map[string]string{
		"Bearer abc":  "abc",
		"bearer abc":  "abc",
		"Bearer  abc": "abc",
		"Basic abc":   "",
		"abc":         "",
		"":            "",
	}
	for header, want := range cases {
		if got := BearerToken(header); got != want {
			t.Errorf("BearerToken(%q) = %q, want %q", header, got, want)
		}
	}
}

func TestAuthorizeSubject(t *testing.T) {
	owner := Identity{Subject: "alice", Scopes: []Scope{ScopeRead}}
	other := Identity{Subject: "bob", Scopes: []Scope{ScopeRead}}
	root := Identity{Subject: "root", Scopes: []Scope{ScopeAdmin}}

	// Authentication disabled: no subject to compare, nothing to restrict.
	if err := AuthorizeSubject(context.Background(), "alice"); err != nil {
		t.Errorf("AuthorizeSubject without an identity = %v, want nil", err)
	}

	if err := AuthorizeSubject(WithIdentity(context.Background(), owner), "alice"); err != nil {
		t.Errorf("owner was denied its own query: %v", err)
	}
	if err := AuthorizeSubject(WithIdentity(context.Background(), other), "alice"); !errors.Is(err, ErrForbidden) {
		t.Errorf("a foreign subject reached the query: %v", err)
	}
	if err := AuthorizeSubject(WithIdentity(context.Background(), root), "alice"); err != nil {
		t.Errorf("admin was denied: %v", err)
	}
	// Records written before subject binding carry no owner and are admin-only.
	if err := AuthorizeSubject(WithIdentity(context.Background(), owner), ""); !errors.Is(err, ErrForbidden) {
		t.Errorf("a legacy record was readable by a non-admin: %v", err)
	}
	if err := AuthorizeSubject(WithIdentity(context.Background(), root), ""); err != nil {
		t.Errorf("admin was denied a legacy record: %v", err)
	}
}

// TestWriteImpliesRead: a token that may submit a query has to be able to poll
// it and fetch its result, otherwise only mode: sync works.
func TestWriteImpliesRead(t *testing.T) {
	writer := Identity{Subject: "alice", Scopes: []Scope{ScopeWrite}}
	if !writer.Has(ScopeRead) {
		t.Error("a write token cannot read its own query")
	}
	reader := Identity{Subject: "watcher", Scopes: []Scope{ScopeRead}}
	if reader.Has(ScopeWrite) {
		t.Error("a read token was allowed to write")
	}
	if reader.Has(ScopeAdmin) {
		t.Error("a read token was allowed to administer")
	}
}
