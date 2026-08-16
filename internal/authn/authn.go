// Package authn authenticates API callers and carries the resulting identity
// through the request context.
//
// It lives outside internal/transport because both the transports (which check
// credentials) and the core (which stamps a query with the subject that
// submitted it) need the identity type, and core must not import a transport.
package authn

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
)

// Scope names a permission an endpoint requires.
type Scope string

const (
	// ScopeRead covers status, stats, download, list and watch.
	ScopeRead Scope = "read"
	// ScopeWrite covers submitting and stopping queries.
	ScopeWrite Scope = "write"
	// ScopeAdmin covers reload and can-stop, and sees every subject's queries.
	ScopeAdmin Scope = "admin"
)

// ValidScopes lists the scopes that can be configured.
var ValidScopes = []Scope{ScopeRead, ScopeWrite, ScopeAdmin}

// ErrUnauthenticated is returned when a request carries no usable credential.
var ErrUnauthenticated = errors.New("missing or invalid credentials")

// ErrForbidden is returned when the caller is known but lacks the scope.
var ErrForbidden = errors.New("insufficient scope")

// TokenSpec is the configured form of a static bearer token. The value is
// normally taken from the environment so it never has to live in a ConfigMap.
type TokenSpec struct {
	Subject  string
	Value    string
	ValueEnv string
	Scopes   []string
}

type token struct {
	value   []byte
	subject string
	scopes  []Scope
}

// Identity is what a successful authentication yields.
type Identity struct {
	Subject string
	Scopes  []Scope
}

// Has reports whether the identity carries a scope. Admin implies every scope,
// and write implies read: a token that may submit a query has to be able to
// poll it and fetch its result, otherwise only mode: sync is usable.
func (i Identity) Has(s Scope) bool {
	if slices.Contains(i.Scopes, ScopeAdmin) {
		return true
	}
	if s == ScopeRead && slices.Contains(i.Scopes, ScopeWrite) {
		return true
	}
	return slices.Contains(i.Scopes, s)
}

// IsAdmin reports whether the identity may act across subjects.
func (i Identity) IsAdmin() bool {
	return slices.Contains(i.Scopes, ScopeAdmin)
}

// Authenticator validates bearer tokens.
type Authenticator struct {
	tokens []token
}

// New builds an Authenticator from the configured specs. It fails rather than
// starting up unprotected: a token list that resolves to nothing is a
// configuration mistake, and silently accepting every request because an
// environment variable was not set is exactly the failure mode to avoid.
func New(specs []TokenSpec) (*Authenticator, error) {
	if len(specs) == 0 {
		return nil, errors.New("auth is configured but no tokens are defined")
	}

	a := &Authenticator{}
	seen := make(map[string]struct{}, len(specs))

	for i, spec := range specs {
		value := spec.Value
		if spec.ValueEnv != "" {
			value = os.Getenv(spec.ValueEnv)
			if value == "" {
				return nil, fmt.Errorf("auth token %d (%s): environment variable %s is empty", i, spec.Subject, spec.ValueEnv)
			}
		}
		if value == "" {
			return nil, fmt.Errorf("auth token %d (%s): no value or value_env", i, spec.Subject)
		}
		if _, dup := seen[value]; dup {
			return nil, fmt.Errorf("auth token %d (%s): duplicate value", i, spec.Subject)
		}
		seen[value] = struct{}{}

		if spec.Subject == "" {
			return nil, fmt.Errorf("auth token %d: subject must not be empty", i)
		}
		if len(spec.Scopes) == 0 {
			return nil, fmt.Errorf("auth token %d (%s): at least one scope is required", i, spec.Subject)
		}

		scopes := make([]Scope, 0, len(spec.Scopes))
		for _, raw := range spec.Scopes {
			s := Scope(raw)
			if !slices.Contains(ValidScopes, s) {
				return nil, fmt.Errorf("auth token %d (%s): unknown scope %q", i, spec.Subject, raw)
			}
			scopes = append(scopes, s)
		}

		a.tokens = append(a.tokens, token{value: []byte(value), subject: spec.Subject, scopes: scopes})
	}

	return a, nil
}

// Authenticate resolves a credential to an identity. Every configured token is
// compared, in constant time and without an early exit, so neither the time nor
// the control flow reveals which token was close to matching.
func (a *Authenticator) Authenticate(credential string) (Identity, error) {
	given := []byte(credential)
	match := -1

	for i, t := range a.tokens {
		if subtle.ConstantTimeCompare(t.value, given) == 1 {
			match = i
		}
	}

	if match < 0 {
		return Identity{}, ErrUnauthenticated
	}
	return Identity{Subject: a.tokens[match].subject, Scopes: a.tokens[match].scopes}, nil
}

// BearerToken extracts a credential from an Authorization header value.
func BearerToken(header string) string {
	const prefix = "Bearer "
	if len(header) < len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(header[len(prefix):])
}

type contextKey struct{}

// WithIdentity attaches an identity to a context.
func WithIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, contextKey{}, id)
}

// FromContext returns the identity attached to ctx. The second result is false
// when authentication is disabled, which callers treat as "no restriction"
// rather than as "deny": the deployment chose to run without credentials.
func FromContext(ctx context.Context) (Identity, bool) {
	id, ok := ctx.Value(contextKey{}).(Identity)
	return id, ok
}

// AuthorizeSubject reports whether the caller may act on a resource owned by
// owner. Admins act across subjects; a record with no owner predates subject
// binding and is treated as admin-only.
func AuthorizeSubject(ctx context.Context, owner string) error {
	id, ok := FromContext(ctx)
	if !ok {
		// Authentication is disabled: there is no subject to compare against.
		return nil
	}
	if id.IsAdmin() {
		return nil
	}
	if owner != "" && owner == id.Subject {
		return nil
	}
	return ErrForbidden
}

// SubjectFromContext returns the caller's subject, or "" when authentication is
// disabled.
func SubjectFromContext(ctx context.Context) string {
	id, ok := FromContext(ctx)
	if !ok {
		return ""
	}
	return id.Subject
}
