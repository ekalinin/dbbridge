package ratelimit

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ekalinin/dbbridge/internal/authn"
)

func TestLimiterAllowsUpToBurstThenRefuses(t *testing.T) {
	l := New(0.0001, 3) // effectively no refill during the test

	for i := range 3 {
		if !l.Allow("a") {
			t.Fatalf("request %d within the burst was refused", i)
		}
	}
	if l.Allow("a") {
		t.Error("a request past the burst was allowed")
	}

	// Budgets are per key, so one caller cannot starve another.
	if !l.Allow("b") {
		t.Error("a different key was refused")
	}
}

func TestNilLimiterAllowsEverything(t *testing.T) {
	var l *Limiter
	for range 100 {
		if !l.Allow("a") {
			t.Fatal("a disabled limiter refused a request")
		}
	}
	if New(0, 10) != nil {
		t.Error("New with rps 0 should disable limiting")
	}
}

func TestKeyOfPrefersSubject(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/v1/databases", nil)
	r.RemoteAddr = "203.0.113.7:5555"

	if got := KeyOf(r); got != "addr:203.0.113.7" {
		t.Errorf("KeyOf without an identity = %q, want addr:203.0.113.7", got)
	}

	authed := r.WithContext(authn.WithIdentity(r.Context(), authn.Identity{Subject: "alice"}))
	if got := KeyOf(authed); got != "subject:alice" {
		t.Errorf("KeyOf with an identity = %q, want subject:alice", got)
	}
}
