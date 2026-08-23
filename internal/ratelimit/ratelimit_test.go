package ratelimit

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5/middleware"
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

func TestKeyOfUsesClientAddr(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/v1/databases", nil)
	r.RemoteAddr = "203.0.113.7:5555"

	if got := KeyOf(r); got != "addr:203.0.113.7" {
		t.Errorf("KeyOf = %q, want addr:203.0.113.7", got)
	}

	// Behind a proxy the budget has to follow the client chi resolved, not the
	// hop every caller shares.
	r.Header.Set("X-Forwarded-For", "198.51.100.9")
	var got string
	middleware.ClientIPFromXFF()(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got = KeyOf(r)
	})).ServeHTTP(httptest.NewRecorder(), r)
	if got != "addr:198.51.100.9" {
		t.Errorf("KeyOf behind a proxy = %q, want addr:198.51.100.9", got)
	}
}
