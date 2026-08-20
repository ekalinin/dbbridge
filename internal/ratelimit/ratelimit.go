// Package ratelimit caps how fast a single caller can hit the API.
//
// The concurrency semaphore in the manager bounds how many queries run at once,
// but nothing bounded how fast they could be submitted, so a client could still
// churn through pool connections, storage writes and idempotency keys as fast
// as the process could reject or accept them.
package ratelimit

import (
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5/middleware"
	"golang.org/x/time/rate"

	"github.com/ekalinin/dbbridge/internal/authn"
)

// idleEviction is how long an unused limiter is kept before it is dropped, so
// the map cannot grow without bound on a busy or hostile deployment.
const idleEviction = 10 * time.Minute

type entry struct {
	limiter *rate.Limiter
	lastUse time.Time
}

// Limiter hands out one token bucket per key.
type Limiter struct {
	mu        sync.Mutex
	entries   map[string]*entry
	rps       rate.Limit
	burst     int
	lastPurge time.Time
}

// New returns a Limiter allowing rps requests per second per key with the given
// burst. A non-positive rps disables limiting and New returns nil, which every
// method below treats as "allow".
func New(rps float64, burst int) *Limiter {
	if rps <= 0 {
		return nil
	}
	if burst <= 0 {
		burst = 1
	}
	return &Limiter{
		entries: make(map[string]*entry),
		rps:     rate.Limit(rps),
		burst:   burst,
	}
}

// Allow reports whether a request from key may proceed.
func (l *Limiter) Allow(key string) bool {
	if l == nil {
		return true
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	l.purgeLocked(now)

	e, ok := l.entries[key]
	if !ok {
		e = &entry{limiter: rate.NewLimiter(l.rps, l.burst)}
		l.entries[key] = e
	}
	e.lastUse = now
	return e.limiter.Allow()
}

// purgeLocked drops limiters that have been idle for longer than idleEviction.
// It runs at most once per eviction window, so the sweep cost is amortized.
func (l *Limiter) purgeLocked(now time.Time) {
	if now.Sub(l.lastPurge) < idleEviction {
		return
	}
	l.lastPurge = now
	for key, e := range l.entries {
		if now.Sub(e.lastUse) > idleEviction {
			delete(l.entries, key)
		}
	}
}

// KeyOf identifies the caller a request should be counted against: the
// authenticated subject when there is one, the client address otherwise.
func KeyOf(r *http.Request) string {
	if subject := authn.SubjectFromContext(r.Context()); subject != "" {
		return "subject:" + subject
	}
	return "addr:" + clientAddr(r)
}

// clientAddr prefers the address chi's ClientIPFrom* middleware resolved, which
// already accounts for the configured number of trusted proxy hops.
func clientAddr(r *http.Request) string {
	if ip := middleware.GetClientIP(r.Context()); ip != "" {
		return ip
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
