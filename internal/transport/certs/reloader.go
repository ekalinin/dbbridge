// Package certs keeps a TLS certificate pair in step with the files it was
// loaded from.
//
// http.Server.ListenAndServeTLS reads the pair once and holds it for the life
// of the process. Certificates are renewed on a schedule - cert-manager rotates
// the Secret a day before expiry - and the listeners are not rebuilt by a
// config reload, so the pod kept serving the old certificate until something
// restarted it, and served nothing at all once that certificate expired.
package certs

import (
	"crypto/tls"
	"fmt"
	"os"
	"sync"
	"time"
)

// Reloader loads a certificate pair and re-reads it when either file changes on
// disk.
type Reloader struct {
	certFile string
	keyFile  string

	mu      sync.RWMutex
	cert    *tls.Certificate
	certMod time.Time
	keyMod  time.Time
}

// NewReloader loads the pair once so a bad path or an unreadable key is a
// startup failure rather than an error from inside a listener goroutine, after
// the other listeners are already up and the deferred cleanups can no longer
// run.
func NewReloader(certFile, keyFile string) (*Reloader, error) {
	r := &Reloader{certFile: certFile, keyFile: keyFile}
	if err := r.reload(); err != nil {
		return nil, err
	}
	return r, nil
}

// TLSConfig returns a config that hands out the current certificate.
func (r *Reloader) TLSConfig() *tls.Config {
	return &tls.Config{
		MinVersion:     tls.VersionTLS12,
		GetCertificate: r.getCertificate,
	}
}

func (r *Reloader) getCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	if r.changed() {
		if err := r.reload(); err != nil {
			// Keep serving the pair already in hand: a half-written renewal is
			// a worse reason to stop answering than a stale certificate.
			r.mu.RLock()
			defer r.mu.RUnlock()
			if r.cert != nil {
				return r.cert, nil
			}
			return nil, err
		}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cert, nil
}

// changed reports whether either file has a different modification time than
// the one the loaded pair came from.
func (r *Reloader) changed() bool {
	certInfo, err := os.Stat(r.certFile)
	if err != nil {
		return false
	}
	keyInfo, err := os.Stat(r.keyFile)
	if err != nil {
		return false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return !certInfo.ModTime().Equal(r.certMod) || !keyInfo.ModTime().Equal(r.keyMod)
}

func (r *Reloader) reload() error {
	certInfo, err := os.Stat(r.certFile)
	if err != nil {
		return fmt.Errorf("failed to stat the TLS certificate: %w", err)
	}
	keyInfo, err := os.Stat(r.keyFile)
	if err != nil {
		return fmt.Errorf("failed to stat the TLS key: %w", err)
	}
	cert, err := tls.LoadX509KeyPair(r.certFile, r.keyFile)
	if err != nil {
		return fmt.Errorf("failed to load the TLS key pair: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.cert = &cert
	r.certMod = certInfo.ModTime()
	r.keyMod = keyInfo.ModTime()
	return nil
}
