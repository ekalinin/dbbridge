package certs

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writePair writes a self-signed pair with the given common name.
func writePair(t *testing.T, dir, commonName string) (string, string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}

	certPath := filepath.Join(dir, "tls.crt")
	keyPath := filepath.Join(dir, "tls.key")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write certificate: %v", err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certPath, keyPath
}

func commonNameOf(t *testing.T, r *Reloader) string {
	t.Helper()
	cert, err := r.getCertificate(nil)
	if err != nil {
		t.Fatalf("getCertificate: %v", err)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}
	return leaf.Subject.CommonName
}

// TestReloader_PicksUpARotatedPair: ListenAndServeTLS read the pair once, so a
// certificate renewed by cert-manager was ignored until the pod restarted, and
// once the old one expired every listener stopped answering.
func TestReloader_PicksUpARotatedPair(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := writePair(t, dir, "first")

	r, err := NewReloader(certPath, keyPath)
	if err != nil {
		t.Fatalf("NewReloader: %v", err)
	}
	if got := commonNameOf(t, r); got != "first" {
		t.Fatalf("common name = %q, want first", got)
	}

	// Rewrite the pair with a distinguishable one and move its mtime, the way
	// a renewal does.
	writePair(t, dir, "second")
	future := time.Now().Add(time.Minute)
	for _, p := range []string{certPath, keyPath} {
		if err := os.Chtimes(p, future, future); err != nil {
			t.Fatalf("chtimes: %v", err)
		}
	}

	if got := commonNameOf(t, r); got != "second" {
		t.Errorf("common name after rotation = %q, want second", got)
	}
}

// TestReloader_KeepsTheOldPairWhenTheNewOneIsBroken: a renewal caught halfway
// through is not a reason to stop answering.
func TestReloader_KeepsTheOldPairWhenTheNewOneIsBroken(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := writePair(t, dir, "first")

	r, err := NewReloader(certPath, keyPath)
	if err != nil {
		t.Fatalf("NewReloader: %v", err)
	}

	if err := os.WriteFile(certPath, []byte("not a certificate"), 0o600); err != nil {
		t.Fatalf("write certificate: %v", err)
	}
	future := time.Now().Add(time.Minute)
	if err := os.Chtimes(certPath, future, future); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	if got := commonNameOf(t, r); got != "first" {
		t.Errorf("common name = %q, want the previous pair to be kept", got)
	}
}

// TestNewReloader_FailsOnAMissingFile keeps a wrong path a startup failure
// rather than an error from inside a listener goroutine.
func TestNewReloader_FailsOnAMissingFile(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := writePair(t, dir, "first")

	if _, err := NewReloader(certPath+".nope", keyPath); err == nil {
		t.Error("a missing certificate path was accepted")
	}
	if _, err := NewReloader(certPath, keyPath+".nope"); err == nil {
		t.Error("a missing key path was accepted")
	}
}

// serveTLS starts an HTTPS listener that takes its certificate from r, and
// returns its base URL.
func serveTLS(t *testing.T, r *Reloader, h http.Handler) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &http.Server{Handler: h, TLSConfig: r.TLSConfig(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		// The paths are empty exactly as in serveOne: the pair comes from
		// TLSConfig.GetCertificate, not from the file arguments.
		_ = srv.ServeTLS(ln, "", "")
	}()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	return "https://" + ln.Addr().String()
}

// tlsGet performs one HTTPS request and reports the common name the server
// presented. The certificates here carry no SANs, so verification is skipped and
// the identity is read off the peer chain instead - which is what the test is
// actually asserting.
func tlsGet(t *testing.T, url string) (int, string) {
	t.Helper()
	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // self-signed pair, identity is asserted below
		DisableKeepAlives: true,
	}}
	defer client.CloseIdleConnections()

	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		t.Fatalf("read body: %v", err)
	}
	if resp.TLS == nil || len(resp.TLS.PeerCertificates) == 0 {
		t.Fatal("the response carried no peer certificate")
	}
	return resp.StatusCode, resp.TLS.PeerCertificates[0].Subject.CommonName
}

// TestReloader_ServesOverTLS covers what §9 Transport promises and no test
// reached: TLSConfig() actually driving a listener. Every other test in this
// file calls getCertificate directly, so the config the listeners are built
// from - and the handshake it has to satisfy - was never exercised.
func TestReloader_ServesOverTLS(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := writePair(t, dir, "first")

	r, err := NewReloader(certPath, keyPath)
	if err != nil {
		t.Fatalf("NewReloader: %v", err)
	}

	url := serveTLS(t, r, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	status, name := tlsGet(t, url)
	if status != http.StatusOK {
		t.Errorf("status = %d, want 200", status)
	}
	if name != "first" {
		t.Errorf("served common name = %q, want first", name)
	}
}

// TestReloader_ServesARotatedPairWithoutARestart is the property a renewal
// depends on: the same running listener starts presenting the new certificate.
// The unit test above it proves the Reloader notices the rotation; this one
// proves a live TLS handshake does.
func TestReloader_ServesARotatedPairWithoutARestart(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := writePair(t, dir, "first")

	r, err := NewReloader(certPath, keyPath)
	if err != nil {
		t.Fatalf("NewReloader: %v", err)
	}
	url := serveTLS(t, r, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	if _, name := tlsGet(t, url); name != "first" {
		t.Fatalf("served common name = %q, want first", name)
	}

	// cert-manager rewrites the Secret; the mtime moves with it.
	writePair(t, dir, "second")
	future := time.Now().Add(time.Minute)
	for _, p := range []string{certPath, keyPath} {
		if err := os.Chtimes(p, future, future); err != nil {
			t.Fatalf("chtimes: %v", err)
		}
	}

	status, name := tlsGet(t, url)
	if status != http.StatusOK {
		t.Errorf("status after rotation = %d, want 200", status)
	}
	if name != "second" {
		t.Errorf("served common name after rotation = %q, want second; the listener is still holding the old pair", name)
	}
}
