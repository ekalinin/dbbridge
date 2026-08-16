package certs

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
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
