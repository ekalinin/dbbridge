package fs

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/ekalinin/dbbridge/internal/core/domain"
)

type FSResultStore struct {
	rootDir string
	root    *os.Root
}

func NewFSResultStore(rootDir string) (*FSResultStore, error) {
	if rootDir == "" {
		rootDir = "results"
	}
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create root results dir: %w", err)
	}
	// Every file operation goes through the root handle, so a traversal in a
	// file name or in a stored Locator cannot escape the results directory even
	// if it slips past the checks below.
	root, err := os.OpenRoot(rootDir)
	if err != nil {
		return nil, fmt.Errorf("failed to open root results dir: %w", err)
	}
	return &FSResultStore{rootDir: rootDir, root: root}, nil
}

// relative maps a stored Locator back to a path inside the root. Locators come
// from the MetaStore, which is a separate system: an attacker able to write
// there must not be able to make dbbridge read or delete an arbitrary file.
func (s *FSResultStore) relative(locator string) (string, error) {
	rel, err := filepath.Rel(s.rootDir, locator)
	if err != nil {
		return "", fmt.Errorf("result locator is outside the results directory")
	}
	if !filepath.IsLocal(rel) {
		return "", fmt.Errorf("result locator is outside the results directory")
	}
	return rel, nil
}

func (s *FSResultStore) Writer(ctx context.Context, queryID string, format string) (io.WriteCloser, domain.ResultRef, error) {
	filename := fmt.Sprintf("%s.%s", queryID, format)
	if !filepath.IsLocal(filename) {
		return nil, domain.ResultRef{}, fmt.Errorf("invalid result file name %q", filename)
	}

	file, err := s.root.Create(filename)
	if err != nil {
		return nil, domain.ResultRef{}, fmt.Errorf("failed to create results file: %w", err)
	}

	ref := domain.ResultRef{
		Backend: "fs",
		Locator: filepath.Join(s.rootDir, filename),
		Format:  format,
	}

	return file, ref, nil
}

func (s *FSResultStore) Reader(ctx context.Context, ref domain.ResultRef) (io.ReadCloser, error) {
	rel, err := s.relative(ref.Locator)
	if err != nil {
		return nil, err
	}
	file, err := s.root.Open(rel)
	if err != nil {
		return nil, fmt.Errorf("failed to open results file: %w", err)
	}
	return file, nil
}

func (s *FSResultStore) Stat(ctx context.Context, ref domain.ResultRef) (domain.ResultRef, error) {
	rel, err := s.relative(ref.Locator)
	if err != nil {
		return domain.ResultRef{}, err
	}
	info, err := s.root.Stat(rel)
	if err != nil {
		return domain.ResultRef{}, fmt.Errorf("failed to stat results file: %w", err)
	}
	ref.SizeBytes = info.Size()
	return ref, nil
}

func (s *FSResultStore) Delete(ctx context.Context, ref domain.ResultRef) error {
	rel, err := s.relative(ref.Locator)
	if err != nil {
		return err
	}
	if err := s.root.Remove(rel); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete results file: %w", err)
	}
	return nil
}

// Close releases the root handle.
func (s *FSResultStore) Close() error {
	return s.root.Close()
}
