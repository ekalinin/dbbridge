package fs

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"dbbridge/internal/core/domain"
)

func init() {
	// We will register it dynamically or statically.
	// Since we need config, we can register factories.
	// To make things simple, we register the instance after creating it from config.
}

type FSResultStore struct {
	rootDir string
}

func NewFSResultStore(rootDir string) (*FSResultStore, error) {
	if rootDir == "" {
		rootDir = "results"
	}
	if err := os.MkdirAll(rootDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create root results dir: %w", err)
	}
	return &FSResultStore{rootDir: rootDir}, nil
}

func (s *FSResultStore) Writer(ctx context.Context, queryID string, format string) (io.WriteCloser, domain.ResultRef, error) {
	filename := fmt.Sprintf("%s.%s", queryID, format)
	path := filepath.Join(s.rootDir, filename)

	file, err := os.Create(path)
	if err != nil {
		return nil, domain.ResultRef{}, fmt.Errorf("failed to create results file: %w", err)
	}

	ref := domain.ResultRef{
		Backend: "fs",
		Locator: path,
		Format:  format,
	}

	return file, ref, nil
}

func (s *FSResultStore) Reader(ctx context.Context, ref domain.ResultRef) (io.ReadCloser, error) {
	file, err := os.Open(ref.Locator)
	if err != nil {
		return nil, fmt.Errorf("failed to open results file: %w", err)
	}
	return file, nil
}

func (s *FSResultStore) Delete(ctx context.Context, ref domain.ResultRef) error {
	err := os.Remove(ref.Locator)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete results file: %w", err)
	}
	return nil
}
