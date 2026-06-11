package storage

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

type mockRowStream struct {
	cols []string
	rows [][]any
	idx  int
}

func (m *mockRowStream) Columns() ([]string, error) {
	return m.cols, nil
}

func (m *mockRowStream) Next() bool {
	if m.idx < len(m.rows) {
		m.idx++
		return true
	}
	return false
}

func (m *mockRowStream) Scan(dest ...any) error {
	if m.idx == 0 || m.idx > len(m.rows) {
		return errors.New("out of bounds")
	}
	row := m.rows[m.idx-1]
	for i, val := range row {
		if i >= len(dest) {
			break
		}
		ptr := dest[i].(*any)
		*ptr = val
	}
	return nil
}

func (m *mockRowStream) Err() error {
	return nil
}

func (m *mockRowStream) Close() error {
	return nil
}

func TestEncodeStreamCSV(t *testing.T) {
	stream := &mockRowStream{
		cols: []string{"id", "name", "active"},
		rows: [][]any{
			{1, "Alice", true},
			{2, "Bob", false},
		},
	}

	buf := &bytes.Buffer{}
	rowCount, bytesWritten, err := EncodeStream(context.Background(), stream, "csv", buf)
	if err != nil {
		t.Fatalf("unexpected error encoding CSV: %v", err)
	}

	if rowCount != 2 {
		t.Errorf("expected 2 rows; got %d", rowCount)
	}
	if bytesWritten == 0 {
		t.Error("expected non-zero bytes written")
	}

	expected := "id,name,active\n1,Alice,true\n2,Bob,false\n"
	if strings.ReplaceAll(buf.String(), "\r\n", "\n") != expected {
		t.Errorf("unexpected CSV content:\n%q\nexpected:\n%q", buf.String(), expected)
	}
}

func TestEncodeStreamJSONL(t *testing.T) {
	stream := &mockRowStream{
		cols: []string{"id", "name"},
		rows: [][]any{
			{101, "Alice"},
			{102, "Bob"},
		},
	}

	buf := &bytes.Buffer{}
	rowCount, bytesWritten, err := EncodeStream(context.Background(), stream, "jsonl", buf)
	if err != nil {
		t.Fatalf("unexpected error encoding JSONL: %v", err)
	}

	if rowCount != 2 {
		t.Errorf("expected 2 rows; got %d", rowCount)
	}
	if bytesWritten == 0 {
		t.Error("expected non-zero bytes written")
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 JSONL lines; got %d", len(lines))
	}

	expectedLine0 := `{"id":101,"name":"Alice"}`
	if lines[0] != expectedLine0 {
		t.Errorf("expected line 0: %q; got: %q", expectedLine0, lines[0])
	}
}
