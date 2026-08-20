package storage

import (
	"bytes"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// sliceStream is a RowStream over static rows, so the encoder can be exercised
// without a database.
type sliceStream struct {
	cols []string
	rows [][]any
	pos  int
	// onExhausted fires when the stream runs dry, which is the point an encoder
	// finalizes its output.
	onExhausted func()
}

func (s *sliceStream) Columns() ([]string, error) { return s.cols, nil }
func (s *sliceStream) Next() bool {
	s.pos++
	if s.pos >= len(s.rows) {
		if s.onExhausted != nil {
			s.onExhausted()
			s.onExhausted = nil
		}
		return false
	}
	return true
}
func (s *sliceStream) Scan(dest ...any) error {
	for i, d := range dest {
		if p, ok := d.(*any); ok {
			*p = s.rows[s.pos][i]
		}
	}
	return nil
}
func (s *sliceStream) Err() error   { return nil }
func (s *sliceStream) Close() error { return nil }

func newSliceStream(cols []string, rows [][]any) *sliceStream {
	return &sliceStream{cols: cols, rows: rows, pos: -1}
}

// TestEncodeStream_ParquetIsReadable is the point of the format: the file used
// to contain JSONL under a .parquet name, so nothing could open it.
func TestEncodeStream_ParquetIsReadable(t *testing.T) {
	ts := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	stream := newSliceStream(
		[]string{"id", "name", "ratio", "active", "created"},
		[][]any{
			{int64(1), "alice", 1.5, true, ts},
			{int64(2), nil, 2.5, false, ts.Add(time.Hour)},
		},
	)

	var buf bytes.Buffer
	rows, bytesWritten, err := EncodeStream(t.Context(), stream, "parquet", &buf)
	if err != nil {
		t.Fatalf("EncodeStream: %v", err)
	}
	if rows != 2 {
		t.Errorf("rows = %d, want 2", rows)
	}
	if bytesWritten != int64(buf.Len()) {
		t.Errorf("reported %d bytes, buffer holds %d", bytesWritten, buf.Len())
	}
	if got := buf.Bytes()[:4]; string(got) != "PAR1" {
		t.Fatalf("file does not start with the parquet magic: %q", got)
	}

	file, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if got := file.NumRows(); got != 2 {
		t.Fatalf("NumRows = %d, want 2", got)
	}

	fields := file.Schema().Fields()
	byName := make(map[string]int, len(fields))
	for i, f := range fields {
		byName[f.Name()] = i
	}
	for _, want := range []string{"id", "name", "ratio", "active", "created"} {
		if _, ok := byName[want]; !ok {
			t.Errorf("schema is missing column %q", want)
		}
	}

	reader := parquet.NewGenericReader[any](bytes.NewReader(buf.Bytes()))
	defer func() {
		if err := reader.Close(); err != nil {
			t.Errorf("close reader: %v", err)
		}
	}()

	parsed := make([]parquet.Row, 2)
	if _, err := reader.ReadRows(parsed); err != nil && err.Error() != "EOF" {
		t.Fatalf("ReadRows: %v", err)
	}

	if got := parsed[0][byName["id"]].Int64(); got != 1 {
		t.Errorf("row 0 id = %d, want 1", got)
	}
	if got := string(parsed[0][byName["name"]].ByteArray()); got != "alice" {
		t.Errorf("row 0 name = %q, want alice", got)
	}
	if got := parsed[0][byName["ratio"]].Double(); got != 1.5 {
		t.Errorf("row 0 ratio = %v, want 1.5", got)
	}
	if !parsed[0][byName["active"]].Boolean() {
		t.Error("row 0 active = false, want true")
	}
	// SQL NULL has to survive the round trip.
	if !parsed[1][byName["name"]].IsNull() {
		t.Errorf("row 1 name = %v, want null", parsed[1][byName["name"]])
	}
	if got := parsed[0][byName["created"]].Int64(); got != ts.UnixMicro() {
		t.Errorf("row 0 created = %d, want %d", got, ts.UnixMicro())
	}
}

// TestEncodeStream_ParquetBeyondSchemaSample covers the streaming path past the
// bounded head that the schema is inferred from.
func TestEncodeStream_ParquetBeyondSchemaSample(t *testing.T) {
	const total = parquetSchemaSample * 2
	rows := make([][]any, 0, total)
	for i := range total {
		rows = append(rows, []any{int64(i)})
	}

	var buf bytes.Buffer
	got, _, err := EncodeStream(t.Context(), newSliceStream([]string{"n"}, rows), "parquet", &buf)
	if err != nil {
		t.Fatalf("EncodeStream: %v", err)
	}
	if got != total {
		t.Fatalf("rows = %d, want %d", got, total)
	}

	file, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if file.NumRows() != total {
		t.Errorf("NumRows = %d, want %d", file.NumRows(), total)
	}
}

func TestUniqueColumnNames(t *testing.T) {
	cases := []struct {
		in   []string
		want []string
	}{
		{[]string{"a", "a", "", "a"}, []string{"a", "a_1", "column_2", "a_2"}},
		// The generated suffix used to be handed out without checking it
		// against the real column names, so these collapsed to two fields.
		{[]string{"a", "a", "a_1"}, []string{"a", "a_1", "a_1_1"}},
		{[]string{"a", "a_1", "a"}, []string{"a", "a_1", "a_2"}},
	}
	for _, tc := range cases {
		got := uniqueColumnNames(tc.in)
		if len(got) != len(tc.want) {
			t.Fatalf("uniqueColumnNames(%v) = %v, want %v", tc.in, got, tc.want)
		}
		for i := range tc.want {
			if got[i] != tc.want[i] {
				t.Errorf("uniqueColumnNames(%v)[%d] = %q, want %q", tc.in, i, got[i], tc.want[i])
			}
		}
	}
}

// TestEncodeStream_ParquetDuplicateNamesCollide is the regression for the panic
// that took the whole process down: SELECT 1 AS a, 2 AS a, 3 AS a_1 built a
// schema one field short of the row it was writing, and parquet-go indexed past
// the end of the column slice. Any client with the write scope could send it.
func TestEncodeStream_ParquetDuplicateNamesCollide(t *testing.T) {
	stream := newSliceStream(
		[]string{"a", "a", "a_1"},
		[][]any{{int64(1), int64(2), int64(3)}},
	)

	var buf bytes.Buffer
	rows, _, err := EncodeStream(t.Context(), stream, "parquet", &buf)
	if err != nil {
		t.Fatalf("EncodeStream: %v", err)
	}
	if rows != 1 {
		t.Fatalf("rows = %d, want 1", rows)
	}

	file, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if got := len(file.Schema().Fields()); got != 3 {
		t.Fatalf("schema has %d fields, want one per result column", got)
	}
}

// TestEncodeStream_ParquetTypeMismatchFails: a value that does not fit the type
// the sample inferred used to be written as NULL, so the query succeeded and
// handed back a file that opens, looks valid and has values missing.
func TestEncodeStream_ParquetTypeMismatchFails(t *testing.T) {
	rows := make([][]any, 0, parquetSchemaSample+1)
	for i := range parquetSchemaSample {
		rows = append(rows, []any{int64(i)})
	}
	rows = append(rows, []any{"not-a-number"})

	var buf bytes.Buffer
	if _, _, err := EncodeStream(t.Context(), newSliceStream([]string{"n"}, rows), "parquet", &buf); err == nil {
		t.Error("a value outside the inferred column type was accepted")
	}
}

// TestEncodeStream_ParquetMixedTypesFallBackToText covers the documented
// fallback: a column whose sampled values disagree is stored as text rather
// than losing the values that do not fit.
func TestEncodeStream_ParquetMixedTypesFallBackToText(t *testing.T) {
	stream := newSliceStream(
		[]string{"v"},
		[][]any{{int64(1)}, {"two"}, {3.5}},
	)

	var buf bytes.Buffer
	rows, _, err := EncodeStream(t.Context(), stream, "parquet", &buf)
	if err != nil {
		t.Fatalf("EncodeStream: %v", err)
	}
	if rows != 3 {
		t.Fatalf("rows = %d, want 3", rows)
	}

	reader := parquet.NewGenericReader[any](bytes.NewReader(buf.Bytes()))
	defer func() {
		if err := reader.Close(); err != nil {
			t.Errorf("close reader: %v", err)
		}
	}()
	parsed := make([]parquet.Row, 3)
	n, err := reader.ReadRows(parsed)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("ReadRows: %v", err)
	}
	if n != 3 {
		t.Fatalf("read %d rows, want 3", n)
	}
	for i, want := range []string{"1", "two", "3.5"} {
		if got := string(parsed[i][0].ByteArray()); got != want {
			t.Errorf("row %d = %q, want %q", i, got, want)
		}
	}
}

// TestEncodeStream_ParquetUnsignedStaysUnsigned: uint64 above MaxInt64 used to
// be written through a signed INT64, so a MySQL BIGINT UNSIGNED came back
// negative.
func TestEncodeStream_ParquetUnsignedStaysUnsigned(t *testing.T) {
	const big = uint64(1) << 63
	stream := newSliceStream([]string{"n"}, [][]any{{big}})

	var buf bytes.Buffer
	if _, _, err := EncodeStream(t.Context(), stream, "parquet", &buf); err != nil {
		t.Fatalf("EncodeStream: %v", err)
	}

	file, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	logical := file.Schema().Fields()[0].Type().LogicalType()
	if logical == nil {
		t.Fatal("column carries no logical type, so it is a plain signed INT64")
	}
	intType, ok := logical.Value.(*format.IntType)
	if !ok || intType.IsSigned {
		t.Fatalf("column is not typed as unsigned: %v", logical)
	}

	reader := parquet.NewGenericReader[any](bytes.NewReader(buf.Bytes()))
	defer func() {
		if err := reader.Close(); err != nil {
			t.Errorf("close reader: %v", err)
		}
	}()
	parsed := make([]parquet.Row, 1)
	if _, err := reader.ReadRows(parsed); err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("ReadRows: %v", err)
	}
	if got := uint64(parsed[0][0].Int64()); got != big {
		t.Errorf("value = %d, want %d", got, big)
	}
}

// TestEncodeStream_ParquetStreamsRowGroups: the writer defaults to one row group
// of math.MaxInt64 rows buffered in the heap, so nothing reached the output
// until Close and the whole result - a size the client picks with its own SQL -
// sat in memory.
func TestEncodeStream_ParquetStreamsRowGroups(t *testing.T) {
	const total = parquetRowsPerRowGroup + 10
	payload := strings.Repeat("x", 100)
	rows := make([][]any, 0, total)
	for i := range total {
		rows = append(rows, []any{strconv.Itoa(i) + payload})
	}

	var buf bytes.Buffer
	stream := newSliceStream([]string{"s"}, rows)
	// Sampled when the stream runs dry, which is the last moment before the
	// footer is written. Everything counted here got out during execution.
	stream.onExhausted = func() {
		if buf.Len() == 0 {
			t.Error("no bytes reached the writer before the file was finalized")
		}
	}

	got, _, err := EncodeStream(t.Context(), stream, "parquet", &buf)
	if err != nil {
		t.Fatalf("EncodeStream: %v", err)
	}
	if got != total {
		t.Fatalf("rows = %d, want %d", got, total)
	}

	file, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if len(file.RowGroups()) < 2 {
		t.Errorf("the file holds %d row groups, want the result split across several", len(file.RowGroups()))
	}
}
