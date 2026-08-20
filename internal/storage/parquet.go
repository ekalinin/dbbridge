package storage

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/ekalinin/dbbridge/internal/db"

	"github.com/parquet-go/parquet-go"
)

// parquetSchemaSample is how many rows are buffered to infer column types.
// Parquet needs its schema up front and a SQL RowStream only reports column
// names, so the types come from the data. The buffer is bounded, everything
// past it is streamed.
const parquetSchemaSample = 128

// parquetRowsPerRowGroup bounds how much of the result is held in memory.
// parquet-go defaults MaxRowsPerRowGroup to math.MaxInt64 and buffers pages in
// the heap, so without an explicit flush nothing reached the output writer
// until Close and the whole result sat in RSS - a client picked the size of
// that allocation with its own SQL. Flushing ends the current row group and
// writes it out, which is also what lets StorageWriteDuration overlap reading
// from the database (I4, spec §5.2).
const parquetRowsPerRowGroup = 50_000

// parquetColumn binds a result column to its position in the parquet schema and
// to the conversion that produces a value of the column's physical type.
type parquetColumn struct {
	index int
	conv  func(any) (parquet.Value, error)
}

// encodeParquet writes the stream as a real Parquet file.
//
// Two properties are worth knowing. Columns appear in alphabetical order,
// because parquet-go sorts the fields of a schema group by name; parquet
// readers address columns by name, so this is a layout detail rather than a
// data one. And duplicate column names, which SQL allows, are disambiguated
// with a numeric suffix, because a parquet schema cannot hold two fields of the
// same name.
func encodeParquet(ctx context.Context, stream db.RowStream, columns []string, cw *CountingWriter, report func(rows int64)) (int64, error) {
	names := uniqueColumnNames(columns)

	scanArgs := make([]any, len(columns))
	values := make([]any, len(columns))
	for i := range scanArgs {
		scanArgs[i] = &values[i]
	}

	// Sample the head of the stream to decide each column's type.
	var sample [][]any
	for len(sample) < parquetSchemaSample && stream.Next() {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if err := stream.Scan(scanArgs...); err != nil {
			return 0, fmt.Errorf("failed to scan row: %w", err)
		}
		row := make([]any, len(values))
		copy(row, values)
		sample = append(sample, row)
	}
	if err := stream.Err(); err != nil {
		return 0, fmt.Errorf("row stream error: %w", err)
	}

	schema, cols, err := buildParquetSchema(names, sample)
	if err != nil {
		return 0, err
	}
	writer := parquet.NewGenericWriter[any](cw, schema, parquet.MaxRowsPerRowGroup(parquetRowsPerRowGroup))

	var rowCount int64
	closed := false
	// parquet-go returns the page buffers to its pool in Close, so an early
	// return without it leaves a row group on the heap until the next GC.
	defer func() {
		if closed {
			return
		}
		// The error is the one that is already on its way out of the function;
		// this call is only here to return the buffers.
		if cerr := writer.Close(); cerr != nil {
			log.Printf("ERROR: failed to close the parquet writer after an error: %v", cerr)
		}
	}()

	// A parquet.Row is positional: each value has to sit at its own column
	// index, which is the alphabetical position rather than the SQL one.
	row := make(parquet.Row, len(cols))
	buf := []parquet.Row{row}

	writeRow := func(src []any) error {
		for i, v := range src {
			pv, err := cols[i].conv(v)
			if err != nil {
				return fmt.Errorf("column %q: %w", names[i], err)
			}
			row[cols[i].index] = pv
		}
		if _, err := writer.WriteRows(buf); err != nil {
			return fmt.Errorf("failed to write parquet row: %w", err)
		}
		rowCount++
		if rowCount%parquetRowsPerRowGroup == 0 {
			if err := writer.Flush(); err != nil {
				return fmt.Errorf("failed to flush parquet row group: %w", err)
			}
		}
		report(rowCount)
		return nil
	}

	for _, sampled := range sample {
		if err := writeRow(sampled); err != nil {
			return rowCount, err
		}
	}

	for stream.Next() {
		if err := ctx.Err(); err != nil {
			return rowCount, err
		}
		if err := stream.Scan(scanArgs...); err != nil {
			return rowCount, fmt.Errorf("failed to scan row: %w", err)
		}
		if err := writeRow(values); err != nil {
			return rowCount, err
		}
	}
	if err := stream.Err(); err != nil {
		return rowCount, fmt.Errorf("row stream error: %w", err)
	}

	// The footer is written here, so a failure means the file is unreadable.
	closed = true
	if err := writer.Close(); err != nil {
		return rowCount, fmt.Errorf("failed to finalize parquet file: %w", err)
	}

	return rowCount, nil
}

// uniqueColumnNames makes the column names usable as parquet field names.
//
// A generated name is checked for collisions too. It used to be handed out
// blind, so SELECT 1 AS a, 2 AS a, 3 AS a_1 produced two fields called a_1;
// a parquet.Group is a map, the schema came out one field short, two result
// columns shared a parquet column index, and writing the row panicked with an
// out-of-range index - taking the whole process, and every query on it, down.
func uniqueColumnNames(columns []string) []string {
	seen := make(map[string]struct{}, len(columns))
	names := make([]string, len(columns))
	for i, col := range columns {
		base := col
		if base == "" {
			base = "column_" + strconv.Itoa(i)
		}
		name := base
		for n := 1; ; n++ {
			if _, dup := seen[name]; !dup {
				break
			}
			name = base + "_" + strconv.Itoa(n)
		}
		seen[name] = struct{}{}
		names[i] = name
	}
	return names
}

// parquetKind is the physical shape chosen for a column.
type parquetKind int

const (
	kindString parquetKind = iota
	kindInt
	kindUint
	kindFloat
	kindBool
	kindTimestamp
)

// buildParquetSchema infers a column type per field and returns the schema plus
// the per-column writers, indexed the same way as the result columns.
func buildParquetSchema(names []string, sample [][]any) (*parquet.Schema, []parquetColumn, error) {
	kinds := make([]parquetKind, len(names))
	decided := make([]bool, len(names))

	for _, row := range sample {
		for i, v := range row {
			if v == nil {
				continue
			}
			k := kindOf(v)
			if !decided[i] {
				kinds[i], decided[i] = k, true
				continue
			}
			// A column whose values disagree falls back to text, which can
			// represent all of them.
			if kinds[i] != k {
				kinds[i] = kindString
			}
		}
	}

	group := parquet.Group{}
	for i, name := range names {
		// Every column is optional: SQL NULL has to round-trip.
		group[name] = parquet.Optional(nodeFor(kinds[i]))
	}
	schema := parquet.NewSchema("row", group)

	// parquet.Group is a map, so two columns that ended up with the same name
	// would collapse into one field and every row built against the schema
	// would address a column index that does not exist. uniqueColumnNames is
	// what prevents that; this reports a mismatch as a query error rather than
	// letting it reach parquet-go as a panic.
	if len(schema.Fields()) != len(names) {
		return nil, nil, fmt.Errorf("parquet schema has %d fields for %d result columns", len(schema.Fields()), len(names))
	}

	// parquet-go orders group fields alphabetically, so the parquet column
	// index of a result column has to be looked up by name.
	position := make(map[string]int, len(names))
	for idx, field := range schema.Fields() {
		position[field.Name()] = idx
	}

	cols := make([]parquetColumn, len(names))
	for i, name := range names {
		idx := position[name]
		cols[i] = parquetColumn{index: idx, conv: converterFor(kinds[i], idx)}
	}
	return schema, cols, nil
}

func nodeFor(k parquetKind) parquet.Node {
	switch k {
	case kindInt:
		return parquet.Int(64)
	case kindUint:
		// INT64 with the unsigned logical type. Plain Int(64) turned a MySQL
		// BIGINT UNSIGNED or a ClickHouse UInt64 above MaxInt64 into a negative
		// number in the file.
		return parquet.Uint(64)
	case kindFloat:
		return parquet.Leaf(parquet.DoubleType)
	case kindBool:
		return parquet.Leaf(parquet.BooleanType)
	case kindTimestamp:
		// Microseconds is enough for Postgres and MySQL; a ClickHouse
		// DateTime64(9) loses its nanoseconds here.
		return parquet.Timestamp(parquet.Microsecond)
	default:
		return parquet.String()
	}
}

func kindOf(v any) parquetKind {
	switch v.(type) {
	case bool:
		return kindBool
	case int, int8, int16, int32, int64:
		return kindInt
	case uint, uint8, uint16, uint32, uint64:
		return kindUint
	case float32, float64:
		return kindFloat
	case time.Time:
		return kindTimestamp
	default:
		return kindString
	}
}

// errUnconvertible reports a value that does not fit the type the sample chose
// for its column. Returning it fails the query: the alternative was writing a
// NULL, which produced a file that opens and looks valid with values silently
// missing - the worst outcome for a proxy that materializes a result exactly
// once (I4).
func errUnconvertible(v any, kind string) error {
	return fmt.Errorf("value of type %T does not fit the inferred %s column", v, kind)
}

func converterFor(k parquetKind, index int) func(any) (parquet.Value, error) {
	null := parquet.NullValue().Level(0, 0, index)
	switch k {
	case kindInt:
		return func(v any) (parquet.Value, error) {
			if v == nil {
				return null, nil
			}
			n, ok := toInt64(v)
			if !ok {
				return null, errUnconvertible(v, "integer")
			}
			return parquet.Int64Value(n).Level(0, 1, index), nil
		}
	case kindUint:
		return func(v any) (parquet.Value, error) {
			if v == nil {
				return null, nil
			}
			n, ok := toUint64(v)
			if !ok {
				return null, errUnconvertible(v, "unsigned integer")
			}
			// The bit pattern is what the unsigned logical type reads back.
			return parquet.Int64Value(int64(n)).Level(0, 1, index), nil
		}
	case kindFloat:
		return func(v any) (parquet.Value, error) {
			if v == nil {
				return null, nil
			}
			f, ok := toFloat64(v)
			if !ok {
				return null, errUnconvertible(v, "float")
			}
			return parquet.DoubleValue(f).Level(0, 1, index), nil
		}
	case kindBool:
		return func(v any) (parquet.Value, error) {
			if v == nil {
				return null, nil
			}
			b, ok := v.(bool)
			if !ok {
				return null, errUnconvertible(v, "boolean")
			}
			return parquet.BooleanValue(b).Level(0, 1, index), nil
		}
	case kindTimestamp:
		return func(v any) (parquet.Value, error) {
			if v == nil {
				return null, nil
			}
			t, ok := v.(time.Time)
			if !ok {
				return null, errUnconvertible(v, "timestamp")
			}
			return parquet.Int64Value(t.UnixMicro()).Level(0, 1, index), nil
		}
	default:
		// Text represents everything, so this branch cannot fail.
		return func(v any) (parquet.Value, error) {
			if v == nil {
				return null, nil
			}
			return parquet.ByteArrayValue([]byte(toString(v))).Level(0, 1, index), nil
		}
	}
}

func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int8:
		return int64(n), true
	case int16:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	default:
		return 0, false
	}
}

func toUint64(v any) (uint64, bool) {
	switch n := v.(type) {
	case uint:
		return uint64(n), true
	case uint8:
		return uint64(n), true
	case uint16:
		return uint64(n), true
	case uint32:
		return uint64(n), true
	case uint64:
		return n, true
	default:
		return 0, false
	}
}

func toFloat64(v any) (float64, bool) {
	switch f := v.(type) {
	case float32:
		return float64(f), true
	case float64:
		return f, true
	default:
		return 0, false
	}
}
