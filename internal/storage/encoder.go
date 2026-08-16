package storage

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/ekalinin/dbbridge/internal/db"
)

// CountingWriter wraps an io.Writer and tracks the number of bytes written.
type CountingWriter struct {
	W     io.Writer
	Count int64
}

func (cw *CountingWriter) Write(p []byte) (int, error) {
	n, err := cw.W.Write(p)
	cw.Count += int64(n)
	return n, err
}

// progressReportEvery is how many rows pass between progress callbacks, and
// progressReportAfter how much time. Rows alone were the trigger, so the case
// the live figure exists for - an aggregation that runs for an hour and returns
// twelve rows - never reported at all, and a 1500-row result reported once.
const (
	progressReportEvery = 1000
	progressReportAfter = time.Second
)

// EncodeStream reads rows from db.RowStream and formats them into the requested
// format, streaming to w.
//
// The optional progress callbacks are invoked with the counts so far every
// progressReportEvery rows, at most every progressReportAfter otherwise, and
// once more when the stream ends. Stats used to be written only once, at
// completion, so a long-running query reported nothing until it was already
// over.
func EncodeStream(ctx context.Context, stream db.RowStream, format string, w io.Writer, progress ...func(rows, bytes int64)) (int64, int64, error) {
	columns, err := stream.Columns()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get columns: %w", err)
	}

	cw := &CountingWriter{W: w}
	var rowCount int64

	last := time.Now()
	emit := func(rows int64) {
		last = time.Now()
		for _, fn := range progress {
			fn(rows, cw.Count)
		}
	}
	report := func(rows int64) {
		if rows%progressReportEvery != 0 && time.Since(last) < progressReportAfter {
			return
		}
		emit(rows)
	}
	// The tail of the stream is reported too, so the last figure a watcher sees
	// is the real one rather than the last multiple of progressReportEvery.
	defer func() { emit(rowCount) }()

	switch format {
	case "csv":
		csvWriter := csv.NewWriter(cw)
		// Write header
		if err := csvWriter.Write(columns); err != nil {
			return 0, 0, fmt.Errorf("failed to write csv header: %w", err)
		}

		scanArgs := make([]any, len(columns))
		values := make([]any, len(columns))
		for i := range scanArgs {
			scanArgs[i] = &values[i]
		}

		for stream.Next() {
			select {
			case <-ctx.Done():
				return rowCount, cw.Count, ctx.Err()
			default:
			}

			if err := stream.Scan(scanArgs...); err != nil {
				return rowCount, cw.Count, fmt.Errorf("failed to scan row: %w", err)
			}

			rowStrings := make([]string, len(columns))
			for i, val := range values {
				rowStrings[i] = toString(val)
			}

			if err := csvWriter.Write(rowStrings); err != nil {
				return rowCount, cw.Count, fmt.Errorf("failed to write csv row: %w", err)
			}
			rowCount++
			report(rowCount)
		}
		csvWriter.Flush()
		if err := csvWriter.Error(); err != nil {
			return rowCount, cw.Count, fmt.Errorf("csv flush failed: %w", err)
		}

	case "parquet":
		rows, err := encodeParquet(ctx, stream, columns, cw, report)
		rowCount = rows
		return rows, cw.Count, err

	case "jsonl":
		scanArgs := make([]any, len(columns))
		values := make([]any, len(columns))
		for i := range scanArgs {
			scanArgs[i] = &values[i]
		}

		for stream.Next() {
			select {
			case <-ctx.Done():
				return rowCount, cw.Count, ctx.Err()
			default:
			}

			if err := stream.Scan(scanArgs...); err != nil {
				return rowCount, cw.Count, fmt.Errorf("failed to scan row: %w", err)
			}

			rowMap := make(map[string]any, len(columns))
			for i, col := range columns {
				val := values[i]
				// Convert byte slices to strings/runes for proper JSON encoding
				if bytes, ok := val.([]byte); ok {
					rowMap[col] = string(bytes)
				} else {
					rowMap[col] = val
				}
			}

			data, err := json.Marshal(rowMap)
			if err != nil {
				return rowCount, cw.Count, fmt.Errorf("failed to marshal row to json: %w", err)
			}

			if _, err := cw.Write(append(data, '\n')); err != nil {
				return rowCount, cw.Count, fmt.Errorf("failed to write jsonl row: %w", err)
			}
			rowCount++
			report(rowCount)
		}

	default:
		return 0, 0, fmt.Errorf("unsupported format %q", format)
	}

	if err := stream.Err(); err != nil {
		return rowCount, cw.Count, fmt.Errorf("row stream error: %w", err)
	}

	return rowCount, cw.Count, nil
}

func toString(val any) string {
	if val == nil {
		return ""
	}
	switch v := val.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case int:
		return strconv.Itoa(v)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	case time.Time:
		return v.Format(time.RFC3339Nano)
	default:
		return fmt.Sprintf("%v", v)
	}
}
