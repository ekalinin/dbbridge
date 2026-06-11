package storage

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"

	"dbbridge/internal/db"
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

// EncodeStream reads rows from db.RowStream and formats them into JSONL or CSV, streaming to w.
func EncodeStream(ctx context.Context, stream db.RowStream, format string, w io.Writer) (int64, int64, error) {
	columns, err := stream.Columns()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get columns: %w", err)
	}

	cw := &CountingWriter{W: w}
	var rowCount int64

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
		}
		csvWriter.Flush()
		if err := csvWriter.Error(); err != nil {
			return rowCount, cw.Count, fmt.Errorf("csv flush failed: %w", err)
		}

	case "jsonl", "parquet": // Fallback parquet to jsonl in v1 if not using full parquet engine
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
