package clickhouse

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io"
	"sync"

	"github.com/ekalinin/dbbridge/internal/core/domain"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

type ClickHouseResultStore struct {
	db    *sql.DB
	table string
}

func NewClickHouseResultStore(dsn, table string) (*ClickHouseResultStore, error) {
	if table == "" {
		table = "dbbridge_results"
	}
	dbConn, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open clickhouse connection for storage: %w", err)
	}

	// Create results table
	ctx := context.Background()
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			query_id String,
			chunk_id UInt32,
			format String,
			data String
		) ENGINE = MergeTree()
		ORDER BY (query_id, chunk_id)
	`, table)

	if _, err := dbConn.ExecContext(ctx, query); err != nil {
		_ = dbConn.Close()
		return nil, fmt.Errorf("failed to create clickhouse results table: %w", err)
	}

	return &ClickHouseResultStore{
		db:    dbConn,
		table: table,
	}, nil
}

type clickhousePipeWriter struct {
	pw  *io.PipeWriter
	wg  sync.WaitGroup
	err error
}

func (w *clickhousePipeWriter) Write(p []byte) (int, error) {
	return w.pw.Write(p)
}

func (w *clickhousePipeWriter) Close() error {
	err := w.pw.Close()
	w.wg.Wait()
	if w.err != nil {
		return w.err
	}
	return err
}

func (s *ClickHouseResultStore) Writer(ctx context.Context, queryID string, format string) (io.WriteCloser, domain.ResultRef, error) {
	pr, pw := io.Pipe()
	pwWrapper := &clickhousePipeWriter{pw: pw}
	// Async insert into ClickHouse
	pwWrapper.wg.Go(func() {
		scanner := bufio.NewScanner(pr)
		// Custom larger buffer for potentially long lines (JSONL rows can be big)
		buf := make([]byte, 0, 64*1024)
		scanner.Buffer(buf, 10*1024*1024) // up to 10MB line

		var chunkID uint32
		tx, err := s.db.Begin()
		if err != nil {
			pwWrapper.err = err
			_ = pr.CloseWithError(err)
			return
		}

		insertQuery := fmt.Sprintf("INSERT INTO %s (query_id, chunk_id, format, data) VALUES (?, ?, ?, ?)", s.table)
		stmt, err := tx.Prepare(insertQuery)
		if err != nil {
			_ = tx.Rollback()
			pwWrapper.err = err
			_ = pr.CloseWithError(err)
			return
		}
		defer stmt.Close()

		for scanner.Scan() {
			line := scanner.Text()
			if _, err := stmt.Exec(queryID, chunkID, format, line); err != nil {
				_ = tx.Rollback()
				pwWrapper.err = err
				_ = pr.CloseWithError(err)
				return
			}
			chunkID++
			// Commit every 500 records to prevent long transactions
			if chunkID%500 == 0 {
				if err := tx.Commit(); err != nil {
					pwWrapper.err = err
					_ = pr.CloseWithError(err)
					return
				}
				tx, err = s.db.Begin()
				if err != nil {
					pwWrapper.err = err
					_ = pr.CloseWithError(err)
					return
				}
				stmt, err = tx.Prepare(insertQuery)
				if err != nil {
					_ = tx.Rollback()
					pwWrapper.err = err
					_ = pr.CloseWithError(err)
					return
				}
			}
		}

		if err := scanner.Err(); err != nil {
			_ = tx.Rollback()
			pwWrapper.err = err
			_ = pr.CloseWithError(err)
			return
		}

		if err := tx.Commit(); err != nil {
			pwWrapper.err = err
			_ = pr.CloseWithError(err)
			return
		}
		_ = pr.Close()
	})

	ref := domain.ResultRef{
		Backend: "clickhouse",
		Locator: queryID,
		Format:  format,
	}

	return pwWrapper, ref, nil
}

type clickhouseReader struct {
	rows        *sql.Rows
	currentLine []byte
	lineReader  *io.PipeReader
	err         error
	mu          sync.Mutex
	isClosed    bool
	closeCh     chan struct{}
}

func (r *clickhouseReader) Read(p []byte) (n int, err error) {
	return r.lineReader.Read(p)
}

func (r *clickhouseReader) Close() error {
	r.mu.Lock()
	if r.isClosed {
		r.mu.Unlock()
		return nil
	}
	r.isClosed = true
	r.mu.Unlock()

	close(r.closeCh)
	_ = r.lineReader.Close()
	_ = r.rows.Close()
	return nil
}

func (s *ClickHouseResultStore) Reader(ctx context.Context, ref domain.ResultRef) (io.ReadCloser, error) {
	query := fmt.Sprintf("SELECT data FROM %s WHERE query_id = ? ORDER BY chunk_id", s.table)
	rows, err := s.db.QueryContext(ctx, query, ref.Locator)
	if err != nil {
		return nil, fmt.Errorf("clickhouse select failed: %w", err)
	}

	pr, pw := io.Pipe()
	closeCh := make(chan struct{})

	cr := &clickhouseReader{
		rows:       rows,
		lineReader: pr,
		closeCh:    closeCh,
	}

	go func() {
		defer pw.Close()
		defer rows.Close()

		for rows.Next() {
			select {
			case <-closeCh:
				return
			default:
			}

			var line string
			if err := rows.Scan(&line); err != nil {
				_ = pw.CloseWithError(err)
				return
			}

			// Add newline back as we read JSONL/CSV lines
			if _, err := pw.Write([]byte(line + "\n")); err != nil {
				return
			}
		}

		if err := rows.Err(); err != nil {
			_ = pw.CloseWithError(err)
		}
	}()

	return cr, nil
}

func (s *ClickHouseResultStore) Stat(ctx context.Context, ref domain.ResultRef) (domain.ResultRef, error) {
	query := fmt.Sprintf("SELECT count(), sum(length(data)) FROM %s WHERE query_id = ?", s.table)
	var rowCount int64
	var byteSize sql.NullInt64
	if err := s.db.QueryRowContext(ctx, query, ref.Locator).Scan(&rowCount, &byteSize); err != nil {
		return domain.ResultRef{}, fmt.Errorf("clickhouse stat failed: %w", err)
	}
	ref.RowCount = rowCount
	if byteSize.Valid {
		ref.SizeBytes = byteSize.Int64
	}
	return ref, nil
}

func (s *ClickHouseResultStore) Delete(ctx context.Context, ref domain.ResultRef) error {
	query := fmt.Sprintf("ALTER TABLE %s DELETE WHERE query_id = ?", s.table)
	_, err := s.db.ExecContext(ctx, query, ref.Locator)
	if err != nil {
		return fmt.Errorf("clickhouse delete failed: %w", err)
	}
	return nil
}

func (s *ClickHouseResultStore) Close() error {
	return s.db.Close()
}
