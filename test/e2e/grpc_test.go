package e2e_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	v1 "github.com/ekalinin/dbbridge/internal/gen/dbbridge/v1"
	"github.com/ekalinin/dbbridge/internal/gen/dbbridge/v1/dbbridgev1connect"

	"connectrpc.com/connect"
)

// connectStartSync submits a synchronous query over the Connect transport and
// returns the finished record. It is also used by later tasks.
func connectStartSync(t *testing.T, c dbbridgev1connect.QueryServiceClient, dbID, sql, format string) *v1.QueryRecord {
	t.Helper()
	resp, err := c.StartQuery(context.Background(), connect.NewRequest(&v1.StartQueryRequest{
		DatabaseId: dbID,
		Sql:        sql,
		Options:    &v1.QueryOptions{Mode: "sync", ResultFormat: format},
	}))
	if err != nil {
		t.Fatalf("StartQuery: %v", err)
	}
	return resp.Msg.Record
}

// TestConnect_UsesUnencryptedHTTP2 pins the claim in this file's name: a
// client that only offers UnencryptedHTTP2 (no HTTP1 fallback) must actually
// complete a request against h.grpcURL over real cleartext HTTP/2. Without
// this, a client transport that enables both protocols would silently fall
// back to HTTP/1.1 - see the comment on (*testHarness).connectClient - and
// every other test in this file would keep passing while never touching h2c.
func TestConnect_UsesUnencryptedHTTP2(t *testing.T) {
	h := newHarness(t)

	tr := &http.Transport{}
	tr.Protocols = new(http.Protocols)
	tr.Protocols.SetUnencryptedHTTP2(true)
	client := &http.Client{Transport: tr}

	resp, err := client.Get(h.grpcURL)
	if err != nil {
		t.Fatalf("GET %s: %v", h.grpcURL, err)
	}
	defer resp.Body.Close()

	if resp.Proto != "HTTP/2.0" {
		t.Fatalf("negotiated protocol = %q, want HTTP/2.0", resp.Proto)
	}
}

// TestConnect_LifecycleOverH2C runs the whole submit -> status -> stats ->
// download path over cleartext HTTP/2, the protocol the process serves when TLS
// is off.
func TestConnect_LifecycleOverH2C(t *testing.T) {
	h := newHarness(t)
	c := h.connectClient(t)

	rec := connectStartSync(t, c, "testdb", "SELECT id, name FROM users", "jsonl")
	if rec.State != v1.QueryState_QUERY_STATE_SUCCEEDED {
		t.Fatalf("state = %v, want SUCCEEDED", rec.State)
	}
	if rec.Result == nil || rec.Result.RowCount != 2 {
		t.Fatalf("result = %+v, want 2 rows", rec.Result)
	}

	status, err := c.GetQueryStatus(context.Background(), connect.NewRequest(&v1.GetQueryStatusRequest{QueryId: rec.Id}))
	if err != nil {
		t.Fatalf("GetQueryStatus: %v", err)
	}
	if status.Msg.Record.Id != rec.Id {
		t.Errorf("status returned id %q, want %q", status.Msg.Record.Id, rec.Id)
	}

	stats, err := c.GetQueryStats(context.Background(), connect.NewRequest(&v1.GetQueryStatsRequest{QueryId: rec.Id}))
	if err != nil {
		t.Fatalf("GetQueryStats: %v", err)
	}
	if stats.Msg.Stats.RowsRead != 2 {
		t.Errorf("rows_read = %d, want 2", stats.Msg.Stats.RowsRead)
	}

	stream, err := c.DownloadResult(context.Background(), connect.NewRequest(&v1.DownloadResultRequest{QueryId: rec.Id}))
	if err != nil {
		t.Fatalf("DownloadResult: %v", err)
	}
	var buf bytes.Buffer
	for stream.Receive() {
		buf.Write(stream.Msg().Chunk)
	}
	if err := stream.Err(); err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("download stream: %v", err)
	}
	if !bytes.Contains(buf.Bytes(), []byte("alice")) || !bytes.Contains(buf.Bytes(), []byte("bob")) {
		t.Fatalf("downloaded body is missing rows: %q", buf.String())
	}
}

// TestConnect_WatchQueryStreamsToTerminal subscribes before the query finishes
// and asserts the server stream carries it to a terminal state.
func TestConnect_WatchQueryStreamsToTerminal(t *testing.T) {
	h := newHarness(t)
	c := h.connectClient(t)

	started, err := c.StartQuery(context.Background(), connect.NewRequest(&v1.StartQueryRequest{
		DatabaseId: "slowdb",
		Sql:        "SELECT 1",
		Options:    &v1.QueryOptions{Mode: "async"},
	}))
	if err != nil {
		t.Fatalf("StartQuery: %v", err)
	}
	id := started.Msg.Record.Id

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := c.WatchQuery(ctx, connect.NewRequest(&v1.WatchQueryRequest{QueryId: id}))
	if err != nil {
		t.Fatalf("WatchQuery: %v", err)
	}

	// Rather than sleeping a fixed duration before issuing the stop, poll
	// GetQueryStatus until the query reports RUNNING. This keeps the test from
	// depending on a machine being fast enough for the sleep to land after the
	// subscription is established but avoids a fixed wall-clock margin.
	go func() {
		deadline := time.After(5 * time.Second)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-deadline:
				return
			case <-ticker.C:
				status, err := c.GetQueryStatus(context.Background(), connect.NewRequest(&v1.GetQueryStatusRequest{QueryId: id}))
				if err == nil && status.Msg.Record.State == v1.QueryState_QUERY_STATE_RUNNING {
					_, _ = c.StopQuery(context.Background(), connect.NewRequest(&v1.StopQueryRequest{QueryId: id}))
					return
				}
			}
		}
	}()

	var last v1.QueryState
	for stream.Receive() {
		last = stream.Msg().State
		if last == v1.QueryState_QUERY_STATE_CANCELED ||
			last == v1.QueryState_QUERY_STATE_SUCCEEDED ||
			last == v1.QueryState_QUERY_STATE_FAILED {
			break
		}
	}
	if last != v1.QueryState_QUERY_STATE_CANCELED {
		t.Fatalf("last streamed state = %v, want CANCELED", last)
	}
}

// TestConnect_ErrorCodesAreTyped: every failure used to be CodeInternal with
// the wrapped driver error attached, DSN included.
func TestConnect_ErrorCodesAreTyped(t *testing.T) {
	h := newHarness(t)
	c := h.connectClient(t)

	_, err := c.GetQueryStatus(context.Background(), connect.NewRequest(&v1.GetQueryStatusRequest{QueryId: "does-not-exist"}))
	if got := connect.CodeOf(err); got != connect.CodeNotFound {
		t.Errorf("unknown query = %v, want not found", got)
	}

	_, err = c.StartQuery(context.Background(), connect.NewRequest(&v1.StartQueryRequest{
		DatabaseId: "no-such-db",
		Sql:        "SELECT 1",
	}))
	if got := connect.CodeOf(err); got != connect.CodeNotFound {
		t.Errorf("unknown database = %v, want not found", got)
	}
	if err != nil {
		for _, leak := range []string{"dsn", "password", "postgres://"} {
			if bytes.Contains(bytes.ToLower([]byte(err.Error())), []byte(leak)) {
				t.Errorf("error message leaks %q: %v", leak, err)
			}
		}
	}

	_, err = c.StartQuery(context.Background(), connect.NewRequest(&v1.StartQueryRequest{
		DatabaseId: "testdb",
		Sql:        "SELECT 1",
		Options:    &v1.QueryOptions{Mode: "nonsense"},
	}))
	if got := connect.CodeOf(err); got != connect.CodeInvalidArgument {
		t.Errorf("bad mode = %v, want invalid argument", got)
	}
}
