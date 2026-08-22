//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// restRecord is the subset of the record DTO these tests read.
type restRecord struct {
	ID     string `json:"id"`
	State  string `json:"state"`
	Result *struct {
		Backend   string `json:"backend"`
		SizeBytes int64  `json:"size_bytes"`
		RowCount  int64  `json:"row_count"`
	} `json:"result"`
}

func restPost(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	return resp
}

func restDecode(t *testing.T, resp *http.Response) restRecord {
	t.Helper()
	defer resp.Body.Close()
	var rec restRecord
	if err := json.NewDecoder(resp.Body).Decode(&rec); err != nil {
		t.Fatalf("decode record: %v", err)
	}
	return rec
}

func restPollState(t *testing.T, baseURL, id, want string, deadline time.Duration) {
	t.Helper()
	timeout := time.After(deadline)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-timeout:
			t.Fatalf("query %s did not reach %s within %v", id, want, deadline)
		case <-ticker.C:
			resp, err := http.Get(baseURL + "/v1/queries/" + id)
			if err != nil {
				t.Fatalf("GET status: %v", err)
			}
			if restDecode(t, resp).State == want {
				return
			}
		}
	}
}

// TestRedis_RESTAcrossInstances: a client that reaches a different node than
// the one that owns the query must still see its status and be able to stop it.
// Cancellation travels over Redis Pub/Sub; a node that only cancelled locally
// would answer 200 and leave the query running on its owner.
func TestRedis_RESTAcrossInstances(t *testing.T) {
	redisAddr := startRedis(t)
	pgDSN := startPostgres(t)
	seed(t, "postgres", pgDSN,
		"CREATE TABLE IF NOT EXISTS people (id int, name text)",
		"INSERT INTO people VALUES (1, 'alice'), (2, 'bob')",
	)

	owner := newHarness(t, harnessOptions{instanceID: "node-a", redisAddr: redisAddr, databases: pgDatabases(pgDSN)})
	peer := newHarness(t, harnessOptions{instanceID: "node-b", redisAddr: redisAddr, databases: pgDatabases(pgDSN)})

	ownerURL := newRESTServer(t, owner)
	peerURL := newRESTServer(t, peer)

	// A query that finishes on its own: the peer has to see the terminal record
	// and serve its result, which lives on the shared filesystem store.
	done := restDecode(t, restPost(t, ownerURL+"/v1/queries",
		`{"database_id":"pg","sql":"SELECT id, name FROM people ORDER BY id","options":{"mode":"sync"}}`))
	if done.State != "SUCCEEDED" {
		t.Fatalf("state = %s, want SUCCEEDED", done.State)
	}

	viaPeer, err := http.Get(peerURL + "/v1/queries/" + done.ID)
	if err != nil {
		t.Fatalf("GET status via peer: %v", err)
	}
	if got := restDecode(t, viaPeer); got.State != "SUCCEEDED" {
		t.Fatalf("peer sees state %s, want SUCCEEDED", got.State)
	}

	dl, err := http.Get(peerURL + "/v1/queries/" + done.ID + "/result")
	if err != nil {
		t.Fatalf("GET result via peer: %v", err)
	}
	defer dl.Body.Close()
	if dl.StatusCode != http.StatusOK {
		t.Fatalf("peer download status = %d, want 200", dl.StatusCode)
	}
	body, _ := io.ReadAll(dl.Body)
	if !bytes.Contains(body, []byte("alice")) {
		t.Errorf("peer download is missing rows: %q", body)
	}

	// A long query submitted to the owner and stopped through the peer.
	running := restDecode(t, restPost(t, ownerURL+"/v1/queries",
		`{"database_id":"pg","sql":"SELECT pg_sleep(30)","options":{"mode":"async"}}`))
	restPollState(t, ownerURL, running.ID, "RUNNING", 15*time.Second)

	stop := restPost(t, peerURL+"/v1/queries/"+running.ID+":stop", "")
	stop.Body.Close()
	if stop.StatusCode != http.StatusOK {
		t.Fatalf("stop via peer = %d, want 200", stop.StatusCode)
	}

	restPollState(t, ownerURL, running.ID, "CANCELED", 20*time.Second)
}

// TestRedis_WebSocketOnANonOwner: events are published across instances, so a
// subscription opened on the peer has to see the owner's terminal transition.
func TestRedis_WebSocketOnANonOwner(t *testing.T) {
	redisAddr := startRedis(t)
	pgDSN := startPostgres(t)
	seed(t, "postgres", pgDSN, "CREATE TABLE IF NOT EXISTS people (id int, name text)")

	owner := newHarness(t, harnessOptions{instanceID: "node-a", redisAddr: redisAddr, databases: pgDatabases(pgDSN)})
	peer := newHarness(t, harnessOptions{instanceID: "node-b", redisAddr: redisAddr, databases: pgDatabases(pgDSN)})

	ownerURL := newRESTServer(t, owner)
	peerURL := newRESTServer(t, peer)

	running := restDecode(t, restPost(t, ownerURL+"/v1/queries",
		`{"database_id":"pg","sql":"SELECT pg_sleep(30)","options":{"mode":"async"}}`))
	restPollState(t, ownerURL, running.ID, "RUNNING", 15*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wsURL := strings.Replace(peerURL, "http://", "ws://", 1) + "/v1/ws?query_id=" + running.ID
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("ws dial on the peer: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	stop := restPost(t, ownerURL+"/v1/queries/"+running.ID+":stop", "")
	stop.Body.Close()

	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("ws read: %v", err)
		}
		var msg struct {
			State string `json:"state"`
		}
		if err := json.Unmarshal(data, &msg); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if msg.State == "CANCELED" {
			return
		}
		if msg.State == "SUCCEEDED" || msg.State == "FAILED" {
			t.Fatalf("terminal state on the peer = %s, want CANCELED", msg.State)
		}
	}
}

// TestS3_RangeDownloadOverREST: partial reads go through the S3 reader and the
// section wrapper, a combination the filesystem store never exercises.
//
// The "s3" storage backend is registered once, process-wide (storage.Register
// panics on a second registration under the same name), so this test shares
// it through ensureS3Store rather than starting its own MinIO container:
// a container scoped to this test alone would be torn down at the end of it,
// but TestS3_ResultRoundTrip (integration_test.go) may run first and claim
// the registration for a container of its own.
func TestS3_RangeDownloadOverREST(t *testing.T) {
	redisAddr := startRedis(t)
	pgDSN := startPostgres(t)
	seed(t, "postgres", pgDSN,
		"CREATE TABLE IF NOT EXISTS people (id int, name text)",
		"INSERT INTO people VALUES (1, 'alice'), (2, 'bob')",
	)
	ep := ensureS3Store(t)

	h := newHarness(t, harnessOptions{
		instanceID:     "node-s3",
		redisAddr:      redisAddr,
		databases:      pgDatabases(pgDSN),
		defaultStorage: "s3",
		storageSection: fmt.Sprintf("  s3:\n    bucket: %s\n    region: us-east-1\n    endpoint: %q\n    access_key_id: %q\n    secret_access_key: %q\n",
			ep.bucket, ep.endpoint, ep.keyID, ep.secret),
	})
	baseURL := newRESTServer(t, h)

	rec := restDecode(t, restPost(t, baseURL+"/v1/queries",
		`{"database_id":"pg","sql":"SELECT id, name FROM people ORDER BY id","options":{"mode":"sync"}}`))
	if rec.State != "SUCCEEDED" {
		t.Fatalf("state = %s, want SUCCEEDED", rec.State)
	}
	if rec.Result == nil || rec.Result.Backend != "s3" {
		t.Fatalf("result = %+v, want an s3 ref", rec.Result)
	}

	whole, err := http.Get(baseURL + "/v1/queries/" + rec.ID + "/result")
	if err != nil {
		t.Fatalf("GET result: %v", err)
	}
	full, _ := io.ReadAll(whole.Body)
	whole.Body.Close()

	req, err := http.NewRequest(http.MethodGet, baseURL+"/v1/queries/"+rec.ID+"/result", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Range", "bytes=0-4")
	partial, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("ranged GET: %v", err)
	}
	defer partial.Body.Close()
	if partial.StatusCode != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", partial.StatusCode)
	}
	if got, want := partial.Header.Get("Content-Range"), fmt.Sprintf("bytes 0-4/%d", rec.Result.SizeBytes); got != want {
		t.Errorf("Content-Range = %q, want %q", got, want)
	}
	head, _ := io.ReadAll(partial.Body)
	if !bytes.Equal(head, full[:5]) {
		t.Errorf("ranged body = %q, want %q", head, full[:5])
	}
}
