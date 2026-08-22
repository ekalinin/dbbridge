package e2e_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/ekalinin/dbbridge/internal/core/domain"

	"github.com/coder/websocket"
)

// wsMessage mirrors ws.ServerMessage for tests.
type wsMessage struct {
	QueryID string `json:"query_id"`
	State   string `json:"state"`
	Error   any    `json:"error"`
}

// startSlowAsync submits a query that stays RUNNING until it is stopped.
func startSlowAsync(t *testing.T, h *testHarness) string {
	t.Helper()
	resp := postJSON(t, h.baseURL+"/v1/queries", startQueryPayload{
		DatabaseID: "slowdb",
		SQL:        "SELECT id, name FROM users",
		Options:    map[string]any{"mode": "async"},
	})
	defer resp.Body.Close()
	assertStatus(t, resp, http.StatusAccepted)

	var rec queryRecord
	decodeJSON(t, resp.Body, &rec)
	if rec.ID == "" {
		t.Fatal("empty query id")
	}
	return rec.ID
}

// stopWhenRunning polls GET /v1/queries/{id} until the query reports RUNNING
// and then requests a stop over REST. A fixed sleep before the stop would
// either race the WebSocket subscription (on a slow machine) or pad every run
// with an unnecessary wait (on a fast one); polling the actual precondition
// avoids both. It runs detached in its own goroutine and never touches *testing.T,
// so it cannot panic after the test that started it has returned.
func stopWhenRunning(h *testHarness, id string) {
	go func() {
		deadline := time.After(5 * time.Second)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-deadline:
				return
			case <-ticker.C:
				resp, err := http.Get(h.baseURL + "/v1/queries/" + id)
				if err != nil {
					continue
				}
				var rec queryRecord
				decErr := json.NewDecoder(resp.Body).Decode(&rec)
				resp.Body.Close()
				if decErr != nil || rec.State != "RUNNING" {
					continue
				}
				stop, err := http.Post(h.baseURL+"/v1/queries/"+id+":stop", "application/json", nil)
				if err == nil {
					stop.Body.Close()
				}
				return
			}
		}
	}()
}

// readUntilTerminal drains the socket until a terminal state arrives.
func readUntilTerminal(ctx context.Context, t *testing.T, conn *websocket.Conn) string {
	t.Helper()
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("ws read: %v", err)
		}
		var msg wsMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			t.Fatalf("unmarshal ws message %q: %v", data, err)
		}
		if domain.QueryState(msg.State).IsTerminal() {
			return msg.State
		}
	}
}

// TestWS_WatchViaQueryParam: the query_id query parameter opens a subscription
// as part of the handshake.
func TestWS_WatchViaQueryParam(t *testing.T) {
	h := newHarness(t)
	id := startSlowAsync(t, h)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, h.wsURL("?query_id="+id), nil)
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	stopWhenRunning(h, id)

	if got := readUntilTerminal(ctx, t, conn); got != "CANCELED" {
		t.Errorf("terminal state = %q, want CANCELED", got)
	}
}

// TestWS_WatchAndUnwatchViaActions covers the dynamic action protocol.
func TestWS_WatchAndUnwatchViaActions(t *testing.T) {
	h := newHarness(t)
	id := startSlowAsync(t, h)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, h.wsURL(""), nil)
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	send := func(action, queryID string) {
		t.Helper()
		payload, err := json.Marshal(map[string]string{"action": action, "query_id": queryID})
		if err != nil {
			t.Fatalf("marshal action: %v", err)
		}
		if err := conn.Write(ctx, websocket.MessageText, payload); err != nil {
			t.Fatalf("ws write: %v", err)
		}
	}

	send("watch", id)
	stopWhenRunning(h, id)

	if got := readUntilTerminal(ctx, t, conn); got != "CANCELED" {
		t.Errorf("terminal state = %q, want CANCELED", got)
	}

	// Unwatching a subscription that has already ended must not break the
	// connection: the socket has to stay usable for the next watch.
	send("unwatch", id)

	second := startSlowAsync(t, h)
	send("watch", second)
	stopWhenRunning(h, second)
	if got := readUntilTerminal(ctx, t, conn); got != "CANCELED" {
		t.Errorf("second terminal state = %q, want CANCELED", got)
	}
}

// TestWS_UnknownQueryIsReported: a watch on an unknown ID used to block for
// ever instead of answering.
func TestWS_UnknownQueryIsReported(t *testing.T) {
	h := newHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, h.wsURL("?query_id=does-not-exist"), nil)
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("ws read: %v", err)
	}
	var msg wsMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("unmarshal ws message: %v", err)
	}
	if msg.Error == nil {
		t.Fatalf("unknown query produced %+v, want an error message", msg)
	}
}

// TestWS_ForeignOriginIsRejected: origin checking was off, so any page inside
// the perimeter could open a connection and read other people's query events.
func TestWS_ForeignOriginIsRejected(t *testing.T) {
	h := newHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _, err := websocket.Dial(ctx, h.wsURL(""), &websocket.DialOptions{
		HTTPHeader: http.Header{"Origin": []string{"http://evil.example"}},
	})
	if err == nil {
		t.Fatal("handshake from a foreign origin succeeded")
	}
}
