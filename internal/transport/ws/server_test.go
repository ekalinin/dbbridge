package ws_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ekalinin/dbbridge/internal/core/domain"
	"github.com/ekalinin/dbbridge/internal/testutil"
	"github.com/ekalinin/dbbridge/internal/transport/ws"

	"github.com/coder/websocket"
)

func TestWS_WatchQueryViaQueryParam(t *testing.T) {
	svc, _ := testutil.NewService(t)
	hub := ws.NewHub(svc, ws.Options{})
	srv := httptest.NewServer(http.HandlerFunc(hub.ServeHTTP))
	t.Cleanup(srv.Close)

	// Long-running query that stays RUNNING until stopped.
	rec, err := svc.StartQuery(context.Background(), "slowdb", "SELECT *", domain.QueryOptions{Mode: "async"})
	if err != nil {
		t.Fatalf("StartQuery: %v", err)
	}

	wsURL := strings.Replace(srv.URL, "http://", "ws://", 1) + "?query_id=" + rec.ID

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	// Stop the query so a terminal event is published to the subscription.
	go func() {
		time.Sleep(300 * time.Millisecond)
		_ = svc.StopQuery(context.Background(), rec.ID)
	}()

	sawTerminal := false
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			break
		}
		var msg struct {
			QueryID string `json:"query_id"`
			State   string `json:"state"`
		}
		if err := json.Unmarshal(data, &msg); err != nil {
			t.Fatalf("unmarshal ws message: %v", err)
		}
		st := domain.QueryState(msg.State)
		if st.IsTerminal() {
			sawTerminal = true
			break
		}
	}
	if !sawTerminal {
		t.Error("did not receive a terminal state over WebSocket")
	}
}

func TestWS_WatchViaActionMessage(t *testing.T) {
	svc, _ := testutil.NewService(t)
	hub := ws.NewHub(svc, ws.Options{})
	srv := httptest.NewServer(http.HandlerFunc(hub.ServeHTTP))
	t.Cleanup(srv.Close)

	rec, err := svc.StartQuery(context.Background(), "slowdb", "SELECT *", domain.QueryOptions{Mode: "async"})
	if err != nil {
		t.Fatalf("StartQuery: %v", err)
	}

	wsURL := strings.Replace(srv.URL, "http://", "ws://", 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	sub, _ := json.Marshal(map[string]string{"action": "watch", "query_id": rec.ID})
	if err := conn.Write(ctx, websocket.MessageText, sub); err != nil {
		t.Fatalf("ws write: %v", err)
	}

	go func() {
		time.Sleep(300 * time.Millisecond)
		_ = svc.StopQuery(context.Background(), rec.ID)
	}()

	sawTerminal := false
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			break
		}
		var msg struct {
			State string `json:"state"`
		}
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}
		if domain.QueryState(msg.State).IsTerminal() {
			sawTerminal = true
			break
		}
	}
	if !sawTerminal {
		t.Error("did not receive a terminal state via action-based subscription")
	}
}

// TestWS_RejectsForeignOrigin: origin checking was disabled outright, so any
// page in a browser inside the perimeter could read other people's query
// events (cross-site WebSocket hijacking).
func TestWS_RejectsForeignOrigin(t *testing.T) {
	svc, _ := testutil.NewService(t)
	hub := ws.NewHub(svc, ws.Options{})
	srv := httptest.NewServer(http.HandlerFunc(hub.ServeHTTP))
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	wsURL := strings.Replace(srv.URL, "http://", "ws://", 1)
	_, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		HTTPHeader: http.Header{"Origin": []string{"http://evil.example"}},
	})
	if err == nil {
		t.Fatal("dial from a foreign origin succeeded")
	}
}

func TestWS_AllowsConfiguredOrigin(t *testing.T) {
	svc, _ := testutil.NewService(t)
	hub := ws.NewHub(svc, ws.Options{AllowedOrigins: []string{"friend.example"}})
	srv := httptest.NewServer(http.HandlerFunc(hub.ServeHTTP))
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	wsURL := strings.Replace(srv.URL, "http://", "ws://", 1)
	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		HTTPHeader: http.Header{"Origin": []string{"http://friend.example"}},
	})
	if err != nil {
		t.Fatalf("dial from an allowed origin: %v", err)
	}
	conn.Close(websocket.StatusNormalClosure, "")
}

// TestWS_BoundsSubscriptions: every watch message used to start a goroutine and
// register a channel that lived until the connection closed, with no cap and no
// check on the query ID.
func TestWS_BoundsSubscriptions(t *testing.T) {
	svc, _ := testutil.NewService(t)
	hub := ws.NewHub(svc, ws.Options{})
	srv := httptest.NewServer(http.HandlerFunc(hub.ServeHTTP))
	t.Cleanup(srv.Close)

	// Long-running queries, so every subscription stays open.
	const queries = 40
	ids := make([]string, 0, queries)
	for range queries {
		rec, err := svc.StartQuery(context.Background(), "slowdb", "SELECT 1", domain.QueryOptions{Mode: "async"})
		if err != nil {
			t.Fatalf("StartQuery: %v", err)
		}
		ids = append(ids, rec.ID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, strings.Replace(srv.URL, "http://", "ws://", 1), nil)
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	for _, id := range ids {
		msg, _ := json.Marshal(map[string]string{"action": "watch", "query_id": id})
		if err := conn.Write(ctx, websocket.MessageText, msg); err != nil {
			t.Fatalf("ws write: %v", err)
		}
	}

	sawLimit := false
	for range queries {
		_, data, err := conn.Read(ctx)
		if err != nil {
			break
		}
		var msg struct {
			Error any `json:"error"`
		}
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}
		if s, ok := msg.Error.(string); ok && strings.Contains(s, "subscription limit") {
			sawLimit = true
			break
		}
	}
	if !sawLimit {
		t.Error("the connection accepted more subscriptions than its budget")
	}
}

// TestWS_UnknownQueryIsReported: a watch on an unknown ID used to hang for ever.
func TestWS_UnknownQueryIsReported(t *testing.T) {
	svc, _ := testutil.NewService(t)
	hub := ws.NewHub(svc, ws.Options{})
	srv := httptest.NewServer(http.HandlerFunc(hub.ServeHTTP))
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, strings.Replace(srv.URL, "http://", "ws://", 1), nil)
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	msg, _ := json.Marshal(map[string]string{"action": "watch", "query_id": "no-such-query"})
	if err := conn.Write(ctx, websocket.MessageText, msg); err != nil {
		t.Fatalf("ws write: %v", err)
	}

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("ws read: %v", err)
	}
	var got struct {
		Error any `json:"error"`
	}
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Error == nil {
		t.Errorf("watching an unknown query reported no error: %s", data)
	}
}
