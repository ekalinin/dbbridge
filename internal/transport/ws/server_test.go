package ws_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"dbbridge/internal/core/domain"
	"dbbridge/internal/testutil"
	"dbbridge/internal/transport/ws"

	"github.com/coder/websocket"
)

func TestWS_WatchQueryViaQueryParam(t *testing.T) {
	svc, _ := testutil.NewService(t)
	hub := ws.NewHub(svc)
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
	hub := ws.NewHub(svc)
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
