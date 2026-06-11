package ws

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"dbbridge/internal/core/service"

	"github.com/coder/websocket"
)

type Hub struct {
	svc *service.QueryService
}

func NewHub(svc *service.QueryService) *Hub {
	return &Hub{svc: svc}
}

type ClientMessage struct {
	Action  string `json:"action"` // "watch" or "unwatch"
	QueryID string `json:"query_id"`
}

type ServerMessage struct {
	QueryID string `json:"query_id"`
	State   string `json:"state"`
	Stats   any    `json:"stats,omitempty"`
	Error   any    `json:"error,omitempty"`
}

func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	opts := &websocket.AcceptOptions{
		InsecureSkipVerify: true, // Allow cross-origin for proxy
	}

	conn, err := websocket.Accept(w, r, opts)
	if err != nil {
		log.Printf("WS: Accept failed: %v", err)
		return
	}
	defer conn.CloseNow()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	// If query_id was provided in the query string, watch it immediately
	queryID := r.URL.Query().Get("query_id")
	if queryID != "" {
		go h.watchQuery(ctx, conn, queryID)
	}

	// Message loop for dynamic action-based subscriptions
	for {
		typ, data, err := conn.Read(ctx)
		if err != nil {
			// Normal closure or client disconnected
			break
		}

		if typ != websocket.MessageText {
			continue
		}

		var msg ClientMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			log.Printf("WS: Invalid message received: %v", err)
			continue
		}

		if msg.Action == "watch" && msg.QueryID != "" {
			go h.watchQuery(ctx, conn, msg.QueryID)
		}
	}
}

func (h *Hub) watchQuery(ctx context.Context, conn *websocket.Conn, queryID string) {
	ch, err := h.svc.WatchQuery(ctx, queryID)
	if err != nil {
		log.Printf("WS: Watch failed for %s: %v", queryID, err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return
			}

			srvMsg := ServerMessage{
				QueryID: ev.QueryID,
				State:   string(ev.State),
				Stats:   ev.Stats,
			}
			if ev.Error != nil {
				srvMsg.Error = ev.Error
			}

			data, err := json.Marshal(srvMsg)
			if err != nil {
				log.Printf("WS: Marshal failed: %v", err)
				continue
			}

			err = conn.Write(ctx, websocket.MessageText, data)
			if err != nil {
				// Connection is likely closed
				return
			}
		}
	}
}
