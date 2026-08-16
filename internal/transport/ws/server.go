package ws

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/ekalinin/dbbridge/internal/core/manager"
	"github.com/ekalinin/dbbridge/internal/core/service"

	"github.com/coder/websocket"
)

// maxSubscriptionsPerConn bounds the watches a single connection can open.
// Every watch used to spawn a goroutine and register a channel that lived until
// the connection closed, with no check on the query ID, so one connection in a
// loop could grow goroutines and memory without limit.
const maxSubscriptionsPerConn = 32

// Options configures the hub.
type Options struct {
	// AllowedOrigins lists the browser origins allowed to open a connection.
	// Empty means same-origin only, which is coder/websocket's default.
	AllowedOrigins []string
}

type Hub struct {
	svc  *service.QueryService
	opts Options
}

func NewHub(svc *service.QueryService, opts Options) *Hub {
	return &Hub{svc: svc, opts: opts}
}

type ClientMessage struct {
	Action  string `json:"action"` // "watch" or "unwatch"
	QueryID string `json:"query_id"`
}

type ServerMessage struct {
	QueryID string `json:"query_id"`
	State   string `json:"state,omitempty"`
	Stats   any    `json:"stats,omitempty"`
	Error   any    `json:"error,omitempty"`
}

// conn tracks the subscriptions of one WebSocket connection so a repeated watch
// is a no-op, an unwatch actually cancels, and everything is released when the
// connection goes away.
type conn struct {
	ws   *websocket.Conn
	mu   sync.Mutex
	subs map[string]context.CancelFunc
	// writeMu serializes writes: coder/websocket allows one writer at a time
	// and each subscription runs in its own goroutine.
	writeMu sync.Mutex
}

func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Origin checking was disabled outright. Any page loaded in a browser
	// inside the perimeter could then open a connection to dbbridge and read
	// other people's query events (cross-site WebSocket hijacking).
	opts := &websocket.AcceptOptions{
		OriginPatterns: h.opts.AllowedOrigins,
	}

	wsConn, err := websocket.Accept(w, r, opts)
	if err != nil {
		log.Printf("WS: Accept failed: %v", err)
		return
	}
	defer func() {
		if err := wsConn.CloseNow(); err != nil {
			log.Printf("WS: close failed: %v", err)
		}
	}()

	// The request context ends when the client goes away, which is exactly the
	// lifetime a subscription should have. It carries no deadline any more: the
	// router applies its request timeout only to the short-lived routes.
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	c := &conn{ws: wsConn, subs: make(map[string]context.CancelFunc)}
	var wg sync.WaitGroup
	// Declared in this order so the deferred calls run as closeAll -> wg.Wait:
	// the pumps have to be canceled before anything waits on them.
	defer wg.Wait()
	defer c.closeAll()

	// If query_id was provided in the query string, watch it immediately
	if queryID := r.URL.Query().Get("query_id"); queryID != "" {
		h.subscribe(ctx, &wg, c, queryID)
	}

	// Message loop for dynamic action-based subscriptions
	for {
		typ, data, err := wsConn.Read(ctx)
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

		switch msg.Action {
		case "watch":
			if msg.QueryID != "" {
				h.subscribe(ctx, &wg, c, msg.QueryID)
			}
		case "unwatch":
			c.unsubscribe(msg.QueryID)
		}
	}
}

// subscribe opens a watch unless the connection already has one for that query
// or has reached its subscription budget.
func (h *Hub) subscribe(ctx context.Context, wg *sync.WaitGroup, c *conn, queryID string) {
	c.mu.Lock()
	if _, ok := c.subs[queryID]; ok {
		c.mu.Unlock()
		return
	}
	if len(c.subs) >= maxSubscriptionsPerConn {
		c.mu.Unlock()
		c.writeError(ctx, queryID, "subscription limit reached")
		return
	}
	subCtx, subCancel := context.WithCancel(ctx)
	c.subs[queryID] = subCancel
	c.mu.Unlock()

	ch, err := h.svc.WatchQuery(subCtx, queryID)
	if err != nil {
		c.unsubscribe(queryID)
		c.writeError(ctx, queryID, "query not found")
		return
	}

	wg.Go(func() {
		defer c.unsubscribe(queryID)
		c.pump(subCtx, ch)
	})
}

func (c *conn) pump(ctx context.Context, ch <-chan manager.QueryEvent) {
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

			if !c.write(ctx, srvMsg) {
				return
			}
		}
	}
}

func (c *conn) write(ctx context.Context, msg ServerMessage) bool {
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("WS: Marshal failed: %v", err)
		return true
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.ws.Write(ctx, websocket.MessageText, data); err != nil {
		// Connection is likely closed
		return false
	}
	return true
}

func (c *conn) writeError(ctx context.Context, queryID, reason string) {
	c.write(ctx, ServerMessage{QueryID: queryID, Error: reason})
}

func (c *conn) unsubscribe(queryID string) {
	c.mu.Lock()
	cancel, ok := c.subs[queryID]
	delete(c.subs, queryID)
	c.mu.Unlock()
	if ok {
		cancel()
	}
}

func (c *conn) closeAll() {
	c.mu.Lock()
	cancels := make([]context.CancelFunc, 0, len(c.subs))
	for id, cancel := range c.subs {
		cancels = append(cancels, cancel)
		delete(c.subs, id)
	}
	c.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}
