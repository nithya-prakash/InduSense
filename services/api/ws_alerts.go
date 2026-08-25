package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/nithya-prakash/indusense/pkg/auth"
	"github.com/nithya-prakash/indusense/pkg/events"
	kafka "github.com/segmentio/kafka-go"
)

var upgrader = websocket.Upgrader{
	// Local-development CORS: in production this would check Origin
	// against the configured frontend origin instead of allowing any.
	CheckOrigin: func(r *http.Request) bool { return true },
}

// wsHub fans out alert events to every connected client whose JWT
// organization matches the event's — the same tenant-isolation rule the
// REST handlers enforce, applied to a push channel instead of a pull one.
type wsHub struct {
	mu      sync.Mutex
	clients map[*websocket.Conn]string // conn -> organization_id
}

func newWSHub() *wsHub {
	return &wsHub{clients: make(map[*websocket.Conn]string)}
}

func (h *wsHub) register(conn *websocket.Conn, orgID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[conn] = orgID
	metricWebsocketConnections.Set(float64(len(h.clients)))
}

func (h *wsHub) unregister(conn *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, conn)
	metricWebsocketConnections.Set(float64(len(h.clients)))
	_ = conn.Close()
}

func (h *wsHub) broadcast(orgID string, payload []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for conn, connOrgID := range h.clients {
		if connOrgID != orgID {
			continue // tenant isolation: never send org A's alerts to org B's connection
		}
		if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
			log.Printf("api: websocket write failed, dropping client: %v", err)
			go h.unregister(conn)
		}
	}
}

// handleWSAlerts upgrades to a WebSocket and streams every alert event for
// the caller's organization in real time. Browsers can't set a custom
// Authorization header on the WebSocket handshake, so the access token is
// accepted as a query parameter here specifically — documented as a
// simplification; a production system would issue a short-lived,
// single-use ws ticket instead of reusing the bearer token in a URL.
func handleWSAlerts(hub *wsHub, accessSecret string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tokenString := r.URL.Query().Get("token")
		claims, err := auth.ParseAndValidate(tokenString, accessSecret, auth.TokenTypeAccess)
		if err != nil {
			http.Error(w, "unauthenticated", http.StatusUnauthorized)
			return
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("api: websocket upgrade failed: %v", err)
			return
		}
		hub.register(conn, claims.OrganizationID)
		log.Printf("api: websocket client connected (org=%s)", claims.OrganizationID)

		// Drain and discard any client-sent frames (this is a push-only
		// feed) purely to detect disconnects promptly via the read error.
		go func() {
			defer hub.unregister(conn)
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
			}
		}()
	}
}

// runAlertsFanOut consumes the `alerts` Kafka topic and broadcasts every
// event to matching WebSocket clients. Each API instance uses its own
// unique, never-reused consumer group so every instance receives a full
// copy of the topic — correct for fan-out-to-many-viewers, unlike a shared
// consumer group (which would split messages across instances, the right
// behavior for work queues but the wrong one for broadcast).
func runAlertsFanOut(ctx context.Context, brokers []string, topic string, hub *wsHub) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: "indusense-api-ws-" + uuid.NewString(),
	})
	defer reader.Close()

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("api: alerts fan-out fetch error: %v", err)
			continue
		}
		_ = reader.CommitMessages(ctx, msg) // best-effort: a missed broadcast on restart is acceptable for a live feed

		var evt events.AlertEvent
		if err := json.Unmarshal(msg.Value, &evt); err != nil {
			log.Printf("api: alerts fan-out: malformed message: %v", err)
			continue
		}
		hub.broadcast(evt.OrganizationID, msg.Value)
	}
}
