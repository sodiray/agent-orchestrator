package httpd

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/aoagents/agent-orchestrator/backend/internal/cdc"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/apispec"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/envelope"
	federationsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/federation"
)

const (
	eventsReplayBatch = 512
	eventsLiveBuffer  = 1024

	remoteEventsReconnectMin = 100 * time.Millisecond
	remoteEventsReconnectMax = 5 * time.Second
)

type cdcSubscriber interface {
	Subscribe(func(cdc.Event)) (unsubscribe func())
}

// EventsController owns the client-facing CDC stream. Durable replay comes from
// change_log through Source; Broadcaster remains a live-only pub/sub seam.
type EventsController struct {
	Source     cdc.Source
	Live       cdcSubscriber
	Federation *federationsvc.Service
	Client     *http.Client
	Log        *slog.Logger
}

// Register mounts the CDC SSE stream route.
func (c *EventsController) Register(r chi.Router) {
	r.Get("/events", c.stream)
}

func (c *EventsController) stream(w http.ResponseWriter, r *http.Request) {
	if c.Source == nil || c.Live == nil {
		apispec.NotImplemented(w, r, "GET", "/api/v1/events")
		return
	}

	after, err := parseEventsAfter(r)
	if err != nil {
		envelope.WriteAPIError(w, r, http.StatusBadRequest, "bad_request", "INVALID_AFTER",
			"after must be a non-negative integer", nil)
		return
	}
	if r.URL.Query().Get("tail") == "1" {
		// Capture before installing the live subscription. replay runs after the
		// subscription is installed, so changes in this small gap are replayed
		// rather than lost while durable history before the capture is skipped.
		after, err = c.Source.LatestSeq(r.Context())
		if err != nil {
			c.logger().Error("capture event stream tail failed", "err", err)
			envelope.WriteAPIError(w, r, http.StatusInternalServerError, "internal", "EVENT_TAIL_UNAVAILABLE",
				"The event stream tail is unavailable", nil)
			return
		}
		c.logger().Info("event stream starts at captured tail", "after", after, "reason", "tail requested")
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		envelope.WriteAPIError(w, r, http.StatusInternalServerError, "internal", "SSE_UNSUPPORTED",
			"Streaming is not supported by this server", nil)
		return
	}

	ctx, cancel := context.WithCancel(r.Context())

	live := make(chan cdc.Event, eventsLiveBuffer)
	unsubscribe := c.Live.Subscribe(func(e cdc.Event) {
		select {
		case live <- e:
		default:
			// Never block the broadcaster. Closing the stream is safer than
			// silently dropping a live event; the client replays on reconnect.
			cancel()
		}
	})
	defer unsubscribe()

	var remote <-chan cdc.Event
	waitRemote := func() {}
	if r.Header.Get("X-AO-Federation-Local") != "1" {
		remote, waitRemote = c.remoteEvents(ctx)
	}
	defer func() {
		cancel()
		waitRemote()
	}()

	h := w.Header()
	h.Set("Content-Type", "text/event-stream; charset=utf-8")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	sentSeq := after
	if err := c.replay(ctx, w, flusher, &sentSeq); err != nil {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case e := <-live:
			if err := writeSSEEvent(w, flusher, e, &sentSeq); err != nil {
				return
			}
		case e := <-remote:
			if err := writeRemoteSSEEvent(w, flusher, e); err != nil {
				return
			}
		}
	}
}

// remoteEvents starts one owner-side SSE subscription per registered host for
// this local client stream. Remote event sequence numbers stay meaningful only
// to their owner, so forwarded frames deliberately have no SSE id: they cannot
// corrupt the local daemon's durable replay cursor.
func (c *EventsController) remoteEvents(ctx context.Context) (<-chan cdc.Event, func()) {
	out := make(chan cdc.Event, eventsLiveBuffer)
	if c.Federation == nil || !c.Federation.HasRegisteredHosts() {
		return out, func() {}
	}
	hosts, err := c.Federation.RemoteHosts(ctx)
	if err != nil {
		c.logger().Error("list remote hosts for event stream failed", "err", err)
		return out, func() {}
	}
	var group sync.WaitGroup
	for _, host := range hosts {
		if host.OperatorState == domain.RemoteHostStateStopped || host.OperatorState == domain.RemoteHostStateDestroyed {
			c.logger().Warn("remote event stream unavailable", "hostId", host.HostID, "reason", "host is stopped")
			continue
		}
		group.Add(1)
		go func(host domain.RemoteHost) {
			defer group.Done()
			c.forwardRemoteEvents(ctx, host, out)
		}(host)
	}
	return out, func() { group.Wait() }
}

func (c *EventsController) forwardRemoteEvents(ctx context.Context, host domain.RemoteHost, out chan<- cdc.Event) {
	after := int64(0)
	tail := true
	for attempt := 0; ; attempt++ {
		if tail {
			c.logger().Info("remote event stream starts at captured tail", "hostId", host.HostID, "address", host.Address, "reason", "no observed remote cursor")
		}
		err := c.readRemoteEvents(ctx, host, after, tail, func(e cdc.Event) error {
			after = e.Seq
			tail = false
			qualified, err := qualifyRemoteEvent(host.HostID, e)
			if err != nil {
				return err
			}
			select {
			case out <- qualified:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			c.logger().Warn("remote event stream dropped", "hostId", host.HostID, "address", host.Address, "err", err)
		} else {
			c.logger().Warn("remote event stream ended", "hostId", host.HostID, "address", host.Address)
		}
		if !waitForRemoteEventsRetry(ctx, attempt) {
			return
		}
	}
}

func (c *EventsController) readRemoteEvents(ctx context.Context, host domain.RemoteHost, after int64, tail bool, handle func(cdc.Event) error) error {
	endpoint := url.URL{Scheme: "http", Host: host.Address, Path: "/api/v1/events"}
	query := endpoint.Query()
	query.Set("after", strconv.FormatInt(after, 10))
	if tail {
		query.Set("tail", "1")
	}
	endpoint.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil) // #nosec G704 -- target is a registered remote-host endpoint.
	if err != nil {
		return fmt.Errorf("build remote event request: %w", err)
	}
	req.Header.Set("X-AO-Federation-Local", "1")
	resp, err := c.client().Do(req) // #nosec G704 -- target is a registered remote-host endpoint.
	if err != nil {
		return fmt.Errorf("request remote event stream: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("remote event stream returned HTTP %d", resp.StatusCode)
	}
	return readSSE(resp.Body, func(data []byte) error {
		var event cdc.Event
		if err := json.Unmarshal(data, &event); err != nil {
			return fmt.Errorf("decode remote event: %w", err)
		}
		if event.Seq <= after {
			return nil
		}
		return handle(event)
	})
}

func (c *EventsController) client() *http.Client {
	if c.Client != nil {
		return c.Client
	}
	return http.DefaultClient
}

func (c *EventsController) logger() *slog.Logger {
	if c.Log != nil {
		return c.Log
	}
	return slog.Default()
}

func waitForRemoteEventsRetry(ctx context.Context, attempt int) bool {
	delay := remoteEventsReconnectMin << min(attempt, 5)
	if delay > remoteEventsReconnectMax {
		delay = remoteEventsReconnectMax
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// readSSE accepts the subset of SSE emitted by this daemon: named events with
// JSON data. It intentionally ignores comments and event names because the
// envelope's Type is the authoritative discriminant.
func readSSE(body io.Reader, handle func([]byte) error) error {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	data := make([]string, 0, 1)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if len(data) == 0 {
				continue
			}
			if err := handle([]byte(strings.Join(data, "\n"))); err != nil {
				return err
			}
			data = data[:0]
			continue
		}
		if value, ok := strings.CutPrefix(line, "data: "); ok {
			data = append(data, value)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if len(data) > 0 {
		return handle([]byte(strings.Join(data, "\n")))
	}
	return nil
}

func qualifyRemoteEvent(hostID domain.RemoteHostID, event cdc.Event) (cdc.Event, error) {
	if event.SessionID != "" {
		event.SessionID = string(domain.QualifySessionID(hostID, domain.SessionID(event.SessionID)))
	}
	if len(event.Payload) == 0 || string(event.Payload) == "null" {
		return event, nil
	}
	var payload any
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return cdc.Event{}, fmt.Errorf("decode remote event payload: %w", err)
	}
	qualifyPayloadSessionIDs(payload, hostID, event.Type)
	encoded, err := json.Marshal(payload)
	if err != nil {
		return cdc.Event{}, fmt.Errorf("encode qualified remote event payload: %w", err)
	}
	event.Payload = encoded
	return event, nil
}

func qualifyPayloadSessionIDs(value any, hostID domain.RemoteHostID, eventType cdc.EventType) {
	switch item := value.(type) {
	case []any:
		for _, nested := range item {
			qualifyPayloadSessionIDs(nested, hostID, eventType)
		}
	case map[string]any:
		for key, nested := range item {
			if raw, ok := nested.(string); ok && payloadSessionIDKey(key, eventType) && raw != "" {
				item[key] = string(domain.QualifySessionID(hostID, domain.SessionID(raw)))
				continue
			}
			qualifyPayloadSessionIDs(nested, hostID, eventType)
		}
	}
}

func payloadSessionIDKey(key string, eventType cdc.EventType) bool {
	if key == "id" {
		return eventType == cdc.EventSessionCreated || eventType == cdc.EventSessionUpdated
	}
	switch key {
	case "sessionId", "session", "fromSession", "toSession", "currentSessionId", "handledBySessionId":
		return true
	default:
		return false
	}
}

func (c *EventsController) replay(ctx context.Context, w http.ResponseWriter, flusher http.Flusher, sentSeq *int64) error {
	for {
		events, err := c.Source.EventsAfter(ctx, *sentSeq, eventsReplayBatch)
		if err != nil {
			return err
		}
		if len(events) == 0 {
			return nil
		}
		for _, e := range events {
			if err := writeSSEEvent(w, flusher, e, sentSeq); err != nil {
				return err
			}
		}
		if len(events) < eventsReplayBatch {
			return nil
		}
	}
}

func parseEventsAfter(r *http.Request) (int64, error) {
	raw := r.URL.Query().Get("after")
	if raw == "" {
		raw = r.Header.Get("Last-Event-ID")
	}
	if raw == "" {
		return 0, nil
	}
	seq, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || seq < 0 {
		return 0, fmt.Errorf("invalid after: %q", raw)
	}
	return seq, nil
}

func writeSSEEvent(w http.ResponseWriter, flusher http.Flusher, e cdc.Event, sentSeq *int64) error {
	if e.Seq <= *sentSeq {
		return nil
	}
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", e.Seq, sseEventName(e.Type), data); err != nil {
		return err
	}
	*sentSeq = e.Seq
	flusher.Flush()
	return nil
}

func writeRemoteSSEEvent(w http.ResponseWriter, flusher http.Flusher, e cdc.Event) error {
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", sseEventName(e.Type), data); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}

func sseEventName(t cdc.EventType) string {
	return strings.NewReplacer("\r", "_", "\n", "_").Replace(string(t))
}
