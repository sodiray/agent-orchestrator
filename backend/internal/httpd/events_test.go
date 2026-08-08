package httpd

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/cdc"
	"github.com/aoagents/agent-orchestrator/backend/internal/config"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/remotedaemonhttp"
	federationsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/federation"
)

type eventPresence bool

func (p eventPresence) HasRegisteredHosts() bool { return bool(p) }

type eventHostStore struct {
	hosts []domain.RemoteHost
	calls int
}

func (s *eventHostStore) ListRemoteHosts(context.Context) ([]domain.RemoteHost, error) {
	s.calls++
	return s.hosts, nil
}

func (*eventHostStore) GetRemoteHost(context.Context, domain.RemoteHostID) (domain.RemoteHost, bool, error) {
	return domain.RemoteHost{}, false, nil
}

func (*eventHostStore) ReplaceRemoteSessionSnapshots(context.Context, domain.RemoteHostID, []domain.RemoteSessionSnapshot) error {
	return nil
}

func (*eventHostStore) ListRemoteSessionSnapshots(context.Context, domain.RemoteHostID) ([]domain.RemoteSessionSnapshot, error) {
	return nil, nil
}

type fakeEventSource struct {
	live                    *fakeEventSubscriber
	sawSubscriptionOnReplay bool
}

func (s *fakeEventSource) EventsAfter(context.Context, int64, int) ([]cdc.Event, error) {
	s.sawSubscriptionOnReplay = s.live.hasSubscriber()
	s.live.publish(testCDCEvent(2))
	return []cdc.Event{testCDCEvent(1)}, nil
}

func (*fakeEventSource) LatestSeq(context.Context) (int64, error) {
	return 0, nil
}

type fakeEventSubscriber struct {
	mu sync.Mutex
	fn func(cdc.Event)
}

type tailEventSource struct {
	live      *fakeEventSubscriber
	after     int64
	latestSeq int64
}

func (s *tailEventSource) EventsAfter(_ context.Context, after int64, _ int) ([]cdc.Event, error) {
	s.after = after
	s.live.publish(testCDCEvent(after + 1))
	return nil, nil
}

func (s *tailEventSource) LatestSeq(context.Context) (int64, error) {
	return s.latestSeq, nil
}

type cancelOnEventWriter struct {
	header http.Header
	body   bytes.Buffer
	cancel context.CancelFunc
}

func (w *cancelOnEventWriter) Header() http.Header {
	return w.header
}

func (w *cancelOnEventWriter) Write(data []byte) (int, error) {
	if _, err := w.body.Write(data); err != nil {
		return 0, err
	}
	if bytes.Contains(data, []byte("id: 2049\n")) {
		w.cancel()
	}
	return len(data), nil
}

func (*cancelOnEventWriter) WriteHeader(int) {}

func (*cancelOnEventWriter) Flush() {}

func (s *fakeEventSubscriber) Subscribe(fn func(cdc.Event)) func() {
	s.mu.Lock()
	s.fn = fn
	s.mu.Unlock()
	return func() {
		s.mu.Lock()
		s.fn = nil
		s.mu.Unlock()
	}
}

func (s *fakeEventSubscriber) hasSubscriber() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fn != nil
}

func (s *fakeEventSubscriber) publish(e cdc.Event) {
	s.mu.Lock()
	fn := s.fn
	s.mu.Unlock()
	if fn != nil {
		fn(e)
	}
}

func TestEventsStreamSubscribesBeforeReplayAndDrainsBufferedLive(t *testing.T) {
	live := &fakeEventSubscriber{}
	src := &fakeEventSource{live: live}
	router := NewRouterWithControl(config.Config{}, discardLogger(), nil, APIDeps{
		CDC:    src,
		Events: live,
	}, ControlDeps{})
	ts := httptest.NewServer(router)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/api/v1/events?after=0", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /api/v1/events: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("Content-Type = %q, want text/event-stream", ct)
	}
	if got := resp.Header.Get("X-Accel-Buffering"); got != "no" {
		t.Fatalf("X-Accel-Buffering = %q, want no", got)
	}

	ids := readSSEIDs(t, resp.Body, 2)
	if got, want := strings.Join(ids, ","), "1,2"; got != want {
		t.Fatalf("ids = %s, want %s", got, want)
	}
	if !src.sawSubscriptionOnReplay {
		t.Fatal("replay started before live subscription was installed")
	}
}

func TestEventsStreamTailSkipsHistoryWithoutLosingCaptureSubscriptionGap(t *testing.T) {
	live := &fakeEventSubscriber{}
	src := &tailEventSource{live: live, latestSeq: 2048}
	controller := &EventsController{Source: src, Live: live}
	ctx, cancel := context.WithCancel(context.Background())
	writer := &cancelOnEventWriter{header: make(http.Header), cancel: cancel}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/events?after=0&tail=1", nil).WithContext(ctx)

	controller.stream(writer, req)

	if src.after != 2048 {
		t.Fatalf("tail replay cursor = %d, want 2048", src.after)
	}
	if got := writer.body.String(); strings.Contains(got, "id: 1\n") || !strings.Contains(got, "id: 2049\n") {
		t.Fatalf("tail stream body = %q, want only the post-capture event", got)
	}
}

func TestEventsStreamRejectsInvalidAfter(t *testing.T) {
	router := NewRouterWithControl(config.Config{}, discardLogger(), nil, APIDeps{
		CDC:    &fakeEventSource{live: &fakeEventSubscriber{}},
		Events: &fakeEventSubscriber{},
	}, ControlDeps{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/events?after=nope", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if !strings.Contains(rec.Body.String(), "INVALID_AFTER") {
		t.Fatalf("body = %s, want INVALID_AFTER", rec.Body.String())
	}
}

func TestWriteSSEEventSanitizesEventNameNewlines(t *testing.T) {
	rec := httptest.NewRecorder()
	sentSeq := int64(0)
	e := testCDCEvent(1)
	e.Type = cdc.EventType("session_updated\nid: 999\rdata: injected")

	if err := writeSSEEvent(rec, rec, e, &sentSeq); err != nil {
		t.Fatalf("writeSSEEvent: %v", err)
	}

	body := rec.Body.String()
	if strings.Contains(body, "\nid: 999") || strings.Contains(body, "\rdata: injected") {
		t.Fatalf("body contains injected SSE field: %q", body)
	}
	if !strings.Contains(body, "event: session_updated_id: 999_data: injected\n") {
		t.Fatalf("body = %q, want sanitized event name", body)
	}
}

func TestQualifyRemoteEventQualifiesEnvelopeAndEveryPayloadSessionReference(t *testing.T) {
	event, err := qualifyRemoteEvent("workstation", cdc.Event{
		Seq:       7,
		SessionID: "project-7",
		Type:      cdc.EventSessionUpdated,
		Payload: []byte(`{
			"id":"project-7",
			"sessionId":"project-7",
			"session":"project-7",
			"fromSession":"project-6",
			"toSession":"project-8",
			"nested":{"currentSessionId":"project-7","handledBySessionId":"project-7"}
		}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if event.SessionID != "workstation~project-7" {
		t.Fatalf("sessionId = %q", event.SessionID)
	}
	var payload map[string]any
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"id", "sessionId", "session", "fromSession", "toSession"} {
		if got, ok := payload[key].(string); !ok || !strings.HasPrefix(got, "workstation~") {
			t.Fatalf("payload[%q] = %#v, want qualified session id", key, payload[key])
		}
	}
	nested := payload["nested"].(map[string]any)
	for _, key := range []string{"currentSessionId", "handledBySessionId"} {
		if got := nested[key]; got != "workstation~project-7" {
			t.Fatalf("payload.nested[%q] = %#v", key, got)
		}
	}
}

func TestEventsRemoteFanInIsNoOpWithoutRegisteredHosts(t *testing.T) {
	store := &eventHostStore{}
	controller := &EventsController{Federation: federationsvc.New(federationsvc.Deps{
		Store:    store,
		Presence: eventPresence(false),
	})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	remote, wait := controller.remoteEvents(ctx)
	defer wait()
	if store.calls != 0 {
		t.Fatalf("remote-host reads = %d, want 0", store.calls)
	}
	select {
	case event := <-remote:
		t.Fatalf("unexpected remote event: %#v", event)
	default:
	}
}

func TestEventsRemoteFanInRefusesRedirectFromInjectedClient(t *testing.T) {
	targetRequests := make(chan struct{}, 1)
	controller := &EventsController{Client: &http.Client{
		Transport: eventRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Host == "target.example" {
				targetRequests <- struct{}{}
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("")), Request: req}, nil
			}
			return &http.Response{StatusCode: http.StatusTemporaryRedirect, Header: http.Header{"Location": []string{"http://target.example"}}, Body: io.NopCloser(strings.NewReader("")), Request: req}, nil
		}),
		CheckRedirect: func(*http.Request, []*http.Request) error { return nil },
	}}
	err := controller.readRemoteEvents(context.Background(), domain.RemoteHost{HostID: "workstation", Address: "remote.example"}, 0, true, func(cdc.Event) error { return nil })
	if !errors.Is(err, remotedaemonhttp.ErrRedirect) {
		t.Fatalf("readRemoteEvents() error = %v, want refused redirect", err)
	}
	select {
	case <-targetRequests:
		t.Fatal("remote event fan-in followed remote redirect")
	default:
	}
}

func TestRemoteEventFanInReconnectsAfterDroppedStream(t *testing.T) {
	type remoteRequest struct {
		after string
		tail  string
	}
	requests := make(chan remoteRequest, 4)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- remoteRequest{after: r.URL.Query().Get("after"), tail: r.URL.Query().Get("tail")}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		after := r.URL.Query().Get("after")
		if after == "0" {
			_, _ = w.Write([]byte("id: 1\nevent: session_updated\ndata: {\"seq\":1,\"sessionId\":\"project-7\",\"type\":\"session_updated\",\"payload\":{\"id\":\"project-7\"}}\n\n"))
			flusher.Flush()
			return
		}
		_, _ = w.Write([]byte("id: 2\nevent: session_updated\ndata: {\"seq\":2,\"sessionId\":\"project-7\",\"type\":\"session_updated\",\"payload\":{\"id\":\"project-7\"}}\n\n"))
		flusher.Flush()
		<-r.Context().Done()
	}))
	defer remote.Close()
	remoteURL, err := url.Parse(remote.URL)
	if err != nil {
		t.Fatal(err)
	}
	store := &eventHostStore{hosts: []domain.RemoteHost{{HostID: "workstation", Address: remoteURL.Host}}}
	controller := &EventsController{Federation: federationsvc.New(federationsvc.Deps{
		Store:    store,
		Presence: eventPresence(true),
	})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan cdc.Event, 2)
	done := make(chan struct{})
	go func() {
		defer close(done)
		controller.forwardRemoteEvents(ctx, store.hosts[0], out)
	}()
	for index := 0; index < 2; index++ {
		select {
		case event := <-out:
			if event.SessionID != "workstation~project-7" {
				t.Fatalf("qualified session id = %q", event.SessionID)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for forwarded remote event")
		}
	}
	if first, second := <-requests, <-requests; first.after != "0" || first.tail != "1" || second.after != "1" || second.tail != "" {
		t.Fatalf("remote replay requests = %#v, %#v; want tail then resume after 1", first, second)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("remote fan-in did not stop after local stream cancellation")
	}
}

func TestRemoteEventFanInLargeBacklogBackpressuresWithoutCancelingLocalStream(t *testing.T) {
	requests := make(chan url.Values, 1)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.URL.Query()
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		for sequence := int64(1); sequence <= eventsLiveBuffer+1; sequence++ {
			_, _ = w.Write([]byte("data: {\"seq\":" + strconv.FormatInt(sequence, 10) + ",\"sessionId\":\"project-7\",\"type\":\"session_updated\"}\n\n"))
			flusher.Flush()
		}
		<-r.Context().Done()
	}))
	defer remote.Close()
	remoteURL, err := url.Parse(remote.URL)
	if err != nil {
		t.Fatal(err)
	}
	controller := &EventsController{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan cdc.Event, eventsLiveBuffer)
	done := make(chan struct{})
	go func() {
		defer close(done)
		controller.forwardRemoteEvents(ctx, domain.RemoteHost{HostID: "workstation", Address: remoteURL.Host}, out)
	}()
	select {
	case request := <-requests:
		if request.Get("tail") != "1" || request.Get("after") != "0" {
			t.Fatalf("initial remote request = %s, want after=0&tail=1", request.Encode())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for remote subscription")
	}
	deadline := time.After(3 * time.Second)
	for len(out) != eventsLiveBuffer {
		select {
		case <-deadline:
			t.Fatalf("forwarded events = %d, want %d", len(out), eventsLiveBuffer)
		default:
			time.Sleep(time.Millisecond)
		}
	}
	select {
	case <-done:
		t.Fatal("large remote backlog ended the local stream")
	default:
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("remote fan-in did not stop after local stream cancellation")
	}
}

func readSSEIDs(t *testing.T, r io.Reader, want int) []string {
	t.Helper()
	ids := make([]string, 0, want)
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "id: ") {
			ids = append(ids, strings.TrimPrefix(line, "id: "))
			if len(ids) == want {
				return ids
			}
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read stream: %v", err)
	}
	t.Fatalf("stream ended after ids %v, want %d ids", ids, want)
	return nil
}

type eventRoundTripFunc func(*http.Request) (*http.Response, error)

func (f eventRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// dedupeEventSource publishes a duplicate of its replay event plus a new event
// into the live channel so the dedupe path in writeSSEEvent can be exercised.
type dedupeEventSource struct {
	live *fakeEventSubscriber
}

func (s *dedupeEventSource) EventsAfter(_ context.Context, _ int64, _ int) ([]cdc.Event, error) {
	// Both published before replay returns: seq=5 duplicates the replay event;
	// seq=6 is genuinely new. After replay sentSeq=5, so seq=5 must be dropped
	// and seq=6 sent.
	s.live.publish(testCDCEvent(5))
	s.live.publish(testCDCEvent(6))
	return []cdc.Event{testCDCEvent(5)}, nil
}

func (*dedupeEventSource) LatestSeq(context.Context) (int64, error) { return 0, nil }

// TestEventsStreamDeduplicatesLiveEventOverlappingReplay verifies that a live
// event whose seq falls within the already-replayed range is silently dropped,
// so the client sees each seq exactly once.
func TestEventsStreamDeduplicatesLiveEventOverlappingReplay(t *testing.T) {
	live := &fakeEventSubscriber{}
	src := &dedupeEventSource{live: live}
	router := NewRouterWithControl(config.Config{}, discardLogger(), nil, APIDeps{
		CDC:    src,
		Events: live,
	}, ControlDeps{})
	ts := httptest.NewServer(router)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/api/v1/events?after=0", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /api/v1/events: %v", err)
	}
	defer resp.Body.Close()

	// Replay emits seq=5; live buffer holds seq=5 (dup) then seq=6 (new).
	// writeSSEEvent must drop seq=5 from the live drain (5 <= sentSeq=5).
	// The client must see exactly [5, 6], not [5, 5, 6].
	ids := readSSEIDs(t, resp.Body, 2)
	if got, want := strings.Join(ids, ","), "5,6"; got != want {
		t.Fatalf("ids = %s, want %s (duplicate seq was not deduped)", got, want)
	}
}

// lastEventIDSource returns a single event whose seq is calledAfter+1, letting
// the test prove EventsAfter received the cursor from Last-Event-ID by checking
// the event seq the client receives.
type lastEventIDSource struct {
	live *fakeEventSubscriber
}

func (s *lastEventIDSource) EventsAfter(_ context.Context, after int64, _ int) ([]cdc.Event, error) {
	return []cdc.Event{testCDCEvent(after + 1)}, nil
}

func (*lastEventIDSource) LatestSeq(context.Context) (int64, error) { return 0, nil }

// TestEventsStreamParsesLastEventIDHeader verifies that the Last-Event-ID
// request header is used as the replay cursor when the after query param is
// absent. The source returns after+1, so receiving seq=8 proves the cursor
// was parsed as 7.
func TestEventsStreamParsesLastEventIDHeader(t *testing.T) {
	live := &fakeEventSubscriber{}
	src := &lastEventIDSource{live: live}
	router := NewRouterWithControl(config.Config{}, discardLogger(), nil, APIDeps{
		CDC:    src,
		Events: live,
	}, ControlDeps{})
	ts := httptest.NewServer(router)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/api/v1/events", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Last-Event-ID", "7")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /api/v1/events: %v", err)
	}
	defer resp.Body.Close()

	// EventsAfter(after=7) returns seq=8. Receiving seq=8 proves the header
	// was parsed correctly. If the header were ignored (after=0 default),
	// EventsAfter would return seq=1 and this assertion would fail.
	ids := readSSEIDs(t, resp.Body, 1)
	if got, want := ids[0], "8"; got != want {
		t.Fatalf("id = %q, want %q (Last-Event-ID header was not used as cursor)", got, want)
	}
}

func testCDCEvent(seq int64) cdc.Event {
	return cdc.Event{
		Seq:       seq,
		ProjectID: "proj_1",
		SessionID: "sess_1",
		Type:      cdc.EventSessionUpdated,
		Payload:   json.RawMessage(`{"status":"running"}`),
		CreatedAt: time.Unix(seq, 0).UTC(),
	}
}
