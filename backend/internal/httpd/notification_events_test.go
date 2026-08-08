package httpd

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/controllers"
	federationsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/federation"
)

type notificationTestStream struct {
	ch           chan domain.NotificationEvent
	subscribes   int
	unsubscribes int
}

func (s *notificationTestStream) Subscribe(domain.ProjectID) (<-chan domain.NotificationEvent, func()) {
	s.subscribes++
	return s.ch, func() { s.unsubscribes++ }
}

var _ controllers.NotificationStream = (*notificationTestStream)(nil)

func TestFederatedNotificationStreamFansInAndTearsDownRemoteSubscription(t *testing.T) {
	remoteDone := make(chan struct{})
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("tail") != "1" || r.Header.Get("X-AO-Federation-Local") != "1" {
			t.Errorf("query=%s federation=%q", r.URL.RawQuery, r.Header.Get("X-AO-Federation-Local"))
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: notification_created\ndata: {\"id\":\"ntf_7\",\"sessionId\":\"project-7\",\"projectId\":\"project\",\"type\":\"needs_input\",\"title\":\"input\",\"body\":\"\",\"status\":\"unread\",\"createdAt\":\"2026-08-08T10:00:00Z\"}\n\n"))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(remoteDone)
	}))
	defer remote.Close()
	remoteURL, err := url.Parse(remote.URL)
	if err != nil {
		t.Fatal(err)
	}
	store := &eventHostStore{hosts: []domain.RemoteHost{{HostID: "workstation", Address: remoteURL.Host}}}
	local := &notificationTestStream{ch: make(chan domain.NotificationEvent)}
	stream := newFederatedNotificationStream(local, federationsvc.New(federationsvc.Deps{Store: store, Presence: eventPresence(true)}))
	ch, unsubscribe := stream.Subscribe("")
	select {
	case event := <-ch:
		if event.Kind != domain.NotificationCreated || event.Record.ID != "workstation~ntf_7" || event.Record.SessionID != "workstation~project-7" {
			t.Fatalf("event = %#v", event)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for remote notification")
	}
	done := make(chan struct{})
	go func() {
		unsubscribe()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("notification fan-in teardown did not return")
	}
	select {
	case <-remoteDone:
	case <-time.After(3 * time.Second):
		t.Fatal("remote notification socket was not closed")
	}
	if local.unsubscribes != 1 {
		t.Fatalf("local unsubscribes=%d", local.unsubscribes)
	}
}

func TestFederatedNotificationStreamWithoutHostsIsStrictLocalNoOp(t *testing.T) {
	store := &eventHostStore{}
	local := &notificationTestStream{ch: make(chan domain.NotificationEvent)}
	stream := newFederatedNotificationStream(local, federationsvc.New(federationsvc.Deps{Store: store, Presence: eventPresence(false)}))
	_, unsubscribe := stream.Subscribe("")
	unsubscribe()
	if store.calls != 0 || local.subscribes != 1 || local.unsubscribes != 1 {
		t.Fatalf("store calls=%d subscribes=%d unsubscribes=%d", store.calls, local.subscribes, local.unsubscribes)
	}
}

func TestReadNotificationSSERejectsMalformedOwnerEvent(t *testing.T) {
	err := readNotificationSSE(&testReader{value: "event: notification_created\ndata: {}\n\n"}, func(domain.NotificationEvent) error { return nil })
	if err == nil {
		t.Fatal("expected invalid remote notification error")
	}
}

type testReader struct{ value string }

func (r *testReader) Read(p []byte) (int, error) {
	if r.value == "" {
		return 0, context.Canceled
	}
	count := copy(p, r.value)
	r.value = r.value[count:]
	return count, nil
}
