package remotedaemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/daemonmeta"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/remotedaemonhttp"
)

func TestHTTPProberAcceptsExpectedDaemon(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			t.Fatalf("path = %q, want /healthz", r.URL.Path)
		}
		_, _ = fmt.Fprintf(w, `{"status":"ok","service":%q}`, daemonmeta.ServiceName)
	}))
	t.Cleanup(srv.Close)
	if err := NewHTTPProber(nil, 0).Probe(context.Background(), strings.TrimPrefix(srv.URL, "http://")); err != nil {
		t.Fatalf("probe: %v", err)
	}
}

func TestHTTPProberRejectsUnexpectedService(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":"ok","service":"other"}`))
	}))
	t.Cleanup(srv.Close)
	err := NewHTTPProber(nil, 0).Probe(context.Background(), strings.TrimPrefix(srv.URL, "http://"))
	if err == nil || !strings.Contains(err.Error(), "unexpected service") {
		t.Fatalf("probe error = %v, want unexpected service", err)
	}
}

func TestHTTPProberRefusesRemoteRedirect(t *testing.T) {
	targetRequests := make(chan struct{}, 1)
	client := redirectingClient(targetRequests)
	err := NewHTTPProber(client, 0).Probe(context.Background(), "remote.example")
	if !errors.Is(err, remotedaemonhttp.ErrRedirect) {
		t.Fatalf("Probe() error = %v, want refused redirect", err)
	}
	select {
	case <-targetRequests:
		t.Fatal("health probe followed remote redirect")
	default:
	}
}

func TestHTTPSessionListerReadsNativeSessionViews(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/sessions" {
			t.Errorf("path = %q", r.URL.Path)
		}
		if r.URL.Query().Get("project") != "project" || r.URL.Query().Get("active") != "true" {
			t.Errorf("query = %q", r.URL.RawQuery)
		}
		if r.Header.Get("X-AO-Federation-Local") != "1" {
			t.Errorf("federation header = %q", r.Header.Get("X-AO-Federation-Local"))
		}
		_, _ = w.Write([]byte(`{"sessions":[{"id":"project-7","projectId":"project","kind":"worker","status":"idle","prs":[]}]}`))
	}))
	t.Cleanup(srv.Close)
	sessions, err := NewHTTPSessionLister(nil, 0).ListSessions(context.Background(), strings.TrimPrefix(srv.URL, "http://"), ports.RemoteSessionListFilter{
		Project: "project",
		Active:  boolPtr(true),
	})
	if err != nil {
		t.Fatalf("ListSessions() error = %v", err)
	}
	if len(sessions) != 1 || sessions[0].SessionID != domain.SessionID("project-7") {
		t.Fatalf("sessions = %#v", sessions)
	}
}

func TestHTTPSessionListerRefusesRemoteRedirect(t *testing.T) {
	targetRequests := make(chan struct{}, 1)
	client := redirectingClient(targetRequests)
	_, err := NewHTTPSessionLister(client, 0).ListSessions(context.Background(), "remote.example", ports.RemoteSessionListFilter{})
	if !errors.Is(err, remotedaemonhttp.ErrRedirect) {
		t.Fatalf("ListSessions() error = %v, want refused redirect", err)
	}
	select {
	case <-targetRequests:
		t.Fatal("session lister followed remote redirect")
	default:
	}
}

func TestHTTPSessionListerReadsNativeNotifications(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/notifications" || r.URL.Query().Get("status") != "unread" || r.URL.Query().Get("limit") != "10" {
			t.Errorf("request = %s?%s", r.URL.Path, r.URL.RawQuery)
		}
		if r.Header.Get("X-AO-Federation-Local") != "1" {
			t.Errorf("federation header = %q", r.Header.Get("X-AO-Federation-Local"))
		}
		_, _ = w.Write([]byte(`{"notifications":[{"id":"ntf_7","sessionId":"project-7","projectId":"project","prUrl":"","type":"needs_input","title":"input","body":"","status":"unread","createdAt":"2026-08-08T10:00:00Z"}],"unreadCount":1,"unresolvedCount":1}`))
	}))
	t.Cleanup(srv.Close)
	page, err := NewHTTPSessionLister(nil, 0).ListNotifications(context.Background(), strings.TrimPrefix(srv.URL, "http://"), domain.NotificationListUnread, 10, "")
	if err != nil {
		t.Fatalf("ListNotifications() error = %v", err)
	}
	if len(page.Notifications) != 1 || page.Notifications[0].ID != "ntf_7" || page.Notifications[0].SessionID != "project-7" || page.UnreadCount != 1 {
		t.Fatalf("page = %#v", page)
	}
}

func boolPtr(v bool) *bool { return &v }

func redirectingClient(targetRequests chan<- struct{}) *http.Client {
	return &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Host == "target.example" {
				targetRequests <- struct{}{}
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("{}")), Request: req}, nil
			}
			return &http.Response{StatusCode: http.StatusTemporaryRedirect, Header: http.Header{"Location": []string{"http://target.example"}}, Body: io.NopCloser(strings.NewReader("")), Request: req}, nil
		}),
		CheckRedirect: func(*http.Request, []*http.Request) error { return nil },
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
