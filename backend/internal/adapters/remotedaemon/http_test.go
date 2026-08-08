package remotedaemon

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/daemonmeta"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
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
