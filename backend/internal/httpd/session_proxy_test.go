package httpd

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	federationsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/federation"
)

type proxyStore struct {
	host  domain.RemoteHost
	found bool
	gets  int
}

func (s *proxyStore) ListRemoteHosts(context.Context) ([]domain.RemoteHost, error) { return nil, nil }

func (s *proxyStore) GetRemoteHost(_ context.Context, id domain.RemoteHostID) (domain.RemoteHost, bool, error) {
	s.gets++
	if s.found && s.host.HostID == id {
		return s.host, true, nil
	}
	return domain.RemoteHost{}, false, nil
}

func (s *proxyStore) ReplaceRemoteSessionSnapshots(context.Context, domain.RemoteHostID, []domain.RemoteSessionSnapshot) error {
	return nil
}

func (s *proxyStore) ListRemoteSessionSnapshots(context.Context, domain.RemoteHostID) ([]domain.RemoteSessionSnapshot, error) {
	return nil, nil
}

func newProxyForTest(store *proxyStore) *sessionProxy {
	federation := federationsvc.New(federationsvc.Deps{Store: store})
	return newSessionProxy(federation)
}

func TestSessionProxyLeavesBareSessionLocal(t *testing.T) {
	store := &proxyStore{}
	called := false
	handler := newProxyForTest(store).Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sessions/project-7/send", nil)
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if !called || res.Code != http.StatusNoContent {
		t.Fatalf("called = %t, status = %d", called, res.Code)
	}
	if store.gets != 0 {
		t.Fatalf("GetRemoteHost calls = %d, want 0", store.gets)
	}
}

func TestSessionProxyForwardsMethodBodyHeadersAndStatus(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			t.Errorf("method = %s", r.Method)
		}
		if r.URL.Path != "/api/v1/sessions/project-7/conversation/settings" {
			t.Errorf("path = %q", r.URL.Path)
		}
		if r.Header.Get("X-Request-Value") != "kept" {
			t.Errorf("header = %q", r.Header.Get("X-Request-Value"))
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if string(body) != `{"mode":"fast"}` {
			t.Errorf("body = %q", body)
		}
		w.Header().Set("X-Remote-Response", "kept")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer remote.Close()
	remoteURL, err := url.Parse(remote.URL)
	if err != nil {
		t.Fatal(err)
	}
	store := &proxyStore{host: domain.RemoteHost{HostID: "workstation", Address: remoteURL.Host}, found: true}
	handler := newProxyForTest(store).Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Fatal("local handler should not run")
	}))
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/sessions/workstation~project-7/conversation/settings", bytes.NewBufferString(`{"mode":"fast"}`))
	req.Header.Set("X-Request-Value", "kept")
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if res.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body = %s", res.Code, res.Body.String())
	}
	if res.Header().Get("X-Remote-Response") != "kept" {
		t.Fatalf("response header = %q", res.Header().Get("X-Remote-Response"))
	}
	if res.Body.String() != `{"ok":true}` {
		t.Fatalf("body = %q", res.Body.String())
	}
}

func TestSessionProxyNamesUnknownHost(t *testing.T) {
	handler := newProxyForTest(&proxyStore{}).Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("local handler should not run")
	}))
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/api/v1/sessions/missing-host~project-7", nil))
	if res.Code != http.StatusNotFound || !strings.Contains(res.Body.String(), "missing-host") {
		t.Fatalf("status = %d, body = %s", res.Code, res.Body.String())
	}
}

func TestSessionProxyForwardsRemoteWorkspaceStream(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/sessions/project-7/workspace/events" {
			t.Errorf("path = %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("event: workspace_changed\ndata: {}\n\n"))
	}))
	defer remote.Close()
	remoteURL, err := url.Parse(remote.URL)
	if err != nil {
		t.Fatal(err)
	}
	store := &proxyStore{host: domain.RemoteHost{HostID: "workstation", Address: remoteURL.Host}, found: true}
	handler := newProxyForTest(store).Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("local handler should not run")
	}))
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/api/v1/sessions/workstation~project-7/workspace/events", nil))
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", res.Code, res.Body.String())
	}
	if got := res.Header().Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("content type = %q", got)
	}
	if got := res.Body.String(); got != "event: workspace_changed\ndata: {}\n\n" {
		t.Fatalf("body = %q", got)
	}
}
