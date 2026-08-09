package httpd

import (
	"bytes"
	"context"
	"encoding/json"
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

type proxyRoundTripper func(*http.Request) (*http.Response, error)

func (f proxyRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
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

func TestSessionProxyForwardsTargetedCreationAndQualifiesResponse(t *testing.T) {
	store := &proxyStore{host: domain.RemoteHost{HostID: "workstation", Address: "remote.example:3001", LastProbeSucceeded: true}, found: true}
	proxy := newProxyForTest(store)
	proxy.client = &http.Client{Transport: proxyRoundTripper(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/sessions" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(body), "targetHostId") {
			t.Fatalf("remote body retained target: %s", body)
		}
		return &http.Response{StatusCode: http.StatusCreated, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"session":{"id":"project-7","projectId":"project","kind":"worker","terminalHandleId":"pane-7"},"promptBytes":10,"systemPromptBytes":20}`))}, nil
	})}
	handler := proxy.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("local creation handler should not run")
	}))
	request := httptest.NewRequest(http.MethodPost, "/api/v1/sessions", strings.NewReader(`{"projectId":"project","targetHostId":"workstation","prompt":"fix"}`))
	request.Header.Set("Authorization", "Bearer local-secret")
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, request)
	if res.Code != http.StatusCreated || !strings.Contains(res.Body.String(), `"id":"workstation~project-7"`) || !strings.Contains(res.Body.String(), `"terminalHandleId":"workstation~pane-7"`) {
		t.Fatalf("status=%d body=%s", res.Code, res.Body.String())
	}
}

func TestSessionProxyTargetedDelegateQualifiesWorkerID(t *testing.T) {
	store := &proxyStore{host: domain.RemoteHost{HostID: "workstation", Address: "remote.example:3001", LastProbeSucceeded: true}, found: true}
	proxy := newProxyForTest(store)
	proxy.client = &http.Client{Transport: proxyRoundTripper(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/api/v1/orchestrators/delegate" {
			t.Errorf("path = %q", r.URL.Path)
		}
		return &http.Response{StatusCode: http.StatusAccepted, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"ok":true,"workerId":"project-7","orchestratorId":"project-orchestrator"}`))}, nil
	})}
	handler := proxy.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("local delegate handler should not run")
	}))
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, httptest.NewRequest(http.MethodPost, "/api/v1/orchestrators/delegate", strings.NewReader(`{"projectId":"project","brief":"fix","targetHostId":"workstation"}`)))
	if res.Code != http.StatusAccepted || !strings.Contains(res.Body.String(), `"workerId":"workstation~project-7"`) || !strings.Contains(res.Body.String(), `"orchestratorId":"workstation~project-orchestrator"`) {
		t.Fatalf("status=%d body=%s", res.Code, res.Body.String())
	}
}

func TestSessionProxyTargetedCreationLeavesUntargetedRequestLocal(t *testing.T) {
	store := &proxyStore{}
	called := false
	handler := newProxyForTest(store).Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if string(body) != `{"projectId":"project"}` {
			t.Fatalf("body = %s", body)
		}
		w.WriteHeader(http.StatusCreated)
	}))
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, httptest.NewRequest(http.MethodPost, "/api/v1/sessions", strings.NewReader(`{"projectId":"project"}`)))
	if !called || res.Code != http.StatusCreated || store.gets != 0 {
		t.Fatalf("called=%t status=%d host reads=%d", called, res.Code, store.gets)
	}
}

func TestSessionProxyRejectsUnavailableTargetedCreationBeforeLocalHandler(t *testing.T) {
	store := &proxyStore{host: domain.RemoteHost{HostID: "workstation", LastProbeError: "remote daemon is not listening"}, found: true}
	handler := newProxyForTest(store).Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("local creation handler should not run")
	}))
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, httptest.NewRequest(http.MethodPost, "/api/v1/sessions", strings.NewReader(`{"projectId":"project","targetHostId":"workstation"}`)))
	if res.Code != http.StatusServiceUnavailable || !strings.Contains(res.Body.String(), "workstation") || !strings.Contains(res.Body.String(), "remote daemon is not listening") {
		t.Fatalf("status=%d body=%s", res.Code, res.Body.String())
	}
}

func TestSessionProxyRoutesQualifiedNotificationMutationToOwner(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch || r.URL.Path != "/api/v1/notifications/ntf_7" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "" {
			t.Error("authorization header was forwarded")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"notification":{"id":"ntf_7","sessionId":"project-7","projectId":"project","type":"needs_input","title":"input","body":"","status":"read","createdAt":"2026-08-08T10:00:00Z","target":{"kind":"session","sessionId":"project-7"}}}`))
	}))
	defer remote.Close()
	remoteURL, err := url.Parse(remote.URL)
	if err != nil {
		t.Fatal(err)
	}
	store := &proxyStore{host: domain.RemoteHost{HostID: "workstation", Address: remoteURL.Host}, found: true}
	handler := newProxyForTest(store).Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("local notification handler should not run")
	}))
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/notifications/workstation~ntf_7", bytes.NewBufferString(`{"status":"read"}`))
	req.Header.Set("Authorization", "Bearer local-secret")
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if res.Code != http.StatusOK || !strings.Contains(res.Body.String(), `"id":"workstation~ntf_7"`) || !strings.Contains(res.Body.String(), `"sessionId":"workstation~project-7"`) {
		t.Fatalf("status=%d body=%s", res.Code, res.Body.String())
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
		if r.Header.Get("Accept") != "application/json" {
			t.Errorf("accept = %q", r.Header.Get("Accept"))
		}
		for _, key := range []string{"Authorization", "Cookie", "Origin", "Referer", "X-Request-Value"} {
			if value := r.Header.Get(key); value != "" {
				t.Errorf("%s = %q, want empty", key, value)
			}
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if string(body) != `{"mode":"fast"}` {
			t.Errorf("body = %q", body)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Location", "http://127.0.0.1:3001/shutdown")
		w.Header().Set("Set-Cookie", "credential=leak")
		w.Header().Set("X-Remote-Response", "not-forwarded")
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
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer local-secret")
	req.Header.Set("Cookie", "local=secret")
	req.Header.Set("Origin", "http://localhost:3001")
	req.Header.Set("Referer", "http://localhost:3001/")
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if res.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body = %s", res.Code, res.Body.String())
	}
	for _, key := range []string{"Location", "Set-Cookie", "X-Remote-Response"} {
		if value := res.Header().Get(key); value != "" {
			t.Fatalf("%s = %q, want empty", key, value)
		}
	}
	if res.Body.String() != `{"ok":true}` {
		t.Fatalf("body = %q", res.Body.String())
	}
}

func TestSessionProxyRejectsRemoteRedirects(t *testing.T) {
	for _, requestPath := range []string{
		"/api/v1/sessions/workstation~project-7",
		"/api/v1/sessions/workstation~project-7/workspace/events",
	} {
		t.Run(requestPath, func(t *testing.T) {
			remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, "http://127.0.0.1:3001/shutdown", http.StatusTemporaryRedirect)
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
			handler.ServeHTTP(res, httptest.NewRequest(http.MethodPost, requestPath, nil))
			if res.Code != http.StatusBadGateway || !strings.Contains(res.Body.String(), "workstation") || !strings.Contains(res.Body.String(), "redirect") {
				t.Fatalf("status = %d, body = %s", res.Code, res.Body.String())
			}
		})
	}
}

func TestSessionProxyQualifiesSessionReferencesAndRemovesRemotePreview(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"sessionId":"project-7","session":{"id":"project-7","projectId":"project","kind":"worker","terminalHandleId":"pane-7","previewUrl":"http://127.0.0.1:5173"},"fromSession":"project-6","toSession":"project-8","currentSessionId":"project-7","handledBySessionId":"project-7","takenOverFrom":["project-9"]}`))
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
	handler.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/api/v1/sessions/workstation~project-7", nil))
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", res.Code, res.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"sessionId", "fromSession", "toSession", "currentSessionId", "handledBySessionId"} {
		if value := payload[key]; value != "workstation~project-7" && key != "fromSession" && key != "toSession" {
			t.Errorf("%s = %#v", key, value)
		}
	}
	if payload["fromSession"] != "workstation~project-6" || payload["toSession"] != "workstation~project-8" {
		t.Fatalf("nested references = %#v", payload)
	}
	session, ok := payload["session"].(map[string]any)
	if !ok {
		t.Fatalf("session = %#v", payload["session"])
	}
	if session["id"] != "workstation~project-7" || session["terminalHandleId"] != "workstation~pane-7" {
		t.Fatalf("session identity = %#v", session)
	}
	if _, ok := session["previewUrl"]; ok {
		t.Fatalf("preview URL = %#v, want omitted", session["previewUrl"])
	}
	takenOverFrom, ok := payload["takenOverFrom"].([]any)
	if !ok || len(takenOverFrom) != 1 || takenOverFrom[0] != "workstation~project-9" {
		t.Fatalf("takenOverFrom = %#v", payload["takenOverFrom"])
	}
}

func TestSessionProxyQualifiesTopLevelSessionResponse(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"project-7","terminalHandleId":"pane-7","previewUrl":"http://127.0.0.1:5173"}`))
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
	handler.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/api/v1/sessions/workstation~project-7", nil))
	if res.Code != http.StatusOK || !strings.Contains(res.Body.String(), "workstation~project-7") || !strings.Contains(res.Body.String(), "workstation~pane-7") {
		t.Fatalf("status = %d, body = %s", res.Code, res.Body.String())
	}
	if strings.Contains(res.Body.String(), "previewUrl") {
		t.Fatalf("body = %s, want preview unavailable", res.Body.String())
	}
}

func TestSessionProxyRejectsUnqualifiableResponse(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"sessionId":7}`))
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
	handler.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/api/v1/sessions/workstation~project-7", nil))
	if res.Code != http.StatusBadGateway || !strings.Contains(res.Body.String(), "workstation") || !strings.Contains(res.Body.String(), "unqualifiable") {
		t.Fatalf("status = %d, body = %s", res.Code, res.Body.String())
	}
}

func TestSessionProxyRoutesAdditionalSessionOperations(t *testing.T) {
	for _, tc := range []struct {
		name         string
		requestPath  string
		expectedPath string
	}{
		{name: "usage", requestPath: "/api/v1/usage/sessions/workstation~project-7", expectedPath: "/api/v1/usage/sessions/project-7"},
		{name: "orchestrator", requestPath: "/api/v1/orchestrators/workstation~project-7", expectedPath: "/api/v1/orchestrators/project-7"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != tc.expectedPath {
					t.Errorf("path = %q, want %q", r.URL.Path, tc.expectedPath)
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"sessionId":"project-7"}`))
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
			handler.ServeHTTP(res, httptest.NewRequest(http.MethodGet, tc.requestPath, nil))
			if res.Code != http.StatusOK || !strings.Contains(res.Body.String(), "workstation~project-7") {
				t.Fatalf("status = %d, body = %s", res.Code, res.Body.String())
			}
		})
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
