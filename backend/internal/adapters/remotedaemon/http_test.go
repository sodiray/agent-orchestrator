package remotedaemon

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/daemonmeta"
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
