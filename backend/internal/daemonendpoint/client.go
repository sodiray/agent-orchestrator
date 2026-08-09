// Package daemonendpoint builds HTTP clients for the local daemon endpoint.
package daemonendpoint

import (
	"context"
	"net"
	"net/http"
	"strconv"

	"github.com/aoagents/agent-orchestrator/backend/internal/config"
	"github.com/aoagents/agent-orchestrator/backend/internal/runfile"
)

// BaseURL returns the URL used with an HTTP client configured for info. Unix
// socket clients retain localhost as the HTTP Host so local-only routes keep
// their existing request validation.
func BaseURL(info *runfile.Info) string {
	if info != nil && info.SocketPath != "" {
		return "http://localhost"
	}
	if info == nil {
		return ""
	}
	return "http://" + net.JoinHostPort(config.LoopbackHost, strconv.Itoa(info.Port))
}

// Client returns a copy of base that dials socketPath when it is non-empty.
// A socket transport always ignores the URL address and connects only to the
// filesystem path supplied by the daemon's run-file.
func Client(base *http.Client, socketPath string) *http.Client {
	if base == nil {
		base = &http.Client{}
	}
	client := *base
	if socketPath == "" {
		return &client
	}
	transport, ok := client.Transport.(*http.Transport)
	if !ok || transport == nil {
		transport = http.DefaultTransport.(*http.Transport).Clone()
	} else {
		transport = transport.Clone()
	}
	dialer := &net.Dialer{}
	transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		return dialer.DialContext(ctx, "unix", socketPath)
	}
	client.Transport = transport
	return &client
}
