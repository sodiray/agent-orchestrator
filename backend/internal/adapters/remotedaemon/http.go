package remotedaemon

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/daemonmeta"
)

const DefaultProbeTimeout = 2 * time.Second

type HTTPProber struct {
	client  *http.Client
	timeout time.Duration
}

func NewHTTPProber(client *http.Client, timeout time.Duration) *HTTPProber {
	if client == nil {
		client = &http.Client{}
	}
	if timeout <= 0 {
		timeout = DefaultProbeTimeout
	}
	return &HTTPProber{client: client, timeout: timeout}
}

func (p *HTTPProber) Probe(ctx context.Context, address string) error {
	probeCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, "http://"+address+"/healthz", nil) // #nosec G704 -- address is an operator-provided remote-host endpoint.
	if err != nil {
		return fmt.Errorf("build health probe request: %w", err)
	}
	resp, err := p.client.Do(req) // #nosec G704 -- request target is the registered remote-host endpoint.
	if err != nil {
		return fmt.Errorf("request health probe: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health probe returned HTTP %d", resp.StatusCode)
	}
	var body struct {
		Status  string `json:"status"`
		Service string `json:"service"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return fmt.Errorf("decode health probe response: %w", err)
	}
	if body.Status != "ok" {
		return fmt.Errorf("health probe reported status %q", body.Status)
	}
	if body.Service != daemonmeta.ServiceName {
		return fmt.Errorf("health probe reported unexpected service %q", body.Service)
	}
	return nil
}
