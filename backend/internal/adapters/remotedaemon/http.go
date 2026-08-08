package remotedaemon

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/daemonmeta"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

const DefaultProbeTimeout = 2 * time.Second

const DefaultSessionListTimeout = 2 * time.Second

type HTTPProber struct {
	client  *http.Client
	timeout time.Duration
}

// HTTPSessionLister reads the local session endpoint of a registered daemon.
type HTTPSessionLister struct {
	client  *http.Client
	timeout time.Duration
}

var _ ports.RemoteDaemonSessionLister = (*HTTPSessionLister)(nil)

func NewHTTPSessionLister(client *http.Client, timeout time.Duration) *HTTPSessionLister {
	if client == nil {
		client = &http.Client{}
	}
	if timeout <= 0 {
		timeout = DefaultSessionListTimeout
	}
	return &HTTPSessionLister{client: client, timeout: timeout}
}

func (l *HTTPSessionLister) ListSessions(ctx context.Context, address string, filter ports.RemoteSessionListFilter) ([]domain.RemoteSessionSnapshot, error) {
	listCtx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()
	endpoint := url.URL{Scheme: "http", Host: address, Path: "/api/v1/sessions"}
	endpoint.RawQuery = remoteSessionQuery(filter).Encode()
	req, err := http.NewRequestWithContext(listCtx, http.MethodGet, endpoint.String(), nil) // #nosec G704 -- address is an operator-provided remote-host endpoint.
	if err != nil {
		return nil, fmt.Errorf("build remote session request: %w", err)
	}
	// Remote daemons normally aggregate too. This header asks the owner for its
	// native sessions only, avoiding recursive qualification across hosts.
	req.Header.Set("X-AO-Federation-Local", "1")
	resp, err := l.client.Do(req) // #nosec G704 -- request target is the registered remote-host endpoint.
	if err != nil {
		return nil, fmt.Errorf("request remote sessions: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("remote session list returned HTTP %d", resp.StatusCode)
	}
	var body struct {
		Sessions []json.RawMessage `json:"sessions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("decode remote session list: %w", err)
	}
	out := make([]domain.RemoteSessionSnapshot, 0, len(body.Sessions))
	for _, view := range body.Sessions {
		var identity struct {
			ID domain.SessionID `json:"id"`
		}
		if err := json.Unmarshal(view, &identity); err != nil {
			return nil, fmt.Errorf("decode remote session identity: %w", err)
		}
		if identity.ID == "" || !json.Valid(view) {
			return nil, fmt.Errorf("remote session list included an invalid session")
		}
		if strings.Contains(string(identity.ID), "~") {
			return nil, fmt.Errorf("remote session list included a qualified session id %q", identity.ID)
		}
		out = append(out, domain.RemoteSessionSnapshot{SessionID: identity.ID, View: view})
	}
	return out, nil
}

func remoteSessionQuery(filter ports.RemoteSessionListFilter) url.Values {
	query := url.Values{}
	if filter.Project != "" {
		query.Set("project", string(filter.Project))
	}
	if filter.Active != nil {
		query.Set("active", strconv.FormatBool(*filter.Active))
	}
	if filter.OrchestratorOnly {
		query.Set("orchestratorOnly", strconv.FormatBool(filter.OrchestratorOnly))
	}
	if filter.Fresh {
		query.Set("fresh", strconv.FormatBool(filter.Fresh))
	}
	return query
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
