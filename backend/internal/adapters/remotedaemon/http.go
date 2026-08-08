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
	"github.com/aoagents/agent-orchestrator/backend/internal/remotedaemonhttp"
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
var _ ports.RemoteDaemonNotificationLister = (*HTTPSessionLister)(nil)

func NewHTTPSessionLister(client *http.Client, timeout time.Duration) *HTTPSessionLister {
	if timeout <= 0 {
		timeout = DefaultSessionListTimeout
	}
	return &HTTPSessionLister{client: remotedaemonhttp.EnforceRedirectRefusal(client), timeout: timeout}
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

// ListNotifications reads native notification rows from an owning daemon. The
// federation header prevents a remote daemon from recursively aggregating its
// own registered hosts into this owner's response.
func (l *HTTPSessionLister) ListNotifications(ctx context.Context, address string, status domain.NotificationListStatus, limit int, cursor string) (ports.RemoteNotificationListPage, error) {
	listCtx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()
	endpoint := url.URL{Scheme: "http", Host: address, Path: "/api/v1/notifications"}
	query := endpoint.Query()
	query.Set("status", string(status))
	query.Set("limit", strconv.Itoa(limit))
	if cursor != "" {
		query.Set("cursor", cursor)
	}
	endpoint.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(listCtx, http.MethodGet, endpoint.String(), nil) // #nosec G704 -- address is an operator-provided remote-host endpoint.
	if err != nil {
		return ports.RemoteNotificationListPage{}, fmt.Errorf("build remote notification request: %w", err)
	}
	req.Header.Set("X-AO-Federation-Local", "1")
	resp, err := l.client.Do(req) // #nosec G704 -- request target is the registered remote-host endpoint.
	if err != nil {
		return ports.RemoteNotificationListPage{}, fmt.Errorf("request remote notifications: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return ports.RemoteNotificationListPage{}, fmt.Errorf("remote notification list returned HTTP %d", resp.StatusCode)
	}
	var body struct {
		Notifications   []remoteNotification `json:"notifications"`
		NextCursor      string               `json:"nextCursor"`
		UnreadCount     int                  `json:"unreadCount"`
		UnresolvedCount int                  `json:"unresolvedCount"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return ports.RemoteNotificationListPage{}, fmt.Errorf("decode remote notification list: %w", err)
	}
	rows := make([]domain.NotificationRecord, 0, len(body.Notifications))
	for _, notification := range body.Notifications {
		record := domain.NotificationRecord{
			ID: notification.ID, SessionID: domain.SessionID(notification.SessionID), ProjectID: domain.ProjectID(notification.ProjectID),
			PRURL: notification.PRURL, Type: domain.NotificationType(notification.Type), Title: notification.Title, Body: notification.Body,
			Status: domain.NotificationStatus(notification.Status), CreatedAt: notification.CreatedAt,
		}
		if notification.ResolvedAt != nil {
			record.ResolvedAt = *notification.ResolvedAt
		}
		if record.ID == "" || record.Validate() != nil || strings.Contains(record.ID, "~") || strings.Contains(string(record.SessionID), "~") {
			return ports.RemoteNotificationListPage{}, fmt.Errorf("remote notification list included an invalid notification")
		}
		rows = append(rows, record)
	}
	return ports.RemoteNotificationListPage{Notifications: rows, NextCursor: body.NextCursor, UnreadCount: body.UnreadCount, UnresolvedCount: body.UnresolvedCount}, nil
}

type remoteNotification struct {
	ID         string     `json:"id"`
	SessionID  string     `json:"sessionId"`
	ProjectID  string     `json:"projectId"`
	PRURL      string     `json:"prUrl"`
	Type       string     `json:"type"`
	Title      string     `json:"title"`
	Body       string     `json:"body"`
	Status     string     `json:"status"`
	CreatedAt  time.Time  `json:"createdAt"`
	ResolvedAt *time.Time `json:"resolvedAt"`
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
	if timeout <= 0 {
		timeout = DefaultProbeTimeout
	}
	return &HTTPProber{client: remotedaemonhttp.EnforceRedirectRefusal(client), timeout: timeout}
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
