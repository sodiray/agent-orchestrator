package httpd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/envelope"
	"github.com/aoagents/agent-orchestrator/backend/internal/remotedaemonhttp"
	federationsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/federation"
)

const (
	remoteSessionProxyTimeout = 10 * time.Second
	maxRemoteJSONResponseSize = 32 << 20
)

var errRemoteRedirect = remotedaemonhttp.ErrRedirect

var remoteRequestHeaderAllowlist = []string{
	"Accept",
	"Content-Type",
	"If-Match",
	"If-Modified-Since",
	"If-None-Match",
	"If-Unmodified-Since",
	"Range",
	"X-Request-Id",
}

var remoteResponseHeaderAllowlist = []string{
	"Cache-Control",
	"Content-Type",
	"ETag",
	"Last-Modified",
	"X-Request-Id",
}

type sessionProxy struct {
	federation   *federationsvc.Service
	client       *http.Client
	streamClient *http.Client
	log          *slog.Logger
}

func newSessionProxy(federation *federationsvc.Service) *sessionProxy {
	return &sessionProxy{
		federation:   federation,
		client:       newRemoteDaemonClient(remoteSessionProxyTimeout),
		streamClient: newRemoteDaemonClient(0),
		log:          slog.Default(),
	}
}

func newRemoteDaemonClient(timeout time.Duration) *http.Client {
	return remotedaemonhttp.NewClient(timeout)
}

// Middleware forwards only qualified session-bearing operations. Bare local
// IDs take the existing handler path without a host-registry read.
func (p *sessionProxy) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if qualified, ok := qualifiedNotificationFromPath(r.URL.Path); ok {
			host, found, err := p.federation.Resolve(r.Context(), qualified.HostID)
			if err != nil {
				p.log.Error("resolve remote notification host failed", "hostId", qualified.HostID, "err", err)
				envelope.WriteAPIError(w, r, http.StatusInternalServerError, "internal", "REMOTE_HOST_RESOLUTION_FAILED",
					"Could not resolve the remote notification host", map[string]any{"hostId": qualified.HostID})
				return
			}
			if !found {
				p.log.Warn("remote notification proxy host is not registered", "hostId", qualified.HostID)
				envelope.WriteAPIError(w, r, http.StatusNotFound, "not_found", "REMOTE_HOST_NOT_FOUND",
					fmt.Sprintf("Remote host %q is not registered", qualified.HostID), map[string]any{"hostId": qualified.HostID})
				return
			}
			if host.OperatorState == domain.RemoteHostStateStopped || host.OperatorState == domain.RemoteHostStateDestroyed {
				reason := fmt.Sprintf("remote host is %s", host.OperatorState)
				p.log.Warn("remote notification proxy unavailable", "hostId", host.HostID, "reason", reason)
				envelope.WriteAPIError(w, r, http.StatusServiceUnavailable, "unavailable", "REMOTE_HOST_UNAVAILABLE",
					fmt.Sprintf("Remote host %q is unavailable: %s", host.HostID, reason), map[string]any{"hostId": host.HostID, "reason": reason})
				return
			}
			p.forwardNotification(w, r, host, qualified)
			return
		}
		qualified, ok := qualifiedSessionFromPath(r.URL.Path)
		if !ok {
			next.ServeHTTP(w, r)
			return
		}
		host, found, err := p.federation.Resolve(r.Context(), qualified.HostID)
		if err != nil {
			p.log.Error("resolve remote session host failed", "hostId", qualified.HostID, "err", err)
			envelope.WriteAPIError(w, r, http.StatusInternalServerError, "internal", "REMOTE_HOST_RESOLUTION_FAILED",
				"Could not resolve the remote session host", map[string]any{"hostId": qualified.HostID})
			return
		}
		if !found {
			p.log.Warn("remote session proxy host is not registered", "hostId", qualified.HostID, "reason", "remote host is not registered")
			envelope.WriteAPIError(w, r, http.StatusNotFound, "not_found", "REMOTE_HOST_NOT_FOUND",
				fmt.Sprintf("Remote host %q is not registered", qualified.HostID), map[string]any{"hostId": qualified.HostID})
			return
		}
		if host.OperatorState == domain.RemoteHostStateStopped || host.OperatorState == domain.RemoteHostStateDestroyed {
			reason := fmt.Sprintf("remote host is %s", host.OperatorState)
			p.log.Warn("remote session proxy unavailable", "hostId", host.HostID, "reason", reason)
			envelope.WriteAPIError(w, r, http.StatusServiceUnavailable, "unavailable", "REMOTE_HOST_UNAVAILABLE",
				fmt.Sprintf("Remote host %q is unavailable: %s", host.HostID, reason), map[string]any{"hostId": host.HostID, "reason": reason})
			return
		}
		if isRemoteSessionStream(r.URL.Path) {
			p.forwardStream(w, r, host, qualified)
			return
		}
		p.forward(w, r, host, qualified)
	})
}

func (p *sessionProxy) forwardNotification(w http.ResponseWriter, r *http.Request, host domain.RemoteHost, qualified domain.QualifiedNotificationID) {
	proxyCtx, cancel := context.WithTimeout(r.Context(), remoteSessionProxyTimeout)
	defer cancel()
	target, err := remoteNotificationURL(r.URL, host.Address, qualified.NotificationID)
	if err != nil {
		p.writeRemoteUnavailable(w, r, host, "build remote notification proxy target", err)
		return
	}
	req, err := remoteProxyRequest(proxyCtx, r, target)
	if err != nil {
		p.writeRemoteUnavailable(w, r, host, "build remote notification proxy request", err)
		return
	}
	resp, err := p.client.Do(req) // #nosec G704 -- target is a registered remote-host endpoint.
	if err != nil {
		p.writeRemoteRequestError(w, r, host, "remote notification proxy failed", err)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if isRedirect(resp.StatusCode) {
		p.writeRemoteRedirect(w, r, host)
		return
	}
	if err := p.writeQualifiedNotificationResponse(w, resp, qualified); err != nil {
		p.log.Warn("qualify remote notification response failed", "hostId", host.HostID, "reason", err.Error())
		envelope.WriteAPIError(w, r, http.StatusBadGateway, "bad_gateway", "REMOTE_HOST_INVALID_RESPONSE",
			fmt.Sprintf("Remote host %q returned an unqualifiable notification response: %v", host.HostID, err), map[string]any{"hostId": host.HostID, "reason": err.Error()})
	}
}

// MarkAllRead forwards each group of qualified notification IDs to its owner.
// It deliberately shares the session proxy's host resolution, allowlisted HTTP
// client, and redirect refusal rather than creating a second remote path.
func (p *sessionProxy) MarkAllRead(ctx context.Context, ids []string) (int64, error) {
	if p == nil || p.federation == nil || !p.federation.HasRegisteredHosts() {
		return 0, nil
	}
	groups := map[domain.RemoteHostID][]string{}
	for _, id := range ids {
		qualified, ok := domain.ParseQualifiedNotificationID(id)
		if !ok {
			continue
		}
		groups[qualified.HostID] = append(groups[qualified.HostID], qualified.NotificationID)
	}
	if len(ids) == 0 {
		hosts, err := p.federation.RemoteHosts(ctx)
		if err != nil {
			return 0, fmt.Errorf("list remote hosts for notification acknowledgement: %w", err)
		}
		for _, host := range hosts {
			groups[host.HostID] = nil
		}
	}
	var updated int64
	for hostID, notificationIDs := range groups {
		host, found, err := p.federation.Resolve(ctx, hostID)
		if err != nil {
			return 0, fmt.Errorf("resolve remote notification host %q: %w", hostID, err)
		}
		if !found {
			return 0, fmt.Errorf("remote notification host %q is not registered", hostID)
		}
		if host.OperatorState == domain.RemoteHostStateStopped || host.OperatorState == domain.RemoteHostStateDestroyed {
			return 0, fmt.Errorf("remote notification host %q is %s", hostID, host.OperatorState)
		}
		count, err := p.markRemoteHostNotificationsRead(ctx, host, notificationIDs)
		if err != nil {
			return 0, err
		}
		updated += count
	}
	return updated, nil
}

func (p *sessionProxy) markRemoteHostNotificationsRead(ctx context.Context, host domain.RemoteHost, ids []string) (int64, error) {
	body, err := json.Marshal(map[string][]string{"ids": ids})
	if err != nil {
		return 0, fmt.Errorf("encode remote notification acknowledgement: %w", err)
	}
	proxyCtx, cancel := context.WithTimeout(ctx, remoteSessionProxyTimeout)
	defer cancel()
	target := &url.URL{Scheme: "http", Host: host.Address, Path: "/api/v1/notifications/read-all"}
	source, err := http.NewRequestWithContext(proxyCtx, http.MethodPost, target.String(), bytes.NewReader(body)) // #nosec G704 -- target is a registered remote-host endpoint.
	if err != nil {
		return 0, fmt.Errorf("build remote notification acknowledgement: %w", err)
	}
	source.Header.Set("Content-Type", "application/json")
	req, err := remoteProxyRequest(proxyCtx, source, target)
	if err != nil {
		return 0, fmt.Errorf("build remote notification acknowledgement proxy request: %w", err)
	}
	resp, err := p.client.Do(req) // #nosec G704 -- target is a registered remote-host endpoint.
	if err != nil {
		if errors.Is(err, errRemoteRedirect) {
			p.log.Warn("remote notification acknowledgement redirect refused", "hostId", host.HostID, "address", host.Address, "reason", errRemoteRedirect.Error())
		}
		return 0, fmt.Errorf("request remote notification acknowledgement for %q: %w", host.HostID, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if isRedirect(resp.StatusCode) {
		p.log.Warn("remote notification acknowledgement redirect refused", "hostId", host.HostID, "address", host.Address, "reason", errRemoteRedirect.Error())
		return 0, errRemoteRedirect
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return 0, fmt.Errorf("remote notification acknowledgement for %q returned HTTP %d", host.HostID, resp.StatusCode)
	}
	var response struct {
		UpdatedCount int64 `json:"updatedCount"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxRemoteJSONResponseSize)).Decode(&response); err != nil {
		return 0, fmt.Errorf("decode remote notification acknowledgement for %q: %w", host.HostID, err)
	}
	return response.UpdatedCount, nil
}

// forwardStream keeps the owning daemon's workspace watcher open for the
// lifetime of the client request. The ordinary session proxy has a bounded
// request timeout; applying that timeout here would turn a healthy SSE stream
// into a periodic disconnect.
func (p *sessionProxy) forwardStream(w http.ResponseWriter, r *http.Request, host domain.RemoteHost, qualified domain.QualifiedSessionID) {
	target, err := remoteSessionURL(r.URL, host.Address, qualified.SessionID)
	if err != nil {
		p.writeRemoteUnavailable(w, r, host, "build remote session stream target", err)
		return
	}
	req, err := remoteProxyRequest(r.Context(), r, target)
	if err != nil {
		p.writeRemoteUnavailable(w, r, host, "build remote session stream request", err)
		return
	}
	resp, err := p.streamClient.Do(req) // #nosec G704 -- target is a registered remote-host endpoint.
	if err != nil {
		p.writeRemoteRequestError(w, r, host, "remote session stream failed", err)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if isRedirect(resp.StatusCode) {
		p.writeRemoteRedirect(w, r, host)
		return
	}
	copyRemoteResponseHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	if _, err := io.Copy(w, resp.Body); err != nil && !errors.Is(err, context.Canceled) {
		p.log.Warn("copy remote session stream response failed", "hostId", host.HostID, "err", err)
	}
}

func (p *sessionProxy) forward(w http.ResponseWriter, r *http.Request, host domain.RemoteHost, qualified domain.QualifiedSessionID) {
	proxyCtx, cancel := context.WithTimeout(r.Context(), remoteSessionProxyTimeout)
	defer cancel()
	target, err := remoteSessionURL(r.URL, host.Address, qualified.SessionID)
	if err != nil {
		p.writeRemoteUnavailable(w, r, host, "build remote session proxy target", err)
		return
	}
	req, err := remoteProxyRequest(proxyCtx, r, target)
	if err != nil {
		p.writeRemoteUnavailable(w, r, host, "build remote session proxy request", err)
		return
	}
	resp, err := p.client.Do(req) // #nosec G704 -- target is a registered remote-host endpoint.
	if err != nil {
		p.writeRemoteRequestError(w, r, host, "remote session proxy failed", err)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if isRedirect(resp.StatusCode) {
		p.writeRemoteRedirect(w, r, host)
		return
	}
	if err := p.writeQualifiedResponse(w, r, resp, qualified); err != nil {
		p.log.Warn("qualify remote session response failed", "hostId", host.HostID, "reason", err.Error())
		envelope.WriteAPIError(w, r, http.StatusBadGateway, "bad_gateway", "REMOTE_HOST_INVALID_RESPONSE",
			fmt.Sprintf("Remote host %q returned an unqualifiable session response: %v", host.HostID, err), map[string]any{"hostId": host.HostID, "reason": err.Error()})
	}
}

func remoteProxyRequest(ctx context.Context, source *http.Request, target *url.URL) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, source.Method, target.String(), source.Body) // #nosec G704 -- target is a registered remote-host endpoint.
	if err != nil {
		return nil, err
	}
	req.Header = allowedRemoteRequestHeaders(source.Header)
	req.ContentLength = source.ContentLength
	req.TransferEncoding = append([]string(nil), source.TransferEncoding...)
	return req, nil
}

func (p *sessionProxy) writeQualifiedResponse(w http.ResponseWriter, r *http.Request, resp *http.Response, qualified domain.QualifiedSessionID) error {
	if !isJSONResponse(resp.Header) || resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusNotModified {
		copyRemoteResponseHeaders(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)
		if _, err := io.Copy(w, resp.Body); err != nil {
			p.log.Warn("copy remote session proxy response failed", "hostId", qualified.HostID, "err", err)
		}
		return nil
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxRemoteJSONResponseSize+1))
	if err != nil {
		return fmt.Errorf("read JSON response: %w", err)
	}
	if len(body) > maxRemoteJSONResponseSize {
		return fmt.Errorf("JSON response exceeds %d bytes", maxRemoteJSONResponseSize)
	}
	var payload any
	if err := json.Unmarshal(body, &payload); err != nil {
		return fmt.Errorf("decode JSON response: %w", err)
	}
	if err := qualifyRemoteResponsePayload(payload, qualified.HostID, qualified.SessionID); err != nil {
		return err
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode qualified JSON response: %w", err)
	}
	copyRemoteResponseHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	if _, err := w.Write(encoded); err != nil {
		p.log.Warn("write qualified remote session response failed", "hostId", qualified.HostID, "err", err)
	}
	return nil
}

func (p *sessionProxy) writeQualifiedNotificationResponse(w http.ResponseWriter, resp *http.Response, qualified domain.QualifiedNotificationID) error {
	if !isJSONResponse(resp.Header) || resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusNotModified {
		copyRemoteResponseHeaders(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)
		_, err := io.Copy(w, resp.Body)
		return err
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxRemoteJSONResponseSize+1))
	if err != nil {
		return fmt.Errorf("read JSON response: %w", err)
	}
	if len(body) > maxRemoteJSONResponseSize {
		return fmt.Errorf("JSON response exceeds %d bytes", maxRemoteJSONResponseSize)
	}
	var payload any
	if err := json.Unmarshal(body, &payload); err != nil {
		return fmt.Errorf("decode JSON response: %w", err)
	}
	if err := qualifyRemoteNotificationResponse(payload, qualified.HostID, qualified.NotificationID); err != nil {
		return err
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode qualified JSON response: %w", err)
	}
	copyRemoteResponseHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, err = w.Write(encoded)
	return err
}

func (p *sessionProxy) writeRemoteUnavailable(w http.ResponseWriter, r *http.Request, host domain.RemoteHost, operation string, err error) {
	p.log.Error(operation, "hostId", host.HostID, "err", err)
	envelope.WriteAPIError(w, r, http.StatusBadGateway, "bad_gateway", "REMOTE_HOST_UNREACHABLE",
		fmt.Sprintf("Remote host %q is unavailable: %v", host.HostID, err), map[string]any{"hostId": host.HostID, "reason": err.Error()})
}

func (p *sessionProxy) writeRemoteRequestError(w http.ResponseWriter, r *http.Request, host domain.RemoteHost, operation string, err error) {
	if errors.Is(err, errRemoteRedirect) {
		p.writeRemoteRedirect(w, r, host)
		return
	}
	p.log.Warn(operation, "hostId", host.HostID, "address", host.Address, "err", err)
	envelope.WriteAPIError(w, r, http.StatusBadGateway, "bad_gateway", "REMOTE_HOST_UNREACHABLE",
		fmt.Sprintf("Remote host %q is unavailable: %v", host.HostID, err), map[string]any{"hostId": host.HostID, "reason": err.Error()})
}

func (p *sessionProxy) writeRemoteRedirect(w http.ResponseWriter, r *http.Request, host domain.RemoteHost) {
	p.log.Warn("remote session proxy redirect refused", "hostId", host.HostID, "reason", errRemoteRedirect.Error())
	envelope.WriteAPIError(w, r, http.StatusBadGateway, "bad_gateway", "REMOTE_HOST_REDIRECT_REFUSED",
		fmt.Sprintf("Remote host %q returned a redirect, which is not allowed", host.HostID), map[string]any{"hostId": host.HostID, "reason": errRemoteRedirect.Error()})
}

func qualifiedSessionFromPath(requestPath string) (domain.QualifiedSessionID, bool) {
	_, id, ok := remoteSessionRoute(requestPath)
	if !ok {
		return domain.QualifiedSessionID{}, false
	}
	return domain.ParseQualifiedSessionID(domain.SessionID(id))
}

func qualifiedNotificationFromPath(requestPath string) (domain.QualifiedNotificationID, bool) {
	prefix := "/api/v1/notifications/"
	if !strings.HasPrefix(requestPath, prefix) {
		return domain.QualifiedNotificationID{}, false
	}
	id, suffix, _ := strings.Cut(strings.TrimPrefix(requestPath, prefix), "/")
	if id == "" || suffix != "" {
		return domain.QualifiedNotificationID{}, false
	}
	return domain.ParseQualifiedNotificationID(id)
}

func isRemoteSessionStream(requestPath string) bool {
	return strings.HasSuffix(strings.TrimSuffix(requestPath, "/"), "/workspace/events")
}

func remoteSessionURL(in *url.URL, address string, sessionID domain.SessionID) (*url.URL, error) {
	prefix, _, ok := remoteSessionRoute(in.Path)
	if !ok {
		return nil, fmt.Errorf("request is not a session-bearing route")
	}
	remainder := strings.TrimPrefix(in.Path, prefix)
	_, suffix, found := strings.Cut(remainder, "/")
	path := prefix + url.PathEscape(string(sessionID))
	if found {
		path += "/" + suffix
	}
	return &url.URL{Scheme: "http", Host: address, Path: path, RawQuery: in.RawQuery}, nil
}

func remoteNotificationURL(in *url.URL, address string, notificationID string) (*url.URL, error) {
	prefix := "/api/v1/notifications/"
	if !strings.HasPrefix(in.Path, prefix) || strings.Contains(strings.TrimPrefix(in.Path, prefix), "/") {
		return nil, fmt.Errorf("request is not a notification-bearing route")
	}
	return &url.URL{Scheme: "http", Host: address, Path: prefix + url.PathEscape(notificationID), RawQuery: in.RawQuery}, nil
}

func remoteSessionRoute(requestPath string) (string, string, bool) {
	for _, prefix := range []string{
		"/api/v1/sessions/",
		"/api/v1/usage/sessions/",
		"/api/v1/orchestrators/",
	} {
		if !strings.HasPrefix(requestPath, prefix) {
			continue
		}
		remainder := strings.TrimPrefix(requestPath, prefix)
		id, _, _ := strings.Cut(remainder, "/")
		if id != "" {
			return prefix, id, true
		}
	}
	return "", "", false
}

func allowedRemoteRequestHeaders(source http.Header) http.Header {
	return copiedAllowedHeaders(source, remoteRequestHeaderAllowlist)
}

func copyRemoteResponseHeaders(destination, source http.Header) {
	for key, values := range copiedAllowedHeaders(source, remoteResponseHeaderAllowlist) {
		destination[key] = values
	}
}

func copiedAllowedHeaders(source http.Header, allowlist []string) http.Header {
	destination := make(http.Header, len(allowlist))
	for _, key := range allowlist {
		if values := source.Values(key); len(values) > 0 {
			destination[key] = append([]string(nil), values...)
		}
	}
	return destination
}

func isJSONResponse(headers http.Header) bool {
	contentType := strings.ToLower(headers.Get("Content-Type"))
	mediaType, _, _ := strings.Cut(contentType, ";")
	return strings.TrimSpace(mediaType) == "application/json" || strings.HasSuffix(strings.TrimSpace(mediaType), "+json")
}

func isRedirect(status int) bool {
	return status >= http.StatusMultipleChoices && status < http.StatusBadRequest
}

func qualifyRemoteResponsePayload(value any, hostID domain.RemoteHostID, ownerSessionID domain.SessionID) error {
	return qualifyRemoteResponseValue(value, hostID, ownerSessionID, false)
}

func qualifyRemoteResponseValue(value any, hostID domain.RemoteHostID, ownerSessionID domain.SessionID, sessionObject bool) error {
	switch item := value.(type) {
	case []any:
		for _, nested := range item {
			if err := qualifyRemoteResponseValue(nested, hostID, ownerSessionID, sessionObject); err != nil {
				return err
			}
		}
	case map[string]any:
		isSession := sessionObject || isRemoteSessionObject(item, ownerSessionID)
		for key, nested := range item {
			if key == "previewUrl" {
				delete(item, key)
				continue
			}
			if isRemoteSessionReferenceKey(key, isSession) {
				qualified, err := qualifyRemoteSessionReference(hostID, key, nested)
				if err != nil {
					return err
				}
				item[key] = qualified
				continue
			}
			if key == "session" {
				if raw, ok := nested.(string); ok {
					qualified, err := qualifyRemoteSessionReference(hostID, key, raw)
					if err != nil {
						return err
					}
					item[key] = qualified
					continue
				}
			}
			if key == "takenOverFrom" {
				qualified, err := qualifyRemoteSessionReferenceList(hostID, key, nested)
				if err != nil {
					return err
				}
				item[key] = qualified
				continue
			}
			if err := qualifyRemoteResponseValue(nested, hostID, ownerSessionID, key == "session"); err != nil {
				return err
			}
		}
	}
	return nil
}

func isRemoteSessionObject(value map[string]any, ownerSessionID domain.SessionID) bool {
	id, ok := value["id"].(string)
	if !ok || id == "" {
		return false
	}
	if id == string(ownerSessionID) {
		return true
	}
	_, hasProjectID := value["projectId"]
	_, hasKind := value["kind"]
	return hasProjectID && hasKind
}

func isRemoteSessionReferenceKey(key string, sessionObject bool) bool {
	if key == "id" {
		return sessionObject
	}
	switch key {
	case "sessionId", "fromSession", "toSession", "currentSessionId", "handledBySessionId", "workerId", "orchestratorId", "terminalHandleId":
		return true
	default:
		return false
	}
}

func qualifyRemoteSessionReference(hostID domain.RemoteHostID, key string, value any) (string, error) {
	raw, ok := value.(string)
	if !ok || raw == "" {
		return "", fmt.Errorf("%s is not a non-empty session reference", key)
	}
	qualified, err := domain.QualifyRemoteSessionID(hostID, domain.SessionID(raw))
	if err != nil {
		return "", fmt.Errorf("qualify %s: %w", key, err)
	}
	return string(qualified), nil
}

func qualifyRemoteSessionReferenceList(hostID domain.RemoteHostID, key string, value any) ([]any, error) {
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("%s is not a session reference list", key)
	}
	qualified := make([]any, 0, len(items))
	for _, item := range items {
		value, err := qualifyRemoteSessionReference(hostID, key, item)
		if err != nil {
			return nil, err
		}
		qualified = append(qualified, value)
	}
	return qualified, nil
}

func qualifyRemoteNotificationResponse(value any, hostID domain.RemoteHostID, ownerNotificationID string) error {
	root, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("notification response is not an object")
	}
	if notification, ok := root["notification"]; ok {
		return qualifyRemoteNotificationObject(notification, hostID, ownerNotificationID)
	}
	if notifications, ok := root["notifications"].([]any); ok {
		for _, notification := range notifications {
			if err := qualifyRemoteNotificationObject(notification, hostID, ownerNotificationID); err != nil {
				return err
			}
		}
	}
	return nil
}

func qualifyRemoteNotificationObject(value any, hostID domain.RemoteHostID, ownerNotificationID string) error {
	notification, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("notification is not an object")
	}
	id, ok := notification["id"].(string)
	if !ok || id == "" || id != ownerNotificationID {
		return fmt.Errorf("notification id is invalid")
	}
	notification["id"] = domain.QualifyNotificationID(hostID, id)
	for _, key := range []string{"sessionId"} {
		if raw, ok := notification[key].(string); ok && raw != "" {
			qualified, err := domain.QualifyRemoteSessionID(hostID, domain.SessionID(raw))
			if err != nil {
				return err
			}
			notification[key] = string(qualified)
		}
	}
	if target, ok := notification["target"].(map[string]any); ok {
		if raw, ok := target["sessionId"].(string); ok && raw != "" {
			qualified, err := domain.QualifyRemoteSessionID(hostID, domain.SessionID(raw))
			if err != nil {
				return err
			}
			target["sessionId"] = string(qualified)
		}
	}
	return nil
}
