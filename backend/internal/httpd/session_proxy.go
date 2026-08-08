package httpd

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/envelope"
	federationsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/federation"
)

const remoteSessionProxyTimeout = 10 * time.Second

type sessionProxy struct {
	federation *federationsvc.Service
	client     *http.Client
	log        *slog.Logger
}

func newSessionProxy(federation *federationsvc.Service) *sessionProxy {
	return &sessionProxy{
		federation: federation,
		client:     &http.Client{Timeout: remoteSessionProxyTimeout},
		log:        slog.Default(),
	}
}

// Middleware forwards only qualified session paths. Bare local IDs take the
// existing handler path without a host-registry read.
func (p *sessionProxy) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		qualified, ok := qualifiedSessionFromPath(r.URL.Path)
		if !ok {
			next.ServeHTTP(w, r)
			return
		}
		if isRemoteSessionStream(r.URL.Path) {
			envelope.WriteAPIError(w, r, http.StatusNotImplemented, "not_implemented", "REMOTE_SESSION_STREAM_UNSUPPORTED",
				"Remote session streaming is not available yet", map[string]any{"hostId": qualified.HostID})
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
		p.forward(w, r, host, qualified)
	})
}

func (p *sessionProxy) forward(w http.ResponseWriter, r *http.Request, host domain.RemoteHost, qualified domain.QualifiedSessionID) {
	proxyCtx, cancel := context.WithTimeout(r.Context(), remoteSessionProxyTimeout)
	defer cancel()
	target, err := remoteSessionURL(r.URL, host.Address, qualified.SessionID)
	if err != nil {
		p.log.Error("build remote session proxy target failed", "hostId", host.HostID, "err", err)
		envelope.WriteAPIError(w, r, http.StatusBadGateway, "bad_gateway", "REMOTE_HOST_UNREACHABLE",
			fmt.Sprintf("Remote host %q is unavailable: %v", host.HostID, err), map[string]any{"hostId": host.HostID, "reason": err.Error()})
		return
	}
	req, err := http.NewRequestWithContext(proxyCtx, r.Method, target.String(), r.Body) // #nosec G704 -- target is a registered remote-host endpoint.
	if err != nil {
		p.log.Error("build remote session proxy request failed", "hostId", host.HostID, "err", err)
		envelope.WriteAPIError(w, r, http.StatusBadGateway, "bad_gateway", "REMOTE_HOST_UNREACHABLE",
			fmt.Sprintf("Remote host %q is unavailable: %v", host.HostID, err), map[string]any{"hostId": host.HostID, "reason": err.Error()})
		return
	}
	req.Header = r.Header.Clone()
	removeHopHeaders(req.Header)
	req.ContentLength = r.ContentLength
	req.TransferEncoding = append([]string(nil), r.TransferEncoding...)
	resp, err := p.client.Do(req) // #nosec G704 -- target is a registered remote-host endpoint.
	if err != nil {
		p.log.Warn("remote session proxy failed", "hostId", host.HostID, "address", host.Address, "err", err)
		envelope.WriteAPIError(w, r, http.StatusBadGateway, "bad_gateway", "REMOTE_HOST_UNREACHABLE",
			fmt.Sprintf("Remote host %q is unavailable: %v", host.HostID, err), map[string]any{"hostId": host.HostID, "reason": err.Error()})
		return
	}
	defer func() { _ = resp.Body.Close() }()
	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	if _, err := io.Copy(w, resp.Body); err != nil {
		p.log.Warn("copy remote session proxy response failed", "hostId", host.HostID, "err", err)
	}
}

func qualifiedSessionFromPath(requestPath string) (domain.QualifiedSessionID, bool) {
	const prefix = "/api/v1/sessions/"
	if !strings.HasPrefix(requestPath, prefix) {
		return domain.QualifiedSessionID{}, false
	}
	remainder := strings.TrimPrefix(requestPath, prefix)
	sessionID, _, _ := strings.Cut(remainder, "/")
	return domain.ParseQualifiedSessionID(domain.SessionID(sessionID))
}

func isRemoteSessionStream(requestPath string) bool {
	return strings.HasSuffix(strings.TrimSuffix(requestPath, "/"), "/workspace/events")
}

func remoteSessionURL(in *url.URL, address string, sessionID domain.SessionID) (*url.URL, error) {
	const prefix = "/api/v1/sessions/"
	if !strings.HasPrefix(in.Path, prefix) {
		return nil, fmt.Errorf("request is not a session route")
	}
	remainder := strings.TrimPrefix(in.Path, prefix)
	_, suffix, found := strings.Cut(remainder, "/")
	path := prefix + url.PathEscape(string(sessionID))
	if found {
		path += "/" + suffix
	}
	return &url.URL{Scheme: "http", Host: address, Path: path, RawQuery: in.RawQuery}, nil
}

func copyHeaders(destination, source http.Header) {
	removeHopHeaders(destination)
	for key, values := range source {
		if isHopHeader(key) {
			continue
		}
		destination[key] = append([]string(nil), values...)
	}
}

func removeHopHeaders(headers http.Header) {
	for key := range headers {
		if isHopHeader(key) {
			headers.Del(key)
		}
	}
}

func isHopHeader(key string) bool {
	switch strings.ToLower(key) {
	case "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade":
		return true
	default:
		return false
	}
}
