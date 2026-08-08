package httpd

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	federationsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/federation"
)

// terminalMuxRelay leaves local frames to terminal.Manager and forwards only
// host-qualified terminal handles to their owning daemon. It keeps the mux
// protocol opaque: JSON is inspected solely to remove or restore the routing
// prefix on the id field, while the frame type and every other field pass
// through unchanged.
type terminalMuxRelay struct {
	client     *websocket.Conn
	federation *federationsvc.Service
	log        *slog.Logger

	writeMu sync.Mutex
	mu      sync.Mutex
	remotes map[domain.RemoteHostID]*remoteMux
	closed  bool
	cancel  context.CancelFunc
}

type remoteMux struct {
	conn   *websocket.Conn
	cancel context.CancelFunc
}

func newTerminalMuxRelay(client *websocket.Conn, federation *federationsvc.Service, log *slog.Logger) *terminalMuxRelay {
	if log == nil {
		log = slog.Default()
	}
	return &terminalMuxRelay{client: client, federation: federation, log: log, remotes: map[domain.RemoteHostID]*remoteMux{}}
}

func (r *terminalMuxRelay) ReadJSON(ctx context.Context, value any) error {
	for {
		var raw json.RawMessage
		if err := wsjson.Read(ctx, r.client, &raw); err != nil {
			return err
		}
		qualified, ok := qualifiedTerminalFrame(raw)
		if !ok {
			return json.Unmarshal(raw, value)
		}
		if err := r.forward(ctx, qualified, raw); err != nil {
			r.log.Warn("remote terminal relay failed", "hostId", qualified.HostID, "err", err)
			r.writeRemoteError(qualified, err)
		}
	}
}

func (r *terminalMuxRelay) WriteJSON(ctx context.Context, value any) error {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	return wsjson.Write(ctx, r.client, value)
}

func (r *terminalMuxRelay) Ping(ctx context.Context) error { return r.client.Ping(ctx) }

func (r *terminalMuxRelay) Close(reason string) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	remotes := make([]*remoteMux, 0, len(r.remotes))
	for _, remote := range r.remotes {
		remotes = append(remotes, remote)
	}
	r.remotes = map[domain.RemoteHostID]*remoteMux{}
	r.mu.Unlock()
	for _, remote := range remotes {
		remote.cancel()
		_ = remote.conn.Close(websocket.StatusNormalClosure, reason)
	}
	return r.client.Close(websocket.StatusNormalClosure, reason)
}

func (r *terminalMuxRelay) forward(ctx context.Context, qualified domain.QualifiedSessionID, raw json.RawMessage) error {
	remote, err := r.remote(ctx, qualified.HostID)
	if err != nil {
		return err
	}
	bare, err := replaceTerminalFrameID(raw, string(qualified.SessionID))
	if err != nil {
		return err
	}
	return remote.conn.Write(ctx, websocket.MessageText, bare)
}

func (r *terminalMuxRelay) remote(ctx context.Context, hostID domain.RemoteHostID) (*remoteMux, error) {
	r.mu.Lock()
	remote := r.remotes[hostID]
	r.mu.Unlock()
	if remote != nil {
		return remote, nil
	}
	host, found, err := r.federation.Resolve(ctx, hostID)
	if err != nil {
		return nil, fmt.Errorf("resolve remote host: %w", err)
	}
	if !found {
		return nil, fmt.Errorf("remote host %q is not registered", hostID)
	}
	if host.OperatorState == domain.RemoteHostStateStopped || host.OperatorState == domain.RemoteHostStateDestroyed {
		return nil, fmt.Errorf("remote host is %s", host.OperatorState)
	}
	endpoint := "ws://" + host.Address + "/mux"
	conn, _, err := websocket.Dial(ctx, endpoint, nil) // #nosec G704 -- target is a registered remote-host endpoint.
	if err != nil {
		return nil, fmt.Errorf("connect remote mux: %w", err)
	}
	conn.SetReadLimit(terminalMuxReadLimit)
	remoteCtx, cancel := context.WithCancel(context.Background())
	created := &remoteMux{conn: conn, cancel: cancel}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		cancel()
		_ = conn.Close(websocket.StatusNormalClosure, "local mux closed")
		return nil, context.Canceled
	}
	if existing := r.remotes[hostID]; existing != nil {
		r.mu.Unlock()
		cancel()
		_ = conn.Close(websocket.StatusNormalClosure, "duplicate remote mux")
		return existing, nil
	}
	r.remotes[hostID] = created
	r.mu.Unlock()
	go r.copyRemoteFrames(remoteCtx, hostID, created)
	return created, nil
}

func (r *terminalMuxRelay) copyRemoteFrames(ctx context.Context, hostID domain.RemoteHostID, remote *remoteMux) {
	for {
		kind, raw, err := remote.conn.Read(ctx)
		if err != nil {
			if ctx.Err() == nil {
				r.remoteDropped(hostID, err)
			}
			return
		}
		qualified, err := qualifyTerminalFrameID(raw, hostID)
		if err != nil {
			r.remoteDropped(hostID, err)
			return
		}
		r.writeMu.Lock()
		err = r.client.Write(ctx, kind, qualified)
		r.writeMu.Unlock()
		if err != nil {
			return
		}
	}
}

func (r *terminalMuxRelay) remoteDropped(hostID domain.RemoteHostID, cause error) {
	r.log.Warn("remote terminal relay dropped", "hostId", hostID, "err", cause)
	r.writeRemoteError(domain.QualifiedSessionID{HostID: hostID}, cause)
	_ = r.Close("remote terminal relay dropped")
}

func (r *terminalMuxRelay) writeRemoteError(qualified domain.QualifiedSessionID, cause error) {
	id := ""
	if qualified.SessionID != "" {
		id = string(domain.QualifySessionID(qualified.HostID, qualified.SessionID))
	}
	_ = r.WriteJSON(context.Background(), map[string]string{
		"ch":    "terminal",
		"id":    id,
		"type":  "error",
		"error": "remote terminal relay: " + cause.Error(),
	})
}

func qualifiedTerminalFrame(raw json.RawMessage) (domain.QualifiedSessionID, bool) {
	var frame struct {
		Ch string `json:"ch"`
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw, &frame); err != nil || frame.Ch != "terminal" {
		return domain.QualifiedSessionID{}, false
	}
	return domain.ParseQualifiedSessionID(domain.SessionID(frame.ID))
}

func replaceTerminalFrameID(raw json.RawMessage, id string) ([]byte, error) {
	var frame map[string]json.RawMessage
	if err := json.Unmarshal(raw, &frame); err != nil {
		return nil, fmt.Errorf("decode terminal frame: %w", err)
	}
	encoded, err := json.Marshal(id)
	if err != nil {
		return nil, fmt.Errorf("encode terminal frame id: %w", err)
	}
	frame["id"] = encoded
	return json.Marshal(frame)
}

func qualifyTerminalFrameID(raw []byte, hostID domain.RemoteHostID) ([]byte, error) {
	var frame struct {
		Ch string `json:"ch"`
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw, &frame); err != nil {
		return nil, fmt.Errorf("decode remote terminal frame: %w", err)
	}
	if frame.Ch != "terminal" || frame.ID == "" {
		return raw, nil
	}
	return replaceTerminalFrameID(raw, string(domain.QualifySessionID(hostID, domain.SessionID(frame.ID))))
}

var _ interface {
	ReadJSON(context.Context, any) error
	WriteJSON(context.Context, any) error
	Ping(context.Context) error
	Close(string) error
} = (*terminalMuxRelay)(nil)
