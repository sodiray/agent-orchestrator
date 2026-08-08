package httpd

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/aoagents/agent-orchestrator/backend/internal/adapters/runtime/ptyexec"
	"github.com/aoagents/agent-orchestrator/backend/internal/config"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	federationsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/federation"
	"github.com/aoagents/agent-orchestrator/backend/internal/terminal"
)

type muxPresence bool

func (p muxPresence) HasRegisteredHosts() bool { return bool(p) }

// stubSource attaches a throwaway shell command instead of a real mux pane, so
// the /mux path exercises the genuine upgrade + wsjson + Serve + creack/pty flow
// without needing a runtime. The pane reports alive until the first attach
// happens (the mux refuses to attach to a dead pane), then dead, so the
// command's exit is treated as the pane being gone (no re-attach).
type stubSource struct {
	argv     []string
	attached atomic.Bool
}

func (s *stubSource) Attach(ctx context.Context, _ ports.RuntimeHandle, rows, cols uint16) (ports.Stream, error) {
	s.attached.Store(true)
	return ptyexec.Spawn(ctx, s.argv, nil, rows, cols)
}

func (s *stubSource) IsAlive(context.Context, ports.RuntimeHandle) (bool, error) {
	return !s.attached.Load(), nil
}

type terminalMuxFrame struct {
	Ch   string `json:"ch"`
	ID   string `json:"id"`
	Type string `json:"type"`
	Data string `json:"data"`
}

func dialMux(t *testing.T, mgr *terminal.Manager) (*websocket.Conn, func()) {
	t.Helper()
	router := newTestRouter(config.Config{}, discardLogger(), mgr)
	ts := httptest.NewServer(router)
	url := "ws" + strings.TrimPrefix(ts.URL, "http") + "/mux"

	c, _, err := websocket.Dial(context.Background(), url, nil)
	if err != nil {
		ts.Close()
		t.Fatalf("dial /mux: %v", err)
	}
	return c, func() {
		_ = c.Close(websocket.StatusNormalClosure, "test done")
		ts.Close()
	}
}

func readFrame(t *testing.T, c *websocket.Conn, ch, typ string, d time.Duration) terminalMuxFrame {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	for {
		var f terminalMuxFrame
		if err := wsjson.Read(ctx, c, &f); err != nil {
			t.Fatalf("waiting for %s/%s: %v", ch, typ, err)
		}
		if f.Ch == ch && f.Type == typ {
			return f
		}
	}
}

func TestMuxUpgradeStreamsTerminal(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("PTY spawning not supported on Windows")
	}
	mgr := terminal.NewManager(
		&stubSource{argv: []string{"/bin/sh", "-c", "printf MUXOK; exit 0"}},
		nil, discardLogger(),
	)
	defer mgr.Close()

	c, done := dialMux(t, mgr)
	defer done()

	ctx := context.Background()
	if err := wsjson.Write(ctx, c, terminalMuxFrame{Ch: "terminal", ID: "t1", Type: "open"}); err != nil {
		t.Fatalf("write open: %v", err)
	}

	readFrame(t, c, "terminal", "opened", 3*time.Second)

	data := readFrame(t, c, "terminal", "data", 5*time.Second)
	got, _ := base64.StdEncoding.DecodeString(data.Data)
	if !strings.Contains(string(got), "MUXOK") {
		t.Fatalf("streamed data = %q, want it to contain MUXOK", got)
	}

	// The shell exits; the pane is reported gone (IsAlive=false) so we get exited.
	readFrame(t, c, "terminal", "exited", 5*time.Second)
}

func TestMuxSystemPingPong(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("PTY spawning not supported on Windows")
	}
	mgr := terminal.NewManager(&stubSource{argv: []string{"/bin/sh"}}, nil, discardLogger())
	defer mgr.Close()

	c, done := dialMux(t, mgr)
	defer done()

	ctx := context.Background()
	if err := wsjson.Write(ctx, c, map[string]string{"ch": "system", "type": "ping"}); err != nil {
		t.Fatalf("write ping: %v", err)
	}
	readFrame(t, c, "system", "pong", 3*time.Second)
}

func TestTerminalMuxRelayQualifiesOnlyTerminalFrameIdentity(t *testing.T) {
	forwarded, err := replaceTerminalFrameID(json.RawMessage(`{"ch":"terminal","type":"data","id":"workstation~pane-7","data":"AQID","cols":80}`), "pane-7")
	if err != nil {
		t.Fatal(err)
	}
	var frame terminalMuxFrame
	if err := json.Unmarshal(forwarded, &frame); err != nil {
		t.Fatal(err)
	}
	if frame.ID != "pane-7" || frame.Data != "AQID" || frame.Type != "data" {
		t.Fatalf("forwarded frame = %#v", frame)
	}
	returned, err := qualifyTerminalFrameID([]byte(`{"ch":"terminal","type":"resize","id":"pane-7","cols":132,"rows":43}`), "workstation")
	if err != nil {
		t.Fatal(err)
	}
	var response map[string]any
	if err := json.Unmarshal(returned, &response); err != nil {
		t.Fatal(err)
	}
	if response["id"] != "workstation~pane-7" || response["cols"] != float64(132) || response["rows"] != float64(43) {
		t.Fatalf("returned frame = %#v", response)
	}
}

func TestTerminalMuxRelayPassesFramesAndClosesRemoteSocket(t *testing.T) {
	remoteClosed := make(chan struct{})
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/mux" {
			t.Errorf("path = %q", r.URL.Path)
		}
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			t.Errorf("accept remote mux: %v", err)
			return
		}
		defer func() {
			_ = conn.Close(websocket.StatusNormalClosure, "test done")
			close(remoteClosed)
		}()
		var frame terminalMuxFrame
		if err := wsjson.Read(r.Context(), conn, &frame); err != nil {
			t.Errorf("read remote mux frame: %v", err)
			return
		}
		if frame.ID != "pane-7" || frame.Type != "open" {
			t.Errorf("remote frame = %#v", frame)
		}
		if err := wsjson.Write(r.Context(), conn, terminalMuxFrame{Ch: "terminal", ID: "pane-7", Type: "opened"}); err != nil {
			t.Errorf("write remote mux frame: %v", err)
			return
		}
		_, _, _ = conn.Read(r.Context())
	}))
	defer remote.Close()
	remoteURL, err := url.Parse(remote.URL)
	if err != nil {
		t.Fatal(err)
	}
	store := &proxyStore{host: domain.RemoteHost{HostID: "workstation", Address: remoteURL.Host}, found: true}
	federation := federationsvc.New(federationsvc.Deps{Store: store, Presence: muxPresence(true)})
	mgr := terminal.NewManager(nil, nil, discardLogger())
	defer mgr.Close()
	router := NewRouterWithControl(config.Config{}, discardLogger(), mgr, APIDeps{Federation: federation}, ControlDeps{})
	local := httptest.NewServer(router)
	defer local.Close()
	client, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(local.URL, "http")+"/mux", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := wsjson.Write(context.Background(), client, terminalMuxFrame{Ch: "terminal", ID: "workstation~pane-7", Type: "open"}); err != nil {
		t.Fatal(err)
	}
	got := readFrame(t, client, "terminal", "opened", 3*time.Second)
	if got.ID != "workstation~pane-7" {
		t.Fatalf("client frame id = %q", got.ID)
	}
	if err := client.Close(websocket.StatusNormalClosure, "test done"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-remoteClosed:
	case <-time.After(3 * time.Second):
		t.Fatal("remote mux socket did not close after client teardown")
	}
}
