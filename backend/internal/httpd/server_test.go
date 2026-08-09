package httpd

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/aoagents/agent-orchestrator/backend/internal/config"
	"github.com/aoagents/agent-orchestrator/backend/internal/daemonendpoint"
	"github.com/aoagents/agent-orchestrator/backend/internal/runfile"
	"github.com/aoagents/agent-orchestrator/backend/internal/terminal"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestHealthProbes(t *testing.T) {
	router := newTestRouter(config.Config{}, discardLogger(), nil)
	srv := httptest.NewServer(router)
	defer srv.Close()

	client := &http.Client{Timeout: 2 * time.Second}
	for _, path := range []string{"/healthz", "/readyz"} {
		resp, err := client.Get(srv.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("GET %s = %d, want 200", path, resp.StatusCode)
		}
		if ct := resp.Header.Get("Content-Type"); ct != "application/json; charset=utf-8" {
			t.Errorf("GET %s Content-Type = %q, want JSON", path, ct)
		}
	}
}

func TestHealthProbesIncludeDaemonIdentity(t *testing.T) {
	router := newTestRouter(config.Config{StartupWorkingDirectory: "/startup"}, discardLogger(), nil)
	srv := httptest.NewServer(router)
	defer srv.Close()

	wantExe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	wantCWD, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	client := &http.Client{Timeout: 2 * time.Second}
	for _, path := range []string{"/healthz", "/readyz"} {
		resp, err := client.Get(srv.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer resp.Body.Close()
		var body struct {
			ExecutablePath          string `json:"executablePath"`
			WorkingDirectory        string `json:"workingDirectory"`
			StartupWorkingDirectory string `json:"startupWorkingDirectory"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatalf("decode %s: %v", path, err)
		}
		if body.ExecutablePath != wantExe {
			t.Errorf("GET %s executablePath = %q, want %q", path, body.ExecutablePath, wantExe)
		}
		if body.WorkingDirectory != wantCWD {
			t.Errorf("GET %s workingDirectory = %q, want %q", path, body.WorkingDirectory, wantCWD)
		}
		if body.StartupWorkingDirectory != "/startup" {
			t.Errorf("GET %s startupWorkingDirectory = %q, want /startup", path, body.StartupWorkingDirectory)
		}
	}
}

// TestServerLifecycle exercises the full Run loop: bind an ephemeral port,
// publish running.json, serve a request, then cancel the context and confirm a
// clean shutdown that removes the handshake file.
func TestServerLifecycle(t *testing.T) {
	runPath := filepath.Join(t.TempDir(), "running.json")
	cfg := config.Config{
		Host:            "127.0.0.1",
		Port:            0, // let the OS pick a free port — no conflict with a real daemon
		ShutdownTimeout: 5 * time.Second,
		RunFilePath:     runPath,
	}

	srv, err := NewWithDeps(cfg, discardLogger(), nil, APIDeps{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run(ctx) }()

	// Wait for the handshake file to confirm the server is up.
	base := "http://" + srv.Addr().String()
	waitForHealth(t, base)

	info, err := runfile.Read(runPath)
	if err != nil {
		t.Fatalf("read run-file: %v", err)
	}
	if info == nil {
		t.Fatal("run-file not written while server running")
		return
	}
	if info.Port == 0 {
		t.Error("run-file recorded port 0; want the actual bound port")
	}

	cancel()

	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run returned error on graceful shutdown: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after context cancel")
	}

	if after, _ := runfile.Read(runPath); after != nil {
		t.Error("run-file still present after shutdown; want it removed")
	}
}

func TestServerShutdownEndpoint(t *testing.T) {
	runPath := filepath.Join(t.TempDir(), "running.json")
	cfg := config.Config{
		Host:            "127.0.0.1",
		Port:            0,
		ShutdownTimeout: 5 * time.Second,
		RunFilePath:     runPath,
	}

	srv, err := NewWithDeps(cfg, discardLogger(), nil, APIDeps{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run(context.Background()) }()

	base := "http://" + srv.Addr().String()
	waitForHealth(t, base)

	resp, err := http.Post(base+"/shutdown", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /shutdown: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST /shutdown = %d, want 202", resp.StatusCode)
	}

	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run returned error on shutdown endpoint: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after shutdown endpoint")
	}

	if after, _ := runfile.Read(runPath); after != nil {
		t.Error("run-file still present after shutdown endpoint; want it removed")
	}
}

func waitForHealth(t *testing.T, base string) {
	t.Helper()
	// Per-request timeout so a stalled connect or hung handshake doesn't park
	// the test for the full Go test timeout; the outer deadline only bounds
	// the polling loop, not any single GET.
	client := &http.Client{Timeout: 500 * time.Millisecond}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(base + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("server did not become healthy within timeout")
}

// TestNewFallsBackOnPortConflict confirms that when the configured port is
// already held, the constructor binds an ephemeral port instead of failing, so
// the desktop supervisor never gets stuck on "daemon not ready".
func TestNewFallsBackOnPortConflict(t *testing.T) {
	cfg := config.Config{Host: "127.0.0.1", Port: 0, RunFilePath: filepath.Join(t.TempDir(), "r.json")}

	first, err := NewWithDeps(cfg, discardLogger(), nil, APIDeps{})
	if err != nil {
		t.Fatalf("first New: %v", err)
	}
	defer first.listen.Close()

	// Request the exact port the first server took; the second server should
	// fall back to a different, ephemeral port rather than error out.
	conflict := config.Config{Host: "127.0.0.1", Port: first.boundPort(), RunFilePath: cfg.RunFilePath}
	second, err := NewWithDeps(conflict, discardLogger(), nil, APIDeps{})
	if err != nil {
		t.Fatalf("New on an already-bound port = %v, want ephemeral fallback", err)
	}
	defer second.listen.Close()

	if second.boundPort() == first.boundPort() {
		t.Fatalf("second server bound the same port %d; want a fallback port", second.boundPort())
	}
	if second.boundPort() == 0 {
		t.Fatal("second server bound port 0; want a real fallback port")
	}
}

func TestNewRefusesNonLoopbackTCPHost(t *testing.T) {
	_, err := NewWithDeps(config.Config{Host: "0.0.0.0", Port: 0}, discardLogger(), nil, APIDeps{})
	if err == nil {
		t.Fatal("NewWithDeps accepted a non-loopback TCP host")
	}
}

func TestUnixSocketServerLifecycleAndStreams(t *testing.T) {
	requireUnixSocket(t)
	dir := filepath.Join(socketDir(t), "private")
	socketPath := filepath.Join(dir, "daemon.sock")
	runPath := filepath.Join(t.TempDir(), "running.json")
	live := &fakeEventSubscriber{}
	source := &fakeEventSource{live: live}
	mgr := terminal.NewManager(&stubSource{}, nil, discardLogger())
	defer mgr.Close()
	srv, err := NewWithDeps(config.Config{
		Listen:          config.ListenUnix,
		UnixSocketPath:  socketPath,
		ShutdownTimeout: 5 * time.Second,
		RunFilePath:     runPath,
	}, discardLogger(), mgr, APIDeps{CDC: source, Events: live})
	if err != nil {
		t.Fatalf("NewWithDeps: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run(ctx) }()

	client := daemonendpoint.Client(&http.Client{Timeout: 2 * time.Second}, socketPath)
	baseURL := daemonendpoint.BaseURL(&runfile.Info{SocketPath: socketPath})
	waitForUnixHealth(t, client, baseURL)

	info, err := runfile.Read(runPath)
	if err != nil {
		t.Fatalf("read run-file: %v", err)
	}
	if info == nil || info.SocketPath != socketPath || info.Port != 0 {
		t.Fatalf("run-file = %+v, want socket path and no port", info)
	}
	file, err := os.Lstat(socketPath)
	if err != nil {
		t.Fatalf("stat socket: %v", err)
	}
	if file.Mode().Perm() != 0o600 {
		t.Errorf("socket permissions = %o, want 600", file.Mode().Perm())
	}
	parent, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat socket parent: %v", err)
	}
	if parent.Mode().Perm()&0o077 != 0 {
		t.Errorf("socket parent permissions = %o, want owner-only", parent.Mode().Perm())
	}

	resp, err := client.Get(baseURL + "/api/v1/events")
	if err != nil {
		t.Fatalf("GET SSE: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		t.Fatalf("GET SSE = %d, want 200", resp.StatusCode)
	}
	if ids := readSSEIDs(t, resp.Body, 2); len(ids) != 2 {
		resp.Body.Close()
		t.Fatalf("SSE ids = %v, want two events", ids)
	}
	_ = resp.Body.Close()

	ws, _, err := websocket.Dial(context.Background(), "ws://localhost/mux", &websocket.DialOptions{HTTPClient: client})
	if err != nil {
		t.Fatalf("WebSocket dial over unix socket: %v", err)
	}
	if err := ws.Close(websocket.StatusNormalClosure, "test complete"); err != nil {
		t.Fatalf("close WebSocket: %v", err)
	}

	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop")
	}
	if _, err := os.Lstat(socketPath); !os.IsNotExist(err) {
		t.Errorf("socket still exists after shutdown: %v", err)
	}
}

func TestUnixSocketStaleFileIsReplaced(t *testing.T) {
	requireUnixSocket(t)
	socketPath := filepath.Join(socketDir(t), "daemon.sock")
	stale, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("create stale socket: %v", err)
	}
	if err := stale.Close(); err != nil {
		t.Fatalf("close stale socket: %v", err)
	}

	srv, err := NewWithDeps(config.Config{Listen: config.ListenUnix, UnixSocketPath: socketPath}, discardLogger(), nil, APIDeps{})
	if err != nil {
		t.Fatalf("NewWithDeps with stale socket: %v", err)
	}
	defer func() {
		_ = srv.listen.Close()
		_ = srv.removeSocketIfOwned()
	}()
	if _, err := os.Lstat(socketPath); err != nil {
		t.Fatalf("replacement socket missing: %v", err)
	}
}

func TestUnixSocketLiveListenerIsNotReplaced(t *testing.T) {
	requireUnixSocket(t)
	socketPath := filepath.Join(socketDir(t), "daemon.sock")
	live, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("create live socket: %v", err)
	}
	defer func() {
		_ = live.Close()
		_ = os.Remove(socketPath)
	}()

	_, err = NewWithDeps(config.Config{Listen: config.ListenUnix, UnixSocketPath: socketPath}, discardLogger(), nil, APIDeps{})
	if err == nil {
		t.Fatal("NewWithDeps replaced a live unix socket")
	}
	if _, statErr := os.Lstat(socketPath); statErr != nil {
		t.Fatalf("live socket was removed: %v", statErr)
	}
}

func waitForUnixHealth(t *testing.T, client *http.Client, baseURL string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(baseURL + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("unix socket server did not become healthy within timeout")
}

func requireUnixSocket(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("Unix-domain sockets are unavailable on Windows")
	}
	path := filepath.Join(socketDir(t), "probe.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		// Deliberately fatal rather than a skip. A skip here reports ok for the
		// whole package while the socket listener goes entirely unexercised,
		// which is how this suite passed on every developer machine without
		// running a single one of these tests.
		t.Fatalf("unix-domain sockets are unavailable in this environment: %v", err)
	}
	_ = ln.Close()
	_ = os.Remove(path)
}

// socketDir returns a directory short enough to hold a bindable socket path.
// The sockaddr_un path limit is 104 bytes on darwin (108 on linux), and
// t.TempDir() alone already exceeds it on macOS, so binding fails with
// "invalid argument" on a platform that supports unix sockets perfectly well.
func socketDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "ao-sock")
	if err != nil {
		t.Fatalf("create socket dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}
