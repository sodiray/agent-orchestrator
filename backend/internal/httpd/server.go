package httpd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/config"
	"github.com/aoagents/agent-orchestrator/backend/internal/runfile"
	"github.com/aoagents/agent-orchestrator/backend/internal/terminal"
)

// Server is the daemon's HTTP server together with its lifecycle: bind the
// configured loopback port or Unix socket, publish the running.json handshake, serve until the context
// is cancelled, then shut down gracefully and clean up the handshake file.
type Server struct {
	cfg    config.Config
	log    *slog.Logger
	http   *http.Server
	listen net.Listener
	// socketFile records the Unix socket node created by this server. It lets
	// shutdown avoid unlinking a socket a successor created after a restart.
	socketFile os.FileInfo

	shutdownRequested chan struct{}
	shutdownOnce      sync.Once
}

// NewWithDeps constructs a Server with API dependencies supplied by the daemon
// and binds the listener immediately, before any running.json is written. The
// caller owns the returned Server's lifecycle via Run. termMgr may be nil, in
// which case the /mux terminal surface is not mounted.
//
// If the configured loopback port is already held, it falls back to an OS-assigned
// ephemeral port rather than failing. A genuine peer AO daemon is ruled out
// upstream (the running.json + /healthz check in daemon.Run), so a conflict here
// means a non-AO process owns the port; exiting would only leave the desktop
// supervisor stuck on "daemon not ready". The actual bound port is logged
// ("daemon listening") and written to running.json, both of which the supervisor
// reads, so the fallback propagates to the renderer with no UI changes.
func NewWithDeps(cfg config.Config, log *slog.Logger, termMgr *terminal.Manager, deps APIDeps) (*Server, error) {
	log = loggerOrDefault(log)
	ln, socketFile, err := listen(cfg, log)
	if err != nil {
		return nil, err
	}

	srv := &Server{
		cfg:               cfg,
		log:               log,
		listen:            ln,
		socketFile:        socketFile,
		shutdownRequested: make(chan struct{}),
	}
	srv.http = &http.Server{
		Handler: NewRouterWithControl(cfg, log, termMgr, deps, ControlDeps{
			RequestShutdown: srv.requestShutdown,
		}),
		// ReadHeaderTimeout guards against slow-loris even on loopback;
		// per-request body/handler timeouts are applied per-surface.
		ReadHeaderTimeout: 10 * time.Second,
	}
	return srv, nil
}

func listen(cfg config.Config, log *slog.Logger) (net.Listener, os.FileInfo, error) {
	if cfg.UsesUnixSocket() {
		ln, socketFile, err := listenUnix(cfg.UnixSocketPath)
		if err != nil {
			return nil, nil, err
		}
		return ln, socketFile, nil
	}
	if !isLoopbackTCPHost(cfg.Host) {
		return nil, nil, fmt.Errorf("refusing non-loopback TCP host %q", cfg.Host)
	}
	ln, err := net.Listen("tcp", cfg.Addr())
	if err == nil {
		return ln, nil, nil
	}
	if !isAddrInUse(err) {
		return nil, nil, fmt.Errorf("bind %s: %w", cfg.Addr(), err)
	}
	// Configured port is taken by a non-AO process: retry on an ephemeral port.
	fallback, ferr := net.Listen("tcp", net.JoinHostPort(cfg.Host, "0"))
	if ferr != nil {
		return nil, nil, fmt.Errorf("bind %s (in use) and ephemeral fallback: %w", cfg.Addr(), ferr)
	}
	log.Warn("configured port in use; bound an ephemeral port instead",
		"configured", cfg.Addr(), "bound", fallback.Addr().String())
	return fallback, nil, nil
}

func isLoopbackTCPHost(host string) bool {
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func listenUnix(path string) (net.Listener, os.FileInfo, error) {
	if path == "" {
		return nil, nil, fmt.Errorf("bind unix socket: path is required")
	}
	if !filepath.IsAbs(path) {
		return nil, nil, fmt.Errorf("bind unix socket %q: path must be absolute", path)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, nil, fmt.Errorf("create unix socket directory: %w", err)
	}
	if err := removeStaleUnixSocket(path); err != nil {
		return nil, nil, err
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, nil, fmt.Errorf("bind unix socket %s: %w", path, err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = ln.Close()
		_ = os.Remove(path)
		return nil, nil, fmt.Errorf("restrict unix socket permissions: %w", err)
	}
	file, err := os.Lstat(path)
	if err != nil {
		_ = ln.Close()
		_ = os.Remove(path)
		return nil, nil, fmt.Errorf("inspect unix socket: %w", err)
	}
	return ln, file, nil
}

func removeStaleUnixSocket(path string) error {
	file, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect unix socket %s: %w", path, err)
	}
	if file.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("refusing to replace %s: existing path is not a unix socket", path)
	}
	conn, err := net.DialTimeout("unix", path, 200*time.Millisecond)
	if err == nil {
		_ = conn.Close()
		return fmt.Errorf("refusing to replace unix socket %s: a listener is accepting connections", path)
	}
	if !errors.Is(err, syscall.ECONNREFUSED) {
		return fmt.Errorf("refusing to replace unix socket %s: cannot prove it is stale: %w", path, err)
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove stale unix socket %s: %w", path, err)
	}
	return nil
}

// Addr returns the actual bound address (useful when the configured port was 0
// and the OS chose one — primarily in tests).
func (s *Server) Addr() net.Addr { return s.listen.Addr() }

// Handler returns the loopback server's built router so the daemon can share
// the exact same handler instance with the LAN listener (via NewMobileLAN),
// keeping the loopback and LAN surfaces identical.
func (s *Server) Handler() http.Handler { return s.http.Handler }

// Run serves until ctx is cancelled (SIGINT/SIGTERM via signal.NotifyContext),
// then performs a graceful shutdown bounded by cfg.ShutdownTimeout. It writes
// running.json before serving and removes it on the way out. Run blocks until
// shutdown is complete.
func (s *Server) Run(ctx context.Context) error {
	info := runfile.Info{
		PID:                   os.Getpid(),
		Port:                  s.boundPort(),
		SocketPath:            s.boundSocketPath(),
		StartedAt:             time.Now().UTC(),
		Owner:                 os.Getenv("AO_OWNER"),
		BrowserRuntimeToken:   os.Getenv("AO_BROWSER_RUNTIME_TOKEN"),
		BrowserRuntimeAddress: os.Getenv("AO_BROWSER_RUNTIME_ADDRESS"),
	}
	if err := runfile.Write(s.cfg.RunFilePath, info); err != nil {
		_ = s.listen.Close()
		_ = s.removeSocketIfOwned()
		return fmt.Errorf("write run-file: %w", err)
	}
	defer func() {
		if err := s.removeSocketIfOwned(); err != nil {
			s.log.Warn("failed to remove unix socket", "path", s.boundSocketPath(), "err", err)
		}
		if err := runfile.RemoveIfOwned(s.cfg.RunFilePath, info.PID); err != nil {
			s.log.Warn("failed to remove run-file", "path", s.cfg.RunFilePath, "err", err)
		}
	}()

	serveErr := make(chan error, 1)
	go func() {
		s.log.Info("daemon listening", "addr", s.Addr().String(), "pid", info.PID)
		// Serve returns ErrServerClosed on a clean Shutdown; that is success.
		if err := s.http.Serve(s.listen); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	select {
	case err := <-serveErr:
		// Serve died on its own (bind already happened, so this is a real
		// runtime failure) before any shutdown signal.
		return err
	case <-s.shutdownRequested:
		s.log.Info("shutdown requested over HTTP", "timeout", s.cfg.ShutdownTimeout)
	case <-ctx.Done():
		s.log.Info("shutdown signal received, draining connections", "timeout", s.cfg.ShutdownTimeout)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
	defer cancel()

	if err := s.http.Shutdown(shutdownCtx); err != nil {
		// The deadline elapsed with connections still open; force them closed.
		s.log.Warn("graceful shutdown timed out, forcing close", "err", err)
		_ = s.http.Close()
		return fmt.Errorf("graceful shutdown exceeded %s: %w", s.cfg.ShutdownTimeout, err)
	}

	s.log.Info("daemon stopped cleanly")
	return <-serveErr
}

func (s *Server) boundPort() int {
	if tcp, ok := s.listen.Addr().(*net.TCPAddr); ok {
		return tcp.Port
	}
	return 0
}

func (s *Server) boundSocketPath() string {
	if !s.cfg.UsesUnixSocket() {
		return ""
	}
	return s.cfg.UnixSocketPath
}

func (s *Server) removeSocketIfOwned() error {
	if s.socketFile == nil || s.boundSocketPath() == "" {
		return nil
	}
	current, err := os.Lstat(s.boundSocketPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !os.SameFile(s.socketFile, current) {
		return nil
	}
	return os.Remove(s.boundSocketPath())
}

func (s *Server) requestShutdown() {
	s.shutdownOnce.Do(func() {
		close(s.shutdownRequested)
	})
}

// RequestShutdown triggers the same clean shutdown as POST /shutdown: it makes
// Run return so the daemon exits without tearing down sessions. Idempotent.
func (s *Server) RequestShutdown() { s.requestShutdown() }
