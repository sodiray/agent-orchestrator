// Package config loads the daemon's runtime configuration. The HTTP daemon is
// a local-only sidecar: it defaults to 127.0.0.1, may explicitly use a Unix
// socket, takes no public traffic, and reads operator settings from an optional
// configuration file and environment variables with sane defaults so it can
// boot with zero configuration in development.
package config

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	// LoopbackHost is the only host the daemon ever binds. There is deliberately
	// no AO_HOST env var: the daemon has no auth/CORS/TLS and a stray
	// AO_HOST=0.0.0.0 would turn it into a public no-auth service. If a
	// non-default loopback (e.g. ::1, 127.0.0.2) is ever needed, add it back with
	// an IsLoopback() validator — not a raw env read.
	LoopbackHost = "127.0.0.1"
	// DefaultPort is the loopback port for REST, terminal mux, health, and control.
	DefaultPort = 3001
	// DefaultRequestTimeout bounds a single REST request. Long-lived terminal mux
	// connections are mounted outside this timeout.
	DefaultRequestTimeout = 60 * time.Second
	// DefaultShutdownTimeout is the hard cap on graceful shutdown. After this
	// the process exits even if connections are still draining.
	DefaultShutdownTimeout = 10 * time.Second
	// DefaultRemoteHostProbeTimeout bounds one remote host health probe and its
	// session snapshot refresh. It leaves room for a forwarded connection
	// to establish while still isolating an unreachable host from other probes.
	DefaultRemoteHostProbeTimeout = 10 * time.Second
	// DefaultHostInventoryInterval balances timely external host discovery
	// against the cost of invoking the operator-configured inventory command.
	DefaultHostInventoryInterval = 30 * time.Second
	// DefaultHostInventoryTimeout stops a stalled local inventory command from
	// holding the remote-host worker indefinitely.
	DefaultHostInventoryTimeout = 10 * time.Second
	// DefaultHostInventoryMaxOutput bounds command output before JSON decoding
	// so a misconfigured inventory source cannot exhaust daemon memory.
	DefaultHostInventoryMaxOutput = 1 << 20
	// DefaultAgent is the compatibility value used when AO_AGENT is unset. The
	// daemon validates it at startup, but worker/orchestrator spawns resolve from
	// explicit requests or project role config instead of falling back to it.
	DefaultAgent = "claude-code"
	// DefaultTelemetryPostHogHost is the default PostHog ingestion host when
	// remote telemetry is enabled and AO_TELEMETRY_POSTHOG_HOST is unset.
	DefaultTelemetryPostHogHost = "https://us.i.posthog.com"
)

// TelemetryRemote selects the remote telemetry exporter.
type TelemetryRemote string

const (
	// TelemetryRemoteOff disables remote telemetry export.
	TelemetryRemoteOff TelemetryRemote = "off"
	// TelemetryRemotePostHog exports allowlisted events to PostHog.
	TelemetryRemotePostHog TelemetryRemote = "posthog"
)

// TelemetryConfig controls local and remote telemetry behavior.
type TelemetryConfig struct {
	Events      bool
	Metrics     bool
	Remote      TelemetryRemote
	PostHogKey  string
	PostHogHost string
	// DisabledEvents names event streams that must never reach the remote
	// (billed) sink. This is the kill switch: a stream that turns out to be
	// noisy or expensive can be silenced by configuration, without waiting for
	// users to install a new build. Local storage still records everything.
	DisabledEvents []string
	// AppVersion is the desktop app version the daemon was launched by, stamped
	// on remote events so failures can be attributed to a release. The daemon
	// binary has no reliable version of its own (see cli.Version, which release
	// tooling does not currently override), so the supervisor passes it in.
	AppVersion string
}

// DefaultAllowedOrigins are the browser origins the daemon's CORS boundary
// trusts, beyond loopback-served content (which the middleware always trusts —
// local pages can reach the no-auth daemon directly anyway). The daemon has no
// auth, so every entry must be an origin web content cannot present:
// app://renderer is the packaged Electron renderer, served from a custom
// scheme only the desktop app registers — no website can bear it. The opaque
// "null" origin (file:// pages, sandboxed iframes on any website) must never
// be added.
var DefaultAllowedOrigins = []string{
	"app://renderer",
}

// Config is the fully-resolved daemon configuration. It is immutable once
// built by Load.
type Config struct {
	// Listen selects the daemon listener. It is always ListenLoopback unless
	// AO_LISTEN explicitly selects a Unix socket.
	Listen Listener
	// Host is the bind address. Always loopback — see LoopbackHost.
	Host string
	// Port is the TCP port to bind. The daemon fails fast if it is taken.
	Port int
	// UnixSocketPath is the absolute filesystem path used when Listen is
	// ListenUnix. It is empty for the default loopback TCP listener.
	UnixSocketPath string
	// RequestTimeout bounds REST request handling.
	RequestTimeout time.Duration
	// ShutdownTimeout is the hard graceful-shutdown deadline.
	ShutdownTimeout time.Duration
	// RemoteHostProbeTimeout bounds one remote host health probe and its session
	// snapshot refresh.
	RemoteHostProbeTimeout time.Duration
	// HostInventoryCommand is an explicit argv vector. An empty vector disables
	// host inventory and preserves registered-host-only behavior.
	HostInventoryCommand   []string
	HostInventoryInterval  time.Duration
	HostInventoryTimeout   time.Duration
	HostInventoryMaxOutput int64
	// RunFilePath is where the PID + listener handshake file (running.json) is
	// written so the Electron supervisor can discover and reap the daemon.
	RunFilePath string
	// DataDir is the directory holding durable SQLite state: DB and WAL files.
	// It is created on first use by the storage layer.
	DataDir string
	// Agent is the compatibility agent adapter id selected by AO_AGENT;
	// startSession fails fast if no adapter with this id is registered.
	Agent string
	// AppRunID identifies one desktop-app launch. The Electron supervisor mints
	// it and passes it down (AO_APP_RUN_ID), holding it constant across daemon
	// restarts it performs, so standalone shell terminals can survive a daemon
	// restart while still being reaped when the APP itself goes away.
	//
	// Empty means no supervising app (a bare `ao daemon`): the daemon mints a
	// fresh id per boot, which correctly makes any surviving shell terminals
	// from an earlier run look like orphans and get cleaned up.
	AppRunID string
	// AllowedOrigins are the browser origins granted CORS read access (see
	// DefaultAllowedOrigins). Overridden by AO_ALLOWED_ORIGINS.
	AllowedOrigins []string
	// Telemetry controls local/remote telemetry sinks.
	Telemetry TelemetryConfig
	// StartupWorkingDirectory is the daemon process cwd before startup
	// normalizes it. The desktop uses this to identify dev daemons after the
	// process cwd is moved to the stable data dir.
	StartupWorkingDirectory string
}

// Listener identifies the daemon listener family.
type Listener string

const (
	// ListenLoopback keeps the daemon on its fixed loopback TCP listener.
	ListenLoopback Listener = "loopback"
	// ListenUnix selects a Unix-domain socket.
	ListenUnix Listener = "unix"
)

// UsesUnixSocket reports whether the daemon listens on a Unix-domain socket.
func (c Config) UsesUnixSocket() bool { return c.Listen == ListenUnix }

// Addr returns the host:port the loopback HTTP server binds. It uses
// net.JoinHostPort so the result is correct for IPv6 literals as well as IPv4
// / hostnames.
func (c Config) Addr() string {
	return net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
}

// Load resolves configuration from defaults, the optional config.yaml under
// AO_DATA_DIR, then the environment. Environment values override file values.
// It returns an error for malformed supplied values; a missing config file is
// equivalent to the historical environment-only behavior.
//
// Recognised variables:
//
//	AO_PORT              bind port           (default 3001)
//	AO_LISTEN            listener selector   (loopback, or unix:<path>; default loopback)
//	AO_REQUEST_TIMEOUT   per-request timeout (Go duration > 0, default 60s)
//	AO_SHUTDOWN_TIMEOUT  shutdown deadline   (Go duration > 0, default 10s)
//	AO_REMOTE_HOST_PROBE_TIMEOUT remote health probe timeout (Go duration > 0, default 10s)
//	AO_HOST_INVENTORY_COMMAND JSON argv array for the optional host inventory command
//	AO_HOST_INVENTORY_INTERVAL inventory refresh interval (Go duration > 0, default 30s)
//	AO_HOST_INVENTORY_TIMEOUT inventory command timeout (Go duration > 0, default 10s)
//	AO_HOST_INVENTORY_MAX_OUTPUT maximum inventory stdout bytes (default 1048576)
//	AO_RUN_FILE          running.json path   (default ~/.ao/running.json)
//	AO_DATA_DIR          durable state dir   (default ~/.ao/data)
//	AO_AGENT             compatibility agent id (default claude-code)
//	AO_APP_RUN_ID        desktop-app launch id, set by the Electron supervisor
//	                     (default: a fresh id minted per daemon boot)
//	AO_ALLOWED_ORIGINS   CORS origins, comma-separated (default DefaultAllowedOrigins)
//	AO_TELEMETRY_EVENTS  local event capture off|on (default off)
//	AO_TELEMETRY_METRICS local metric capture off|on (default off)
//	AO_TELEMETRY_REMOTE  remote exporter off|posthog (default off)
//	AO_TELEMETRY_POSTHOG_KEY   PostHog project key
//	AO_TELEMETRY_POSTHOG_HOST  PostHog host (default DefaultTelemetryPostHogHost)
//
// The bind host is not configurable: the daemon is loopback-only by design.
// AO_LISTEN may instead select a Unix-domain socket, which has no network
// reachability and is protected by filesystem permissions.
func Load() (Config, error) {
	dataDir, err := resolveDataDir()
	if err != nil {
		return Config{}, err
	}
	file, configPath, err := loadConfigFile(dataDir)
	if err != nil {
		return Config{}, err
	}

	cfg := Config{
		Listen:                 ListenLoopback,
		Host:                   LoopbackHost,
		Port:                   DefaultPort,
		RequestTimeout:         DefaultRequestTimeout,
		ShutdownTimeout:        DefaultShutdownTimeout,
		RemoteHostProbeTimeout: DefaultRemoteHostProbeTimeout,
		HostInventoryInterval:  DefaultHostInventoryInterval,
		HostInventoryTimeout:   DefaultHostInventoryTimeout,
		HostInventoryMaxOutput: DefaultHostInventoryMaxOutput,
		Agent:                  DefaultAgent,
		AllowedOrigins:         DefaultAllowedOrigins,
		Telemetry: TelemetryConfig{
			Remote:      TelemetryRemoteOff,
			PostHogHost: DefaultTelemetryPostHogHost,
		},
		DataDir: dataDir,
	}
	if err := applyConfigFile(&cfg, file, configPath); err != nil {
		return Config{}, err
	}

	if raw := os.Getenv("AO_LISTEN"); raw != "" {
		listener, socketPath, err := parseListener(raw)
		if err != nil {
			return Config{}, err
		}
		cfg.Listen = listener
		cfg.UnixSocketPath = socketPath
	}

	if raw := os.Getenv("AO_PORT"); raw != "" && !cfg.UsesUnixSocket() {
		port, err := strconv.Atoi(raw)
		if err != nil {
			return Config{}, fmt.Errorf("invalid AO_PORT %q: %w", raw, err)
		}
		if err := applyPort(&cfg, port, "AO_PORT"); err != nil {
			return Config{}, err
		}
	}

	if raw := os.Getenv("AO_REQUEST_TIMEOUT"); raw != "" {
		d, err := parsePositiveDuration("AO_REQUEST_TIMEOUT", raw)
		if err != nil {
			return Config{}, err
		}
		cfg.RequestTimeout = d
	}

	if raw := os.Getenv("AO_SHUTDOWN_TIMEOUT"); raw != "" {
		d, err := parsePositiveDuration("AO_SHUTDOWN_TIMEOUT", raw)
		if err != nil {
			return Config{}, err
		}
		cfg.ShutdownTimeout = d
	}

	if raw := os.Getenv("AO_REMOTE_HOST_PROBE_TIMEOUT"); raw != "" {
		d, err := parsePositiveDuration("AO_REMOTE_HOST_PROBE_TIMEOUT", raw)
		if err != nil {
			return Config{}, err
		}
		cfg.RemoteHostProbeTimeout = d
	}
	if raw, ok := os.LookupEnv("AO_HOST_INVENTORY_COMMAND"); ok && raw != "" {
		command, err := parseCommandArgs("AO_HOST_INVENTORY_COMMAND", raw)
		if err != nil {
			return Config{}, err
		}
		cfg.HostInventoryCommand = command
	}
	if raw := os.Getenv("AO_HOST_INVENTORY_INTERVAL"); raw != "" {
		d, err := parsePositiveDuration("AO_HOST_INVENTORY_INTERVAL", raw)
		if err != nil {
			return Config{}, err
		}
		cfg.HostInventoryInterval = d
	}
	if raw := os.Getenv("AO_HOST_INVENTORY_TIMEOUT"); raw != "" {
		d, err := parsePositiveDuration("AO_HOST_INVENTORY_TIMEOUT", raw)
		if err != nil {
			return Config{}, err
		}
		cfg.HostInventoryTimeout = d
	}
	if raw := os.Getenv("AO_HOST_INVENTORY_MAX_OUTPUT"); raw != "" {
		limit, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || limit <= 0 {
			return Config{}, fmt.Errorf("invalid AO_HOST_INVENTORY_MAX_OUTPUT %q: must be a positive byte count", raw)
		}
		cfg.HostInventoryMaxOutput = limit
	}

	if raw := os.Getenv("AO_AGENT"); raw != "" {
		cfg.Agent = raw
	}

	// A missing AO_APP_RUN_ID means nothing is supervising this daemon, so this
	// boot IS the run: mint an id rather than leaving it empty, which would make
	// every boot share one run id and defeat orphan detection entirely.
	if raw := os.Getenv("AO_APP_RUN_ID"); raw != "" {
		cfg.AppRunID = raw
	} else {
		cfg.AppRunID = newAppRunID()
	}

	if raw, ok := os.LookupEnv("AO_ALLOWED_ORIGINS"); ok && raw != "" {
		// Explicit override replaces the defaults entirely so a deployment can
		// also narrow the list. The "null" origin is rejected, never silently
		// dropped: an operator allowing it would open the no-auth daemon to
		// every sandboxed iframe on the web.
		origins, err := validateAllowedOrigins(strings.Split(raw, ","), "AO_ALLOWED_ORIGINS")
		if err != nil {
			return Config{}, err
		}
		cfg.AllowedOrigins = origins
	}

	if raw := os.Getenv("AO_TELEMETRY_EVENTS"); raw != "" {
		v, err := parseToggleEnv("AO_TELEMETRY_EVENTS", raw)
		if err != nil {
			return Config{}, err
		}
		cfg.Telemetry.Events = v
	}
	if raw := os.Getenv("AO_TELEMETRY_METRICS"); raw != "" {
		v, err := parseToggleEnv("AO_TELEMETRY_METRICS", raw)
		if err != nil {
			return Config{}, err
		}
		cfg.Telemetry.Metrics = v
	}
	if raw := os.Getenv("AO_TELEMETRY_REMOTE"); raw != "" {
		remote, err := parseTelemetryRemote(raw)
		if err != nil {
			return Config{}, fmt.Errorf("invalid AO_TELEMETRY_REMOTE %q: %w", raw, err)
		}
		cfg.Telemetry.Remote = remote
	}
	if raw := os.Getenv("AO_TELEMETRY_POSTHOG_KEY"); raw != "" {
		cfg.Telemetry.PostHogKey = raw
	}
	if raw := os.Getenv("AO_TELEMETRY_POSTHOG_HOST"); raw != "" {
		cfg.Telemetry.PostHogHost = raw
	}
	if raw := os.Getenv("AO_TELEMETRY_DISABLED_EVENTS"); raw != "" {
		cfg.Telemetry.DisabledEvents = parseTelemetryDisabledEvents(raw)
	}
	if raw := os.Getenv("AO_TELEMETRY_APP_VERSION"); raw != "" {
		cfg.Telemetry.AppVersion = strings.TrimSpace(raw)
	}

	runFile, err := resolveRunFilePath(cfg.RunFilePath)
	if err != nil {
		return Config{}, err
	}
	cfg.RunFilePath = runFile

	return cfg, nil
}

func parseCommandArgs(name, raw string) ([]string, error) {
	var args []string
	if err := json.Unmarshal([]byte(raw), &args); err != nil {
		return nil, fmt.Errorf("invalid %s: must be a JSON array of command arguments: %w", name, err)
	}
	if err := validateCommandArgs(name, args, false); err != nil {
		return nil, err
	}
	return args, nil
}

func parseListener(raw string) (Listener, string, error) {
	value := strings.TrimSpace(raw)
	if value == "loopback" {
		return ListenLoopback, "", nil
	}
	if path, ok := strings.CutPrefix(value, "unix:"); ok {
		if strings.TrimSpace(path) == "" {
			return "", "", fmt.Errorf("invalid AO_LISTEN %q: unix socket path is required", raw)
		}
		abs, err := absOverride("AO_LISTEN", path)
		if err != nil {
			return "", "", err
		}
		return ListenUnix, abs, nil
	}
	if strings.HasPrefix(value, "tcp:") || strings.HasPrefix(value, "tcp://") {
		return "", "", fmt.Errorf("invalid AO_LISTEN %q: non-loopback TCP hosts are not allowed; use loopback or unix:<path>", raw)
	}
	return "", "", fmt.Errorf("invalid AO_LISTEN %q: must be loopback or unix:<path>", raw)
}

func parseToggleEnv(name, raw string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "on", "true", "1", "yes":
		return true, nil
	case "off", "false", "0", "no":
		return false, nil
	default:
		return false, fmt.Errorf("%s must be off|on", name)
	}
}

func parseTelemetryRemote(raw string) (TelemetryRemote, error) {
	switch TelemetryRemote(strings.ToLower(strings.TrimSpace(raw))) {
	case TelemetryRemoteOff:
		return TelemetryRemoteOff, nil
	case TelemetryRemotePostHog:
		return TelemetryRemotePostHog, nil
	default:
		return "", fmt.Errorf("must be off|posthog")
	}
}

// parseTelemetryDisabledEvents reads the comma-separated kill-switch list.
// Unlike the other telemetry env vars this never fails: an unparseable or
// misspelled entry must not stop the daemon from booting, because the whole
// point of the switch is to be usable in a hurry during an incident. An entry
// that matches no event name is simply inert.
func parseTelemetryDisabledEvents(raw string) []string {
	return filterBlankStrings(strings.Split(raw, ","))
}

// parsePositiveDuration rejects zero and negative durations: a zero
// RequestTimeout would expire every request instantly, and a non-positive
// ShutdownTimeout would defeat graceful shutdown.
func parsePositiveDuration(name, raw string) (time.Duration, error) {
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q: %w", name, raw, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("invalid %s %q: must be > 0", name, raw)
	}
	return d, nil
}

// newAppRunID mints the fallback launch id used when no supervising app
// supplied one. Randomness (not a timestamp or PID) is what guarantees two
// boots never collide, which is what orphan detection relies on. A failure to
// read entropy falls back to the boot time — worse, but still monotonic enough
// to distinguish runs, and never worth refusing to start the daemon over.
func newAppRunID() string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return "apprun-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return "apprun-" + hex.EncodeToString(buf)
}

// resolveRunFilePath picks where running.json lives. AO_RUN_FILE wins, followed
// by the optional config file, then the canonical AO home directory so the CLI
// and Electron supervisor share one handshake location.
func resolveRunFilePath(configured string) (string, error) {
	if p, ok := os.LookupEnv("AO_RUN_FILE"); ok && p != "" {
		return absOverride("AO_RUN_FILE", p)
	}
	if configured != "" {
		return configured, nil
	}
	stateDir, err := defaultStateDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(stateDir, "running.json"), nil
}

// resolveDataDir picks where durable state (the SQLite DB) lives. An explicit
// AO_DATA_DIR wins; otherwise it defaults under the same canonical AO home
// directory as the run-file.
func resolveDataDir() (string, error) {
	if p, ok := os.LookupEnv("AO_DATA_DIR"); ok && p != "" {
		return absOverride("AO_DATA_DIR", p)
	}
	stateDir, err := defaultStateDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(stateDir, "data"), nil
}

func defaultStateDir() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve state dir: %w", err)
	}
	return filepath.Join(homeDir, ".ao"), nil
}

// absOverride resolves an explicit AO_DATA_DIR/AO_RUN_FILE override to an
// absolute path against the process's launch cwd. The daemon chdir's into its
// data dir at startup (see stabilizeWorkingDirectory), so a relative override
// left as-is would be re-resolved against the new cwd and double-nest state
// (e.g. AO_DATA_DIR=data -> <cwd>/data/data). Absolutizing here keeps the path
// stable regardless of the later chdir.
func absOverride(name, p string) (string, error) {
	abs, err := filepath.Abs(p)
	if err != nil {
		return "", fmt.Errorf("resolve %s %q: %w", name, p, err)
	}
	return abs, nil
}
