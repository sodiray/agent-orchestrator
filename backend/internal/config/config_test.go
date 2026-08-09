package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	// Clear every recognised var so we observe pure defaults regardless of the
	// surrounding environment.
	for _, k := range []string{"AO_PORT", "AO_LISTEN", "AO_REQUEST_TIMEOUT", "AO_SHUTDOWN_TIMEOUT", "AO_REMOTE_HOST_PROBE_TIMEOUT", "AO_HOST_INVENTORY_COMMAND", "AO_HOST_INVENTORY_INTERVAL", "AO_HOST_INVENTORY_TIMEOUT", "AO_HOST_INVENTORY_MAX_OUTPUT", "AO_RUN_FILE", "AO_DATA_DIR", "AO_AGENT", "AO_ALLOWED_ORIGINS", "AO_TELEMETRY_EVENTS", "AO_TELEMETRY_METRICS", "AO_TELEMETRY_REMOTE", "AO_TELEMETRY_POSTHOG_KEY", "AO_TELEMETRY_POSTHOG_HOST", "AO_TELEMETRY_DISABLED_EVENTS", "AO_TELEMETRY_APP_VERSION"} {
		t.Setenv(k, "")
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Host != LoopbackHost {
		t.Errorf("Host = %q, want %q", cfg.Host, LoopbackHost)
	}
	if cfg.Port != DefaultPort {
		t.Errorf("Port = %d, want %d", cfg.Port, DefaultPort)
	}
	if cfg.Listen != ListenLoopback || cfg.UnixSocketPath != "" {
		t.Errorf("listener = %q, socket = %q, want loopback with no socket", cfg.Listen, cfg.UnixSocketPath)
	}
	if cfg.RequestTimeout != DefaultRequestTimeout {
		t.Errorf("RequestTimeout = %s, want %s", cfg.RequestTimeout, DefaultRequestTimeout)
	}
	if cfg.ShutdownTimeout != DefaultShutdownTimeout {
		t.Errorf("ShutdownTimeout = %s, want %s", cfg.ShutdownTimeout, DefaultShutdownTimeout)
	}
	if cfg.RemoteHostProbeTimeout != DefaultRemoteHostProbeTimeout {
		t.Errorf("RemoteHostProbeTimeout = %s, want %s", cfg.RemoteHostProbeTimeout, DefaultRemoteHostProbeTimeout)
	}
	if len(cfg.HostInventoryCommand) != 0 || cfg.HostInventoryInterval != DefaultHostInventoryInterval || cfg.HostInventoryTimeout != DefaultHostInventoryTimeout || cfg.HostInventoryMaxOutput != DefaultHostInventoryMaxOutput {
		t.Fatalf("host inventory defaults = %+v", cfg)
	}
	if cfg.RunFilePath == "" {
		t.Error("RunFilePath is empty, want a resolved default path")
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("UserHomeDir: %v", err)
	}
	wantRunFilePath := filepath.Join(homeDir, ".ao", "running.json")
	if cfg.RunFilePath != wantRunFilePath {
		t.Errorf("RunFilePath = %q, want %q", cfg.RunFilePath, wantRunFilePath)
	}
	if cfg.DataDir == "" {
		t.Error("DataDir is empty, want a resolved default path")
	}
	wantDataDir := filepath.Join(homeDir, ".ao", "data")
	if cfg.DataDir != wantDataDir {
		t.Errorf("DataDir = %q, want %q", cfg.DataDir, wantDataDir)
	}
	if cfg.Telemetry.Remote != TelemetryRemoteOff || cfg.Telemetry.PostHogHost != DefaultTelemetryPostHogHost {
		t.Fatalf("Telemetry defaults = %+v", cfg.Telemetry)
	}
}

func TestLoadAbsolutizesRelativeOverrides(t *testing.T) {
	// A relative override must be resolved to absolute at Load time. The daemon
	// chdir's into its data dir at startup, so a relative path left as-is would
	// be re-resolved against the new cwd and double-nest state.
	t.Setenv("AO_RUN_FILE", "rel-running.json")
	t.Setenv("AO_DATA_DIR", "rel-data")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !filepath.IsAbs(cfg.RunFilePath) {
		t.Errorf("RunFilePath = %q, want absolute", cfg.RunFilePath)
	}
	if !filepath.IsAbs(cfg.DataDir) {
		t.Errorf("DataDir = %q, want absolute", cfg.DataDir)
	}
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(cwd, "rel-data"); cfg.DataDir != want {
		t.Errorf("DataDir = %q, want %q", cfg.DataDir, want)
	}
	if want := filepath.Join(cwd, "rel-running.json"); cfg.RunFilePath != want {
		t.Errorf("RunFilePath = %q, want %q", cfg.RunFilePath, want)
	}
}

func TestLoadOverrides(t *testing.T) {
	overrideDir := t.TempDir()
	runFilePath := filepath.Join(overrideDir, "ao-test-running.json")
	dataDir := filepath.Join(overrideDir, "ao-test-data")

	t.Setenv("AO_PORT", "4002")
	t.Setenv("AO_REQUEST_TIMEOUT", "5s")
	t.Setenv("AO_SHUTDOWN_TIMEOUT", "3s")
	t.Setenv("AO_REMOTE_HOST_PROBE_TIMEOUT", "12s")
	t.Setenv("AO_HOST_INVENTORY_COMMAND", `["inventory-command","list","--json"]`)
	t.Setenv("AO_HOST_INVENTORY_INTERVAL", "45s")
	t.Setenv("AO_HOST_INVENTORY_TIMEOUT", "8s")
	t.Setenv("AO_HOST_INVENTORY_MAX_OUTPUT", "8192")
	t.Setenv("AO_RUN_FILE", runFilePath)
	t.Setenv("AO_DATA_DIR", dataDir)
	t.Setenv("AO_TELEMETRY_EVENTS", "on")
	t.Setenv("AO_TELEMETRY_METRICS", "off")
	t.Setenv("AO_TELEMETRY_REMOTE", "posthog")
	t.Setenv("AO_TELEMETRY_POSTHOG_KEY", "phc_test")
	t.Setenv("AO_TELEMETRY_POSTHOG_HOST", "https://eu.i.posthog.com")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Addr() != "127.0.0.1:4002" {
		t.Errorf("Addr() = %q, want 127.0.0.1:4002", cfg.Addr())
	}
	if cfg.RequestTimeout != 5*time.Second {
		t.Errorf("RequestTimeout = %s, want 5s", cfg.RequestTimeout)
	}
	if cfg.ShutdownTimeout != 3*time.Second {
		t.Errorf("ShutdownTimeout = %s, want 3s", cfg.ShutdownTimeout)
	}
	if cfg.RemoteHostProbeTimeout != 12*time.Second {
		t.Errorf("RemoteHostProbeTimeout = %s, want 12s", cfg.RemoteHostProbeTimeout)
	}
	if got := cfg.HostInventoryCommand; len(got) != 3 || got[0] != "inventory-command" || got[1] != "list" || got[2] != "--json" {
		t.Fatalf("HostInventoryCommand = %#v", got)
	}
	if cfg.HostInventoryInterval != 45*time.Second || cfg.HostInventoryTimeout != 8*time.Second || cfg.HostInventoryMaxOutput != 8192 {
		t.Fatalf("host inventory config = %+v", cfg)
	}
	if cfg.RunFilePath != runFilePath {
		t.Errorf("RunFilePath = %q, want %q", cfg.RunFilePath, runFilePath)
	}
	if cfg.DataDir != dataDir {
		t.Errorf("DataDir = %q, want %q", cfg.DataDir, dataDir)
	}
	if !cfg.Telemetry.Events || cfg.Telemetry.Metrics {
		t.Fatalf("Telemetry toggles = %+v", cfg.Telemetry)
	}
	if cfg.Telemetry.Remote != TelemetryRemotePostHog || cfg.Telemetry.PostHogKey != "phc_test" || cfg.Telemetry.PostHogHost != "https://eu.i.posthog.com" {
		t.Fatalf("Telemetry remote = %+v", cfg.Telemetry)
	}
}

func TestLoadUnixSocketListener(t *testing.T) {
	t.Setenv("AO_DATA_DIR", t.TempDir())
	socketPath := filepath.Join(t.TempDir(), "nested", "daemon.sock")
	t.Setenv("AO_LISTEN", "unix:"+socketPath)
	t.Setenv("AO_PORT", "not-a-port")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Listen != ListenUnix {
		t.Fatalf("Listen = %q, want %q", cfg.Listen, ListenUnix)
	}
	if cfg.UnixSocketPath != socketPath {
		t.Fatalf("UnixSocketPath = %q, want %q", cfg.UnixSocketPath, socketPath)
	}
}

func TestLoadInvalid(t *testing.T) {
	t.Setenv("AO_DATA_DIR", t.TempDir())
	tests := []struct {
		name string
		env  map[string]string
	}{
		{"non-numeric port", map[string]string{"AO_PORT": "abc"}},
		{"port out of range", map[string]string{"AO_PORT": "70000"}},
		{"unix socket path missing", map[string]string{"AO_LISTEN": "unix:"}},
		{"non-loopback tcp listener", map[string]string{"AO_LISTEN": "tcp:0.0.0.0:3001"}},
		{"bad request timeout", map[string]string{"AO_REQUEST_TIMEOUT": "soon"}},
		{"bad shutdown timeout", map[string]string{"AO_SHUTDOWN_TIMEOUT": "later"}},
		{"bad remote host probe timeout", map[string]string{"AO_REMOTE_HOST_PROBE_TIMEOUT": "later"}},
		{"bad inventory command", map[string]string{"AO_HOST_INVENTORY_COMMAND": "inventory-command list"}},
		{"empty inventory command", map[string]string{"AO_HOST_INVENTORY_COMMAND": "[]"}},
		{"bad inventory interval", map[string]string{"AO_HOST_INVENTORY_INTERVAL": "later"}},
		{"bad inventory timeout", map[string]string{"AO_HOST_INVENTORY_TIMEOUT": "later"}},
		{"bad inventory output limit", map[string]string{"AO_HOST_INVENTORY_MAX_OUTPUT": "0"}},
		{"zero request timeout", map[string]string{"AO_REQUEST_TIMEOUT": "0s"}},
		{"negative request timeout", map[string]string{"AO_REQUEST_TIMEOUT": "-1s"}},
		{"zero shutdown timeout", map[string]string{"AO_SHUTDOWN_TIMEOUT": "0s"}},
		{"negative shutdown timeout", map[string]string{"AO_SHUTDOWN_TIMEOUT": "-5s"}},
		{"zero remote host probe timeout", map[string]string{"AO_REMOTE_HOST_PROBE_TIMEOUT": "0s"}},
		{"negative remote host probe timeout", map[string]string{"AO_REMOTE_HOST_PROBE_TIMEOUT": "-5s"}},
		{"null origin", map[string]string{"AO_ALLOWED_ORIGINS": "app://renderer,null"}},
		{"wildcard origin", map[string]string{"AO_ALLOWED_ORIGINS": "*"}},
		{"bad telemetry events", map[string]string{"AO_TELEMETRY_EVENTS": "maybe"}},
		{"bad telemetry metrics", map[string]string{"AO_TELEMETRY_METRICS": "maybe"}},
		{"bad telemetry remote", map[string]string{"AO_TELEMETRY_REMOTE": "otlp"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			if _, err := Load(); err == nil {
				t.Fatal("Load() = nil error, want error")
			}
		})
	}
}

func TestLoadAllowedOrigins(t *testing.T) {
	t.Setenv("AO_DATA_DIR", t.TempDir())
	t.Run("default includes the packaged renderer origin", func(t *testing.T) {
		t.Setenv("AO_ALLOWED_ORIGINS", "")
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		found := false
		for _, origin := range cfg.AllowedOrigins {
			if origin == "app://renderer" {
				found = true
			}
		}
		if !found {
			t.Errorf("AllowedOrigins = %v, want app://renderer included", cfg.AllowedOrigins)
		}
	})

	t.Run("override replaces defaults and trims entries", func(t *testing.T) {
		t.Setenv("AO_ALLOWED_ORIGINS", " app://renderer , http://localhost:9999 ,")
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		want := []string{"app://renderer", "http://localhost:9999"}
		if len(cfg.AllowedOrigins) != len(want) {
			t.Fatalf("AllowedOrigins = %v, want %v", cfg.AllowedOrigins, want)
		}
		for i, origin := range want {
			if cfg.AllowedOrigins[i] != origin {
				t.Errorf("AllowedOrigins[%d] = %q, want %q", i, cfg.AllowedOrigins[i], origin)
			}
		}
	})
}

// The kill switch and the supervisor-supplied version are user-visible
// boundaries: the daemon reads them from the environment the desktop app hands
// it, so a parsing regression here silently disables the switch.
func TestLoadTelemetryDisabledEventsAndAppVersion(t *testing.T) {
	t.Setenv("AO_DATA_DIR", t.TempDir())
	t.Setenv("AO_TELEMETRY_DISABLED_EVENTS", " ao.v2.app.active , ao.renderer.* ,, ")
	t.Setenv("AO_TELEMETRY_APP_VERSION", "  0.11.2  ")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := []string{"ao.v2.app.active", "ao.renderer.*"}
	if len(cfg.Telemetry.DisabledEvents) != len(want) {
		t.Fatalf("DisabledEvents = %#v, want %#v", cfg.Telemetry.DisabledEvents, want)
	}
	for i, name := range want {
		if cfg.Telemetry.DisabledEvents[i] != name {
			t.Fatalf("DisabledEvents[%d] = %q, want %q", i, cfg.Telemetry.DisabledEvents[i], name)
		}
	}
	if cfg.Telemetry.AppVersion != "0.11.2" {
		t.Fatalf("AppVersion = %q, want trimmed 0.11.2", cfg.Telemetry.AppVersion)
	}
}

// An unparseable or blank list must never stop the daemon booting: the switch
// has to be usable in a hurry, so a bad entry is inert rather than fatal.
func TestLoadTelemetryDisabledEventsBlankIsInert(t *testing.T) {
	t.Setenv("AO_DATA_DIR", t.TempDir())
	t.Setenv("AO_TELEMETRY_DISABLED_EVENTS", " , , ")
	t.Setenv("AO_TELEMETRY_APP_VERSION", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Telemetry.DisabledEvents) != 0 {
		t.Fatalf("DisabledEvents = %#v, want empty", cfg.Telemetry.DisabledEvents)
	}
	if cfg.Telemetry.AppVersion != "" {
		t.Fatalf("AppVersion = %q, want empty", cfg.Telemetry.AppVersion)
	}
}

func TestLoadConfigFileSuppliesSettings(t *testing.T) {
	dataDir := t.TempDir()
	unsetConfigEnvironment(t)
	t.Setenv("AO_DATA_DIR", dataDir)
	writeConfigFile(t, dataDir, `version: 1
listen: loopback
port: 4012
requestTimeout: 7s
shutdownTimeout: 4s
remoteHostProbeTimeout: 13s
hostInventory:
  command: [inventory, list, --json]
  interval: 50s
  timeout: 9s
  maxOutput: 8192
`)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Port != 4012 || cfg.RequestTimeout != 7*time.Second || cfg.ShutdownTimeout != 4*time.Second || cfg.RemoteHostProbeTimeout != 13*time.Second {
		t.Fatalf("file settings = %+v", cfg)
	}
	if got := cfg.HostInventoryCommand; len(got) != 3 || got[0] != "inventory" || got[1] != "list" || got[2] != "--json" {
		t.Fatalf("HostInventoryCommand = %#v", got)
	}
	if cfg.HostInventoryInterval != 50*time.Second || cfg.HostInventoryTimeout != 9*time.Second || cfg.HostInventoryMaxOutput != 8192 {
		t.Fatalf("host inventory settings = %+v", cfg)
	}
}

func TestLoadEnvironmentOverridesConfigFile(t *testing.T) {
	dataDir := t.TempDir()
	unsetConfigEnvironment(t)
	t.Setenv("AO_DATA_DIR", dataDir)
	writeConfigFile(t, dataDir, `version: 1
listen: unix:/tmp/ao-from-file.sock
port: 4012
hostInventory:
  command: [from-file]
  interval: 50s
  timeout: 52s
`)
	t.Setenv("AO_LISTEN", "loopback")
	t.Setenv("AO_PORT", "4013")
	t.Setenv("AO_HOST_INVENTORY_COMMAND", `["from-environment","--json"]`)
	t.Setenv("AO_HOST_INVENTORY_INTERVAL", "51s")
	t.Setenv("AO_HOST_INVENTORY_TIMEOUT", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Listen != ListenLoopback || cfg.Port != 4013 {
		t.Fatalf("listener settings = %+v", cfg)
	}
	if got := cfg.HostInventoryCommand; len(got) != 2 || got[0] != "from-environment" || got[1] != "--json" {
		t.Fatalf("HostInventoryCommand = %#v", got)
	}
	if cfg.HostInventoryInterval != 51*time.Second {
		t.Fatalf("HostInventoryInterval = %s, want 51s", cfg.HostInventoryInterval)
	}
	if cfg.HostInventoryTimeout != DefaultHostInventoryTimeout {
		t.Fatalf("HostInventoryTimeout = %s, want %s after empty environment override", cfg.HostInventoryTimeout, DefaultHostInventoryTimeout)
	}
}

func TestLoadAbsentConfigFilePreservesDefaults(t *testing.T) {
	dataDir := t.TempDir()
	unsetConfigEnvironment(t)
	t.Setenv("AO_DATA_DIR", dataDir)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Port != DefaultPort || cfg.Listen != ListenLoopback || cfg.RequestTimeout != DefaultRequestTimeout {
		t.Fatalf("settings with no config file = %+v", cfg)
	}
	if len(cfg.HostInventoryCommand) != 0 || cfg.HostInventoryInterval != DefaultHostInventoryInterval {
		t.Fatalf("host inventory with no config file = %+v", cfg)
	}
}

func TestLoadMalformedConfigFileFailsLoudly(t *testing.T) {
	dataDir := t.TempDir()
	unsetConfigEnvironment(t)
	t.Setenv("AO_DATA_DIR", dataDir)
	writeConfigFile(t, dataDir, "version: 1\nhostInventory: [\n")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() = nil error, want malformed configuration file failure")
	}
	if !strings.Contains(err.Error(), "parse daemon configuration file") {
		t.Fatalf("Load() error = %q, want configuration file parse error", err)
	}
}

func TestLoadConfigFileRejectsWorldWritableFile(t *testing.T) {
	dataDir := t.TempDir()
	unsetConfigEnvironment(t)
	t.Setenv("AO_DATA_DIR", dataDir)
	path := writeConfigFile(t, dataDir, "version: 1\n")
	if err := os.Chmod(path, 0o666); err != nil {
		t.Fatalf("chmod world-writable config file: %v", err)
	}

	_, err := Load()
	if err == nil {
		t.Fatal("Load() = nil error, want unsafe configuration file failure")
	}
	if !strings.Contains(err.Error(), "must not be writable by group or other users") {
		t.Fatalf("Load() error = %q, want unsafe permissions error", err)
	}
}

func unsetConfigEnvironment(t *testing.T) {
	t.Helper()
	for _, key := range []string{"AO_PORT", "AO_LISTEN", "AO_REQUEST_TIMEOUT", "AO_SHUTDOWN_TIMEOUT", "AO_REMOTE_HOST_PROBE_TIMEOUT", "AO_HOST_INVENTORY_COMMAND", "AO_HOST_INVENTORY_INTERVAL", "AO_HOST_INVENTORY_TIMEOUT", "AO_HOST_INVENTORY_MAX_OUTPUT", "AO_RUN_FILE", "AO_AGENT", "AO_ALLOWED_ORIGINS", "AO_TELEMETRY_EVENTS", "AO_TELEMETRY_METRICS", "AO_TELEMETRY_REMOTE", "AO_TELEMETRY_POSTHOG_KEY", "AO_TELEMETRY_POSTHOG_HOST", "AO_TELEMETRY_DISABLED_EVENTS"} {
		t.Setenv(key, "")
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("unset %s: %v", key, err)
		}
	}
}

func writeConfigFile(t *testing.T, dataDir, contents string) string {
	t.Helper()
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		t.Fatalf("make data directory: %v", err)
	}
	path := filepath.Join(dataDir, configFileName)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write configuration file: %v", err)
	}
	return path
}
