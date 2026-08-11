package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	yaml "gopkg.in/yaml.v3"
)

const configFileName = "config.yaml"

type daemonConfigFile struct {
	Version                int                  `yaml:"version"`
	Listen                 *string              `yaml:"listen"`
	Port                   *int                 `yaml:"port"`
	RequestTimeout         *string              `yaml:"requestTimeout"`
	ShutdownTimeout        *string              `yaml:"shutdownTimeout"`
	RemoteHostProbeTimeout *string              `yaml:"remoteHostProbeTimeout"`
	HostInventory          *hostInventoryFile   `yaml:"hostInventory"`
	RunFile                *string              `yaml:"runFile"`
	Agent                  *string              `yaml:"agent"`
	AllowedOrigins         *[]string            `yaml:"allowedOrigins"`
	Telemetry              *telemetryConfigFile `yaml:"telemetry"`
}

type hostInventoryFile struct {
	Command   *[]string `yaml:"command"`
	Interval  *string   `yaml:"interval"`
	Timeout   *string   `yaml:"timeout"`
	MaxOutput *int64    `yaml:"maxOutput"`
}

type telemetryConfigFile struct {
	Events         *bool     `yaml:"events"`
	Metrics        *bool     `yaml:"metrics"`
	Remote         *string   `yaml:"remote"`
	PostHogKey     *string   `yaml:"postHogKey"`
	PostHogHost    *string   `yaml:"postHogHost"`
	DisabledEvents *[]string `yaml:"disabledEvents"`
}

func loadConfigFile(dataDir string) (daemonConfigFile, string, error) {
	path := filepath.Join(dataDir, configFileName)
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return daemonConfigFile{}, path, nil
	}
	if err != nil {
		return daemonConfigFile{}, path, fmt.Errorf("open daemon configuration file %s: %w", path, err)
	}
	// The file is read-only; a close error cannot change the parsed configuration.
	defer func() { _ = file.Close() }()

	info, err := file.Stat()
	if err != nil {
		return daemonConfigFile{}, path, fmt.Errorf("inspect daemon configuration file %s: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return daemonConfigFile{}, path, fmt.Errorf("invalid daemon configuration file %s: must be a regular file", path)
	}
	if info.Mode().Perm()&0o022 != 0 {
		return daemonConfigFile{}, path, fmt.Errorf("unsafe daemon configuration file %s: it must not be writable by group or other users", path)
	}

	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)
	var cfg daemonConfigFile
	if err := decoder.Decode(&cfg); err != nil {
		return daemonConfigFile{}, path, fmt.Errorf("parse daemon configuration file %s: %w", path, err)
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return daemonConfigFile{}, path, fmt.Errorf("parse daemon configuration file %s: multiple YAML documents are not allowed", path)
		}
		return daemonConfigFile{}, path, fmt.Errorf("parse daemon configuration file %s: %w", path, err)
	}
	if cfg.Version != 1 {
		return daemonConfigFile{}, path, fmt.Errorf("invalid daemon configuration file %s: version must be 1", path)
	}
	return cfg, path, nil
}

func applyConfigFile(cfg *Config, file daemonConfigFile, path string) error {
	if file.Version == 0 {
		return nil
	}
	if file.Listen != nil && !environmentOverrides("AO_LISTEN") {
		listener, socketPath, err := parseListener(*file.Listen)
		if err != nil {
			return configFileError(path, err)
		}
		cfg.Listen = listener
		cfg.UnixSocketPath = socketPath
	}
	if file.Port != nil && !environmentOverrides("AO_PORT") && !cfg.UsesUnixSocket() {
		if err := applyPort(cfg, *file.Port, "port"); err != nil {
			return configFileError(path, err)
		}
	}
	if file.RequestTimeout != nil && !environmentOverrides("AO_REQUEST_TIMEOUT") {
		d, err := parsePositiveDuration("requestTimeout", *file.RequestTimeout)
		if err != nil {
			return configFileError(path, err)
		}
		cfg.RequestTimeout = d
	}
	if file.ShutdownTimeout != nil && !environmentOverrides("AO_SHUTDOWN_TIMEOUT") {
		d, err := parsePositiveDuration("shutdownTimeout", *file.ShutdownTimeout)
		if err != nil {
			return configFileError(path, err)
		}
		cfg.ShutdownTimeout = d
	}
	if file.RemoteHostProbeTimeout != nil && !environmentOverrides("AO_REMOTE_HOST_PROBE_TIMEOUT") {
		d, err := parsePositiveDuration("remoteHostProbeTimeout", *file.RemoteHostProbeTimeout)
		if err != nil {
			return configFileError(path, err)
		}
		cfg.RemoteHostProbeTimeout = d
	}
	if inventory := file.HostInventory; inventory != nil {
		if inventory.Command != nil && !environmentOverrides("AO_HOST_INVENTORY_COMMAND") {
			if err := validateCommandArgs("hostInventory.command", *inventory.Command, true); err != nil {
				return configFileError(path, err)
			}
			cfg.HostInventoryCommand = *inventory.Command
		}
		if inventory.Interval != nil && !environmentOverrides("AO_HOST_INVENTORY_INTERVAL") {
			d, err := parsePositiveDuration("hostInventory.interval", *inventory.Interval)
			if err != nil {
				return configFileError(path, err)
			}
			cfg.HostInventoryInterval = d
		}
		if inventory.Timeout != nil && !environmentOverrides("AO_HOST_INVENTORY_TIMEOUT") {
			d, err := parsePositiveDuration("hostInventory.timeout", *inventory.Timeout)
			if err != nil {
				return configFileError(path, err)
			}
			cfg.HostInventoryTimeout = d
		}
		if inventory.MaxOutput != nil && !environmentOverrides("AO_HOST_INVENTORY_MAX_OUTPUT") {
			if *inventory.MaxOutput <= 0 {
				return configFileError(path, fmt.Errorf("invalid hostInventory.maxOutput %d: must be a positive byte count", *inventory.MaxOutput))
			}
			cfg.HostInventoryMaxOutput = *inventory.MaxOutput
		}
	}
	if file.RunFile != nil && !environmentOverrides("AO_RUN_FILE") {
		resolved, err := absOverride("runFile", *file.RunFile)
		if err != nil {
			return configFileError(path, err)
		}
		cfg.RunFilePath = resolved
	}
	if file.Agent != nil && !environmentOverrides("AO_AGENT") {
		cfg.Agent = *file.Agent
	}
	if file.AllowedOrigins != nil && !environmentOverrides("AO_ALLOWED_ORIGINS") {
		origins, err := validateAllowedOrigins(*file.AllowedOrigins, "allowedOrigins")
		if err != nil {
			return configFileError(path, err)
		}
		cfg.AllowedOrigins = origins
	}
	if telemetry := file.Telemetry; telemetry != nil {
		if telemetry.Events != nil && !environmentOverrides("AO_TELEMETRY_EVENTS") {
			cfg.Telemetry.Events = *telemetry.Events
		}
		if telemetry.Metrics != nil && !environmentOverrides("AO_TELEMETRY_METRICS") {
			cfg.Telemetry.Metrics = *telemetry.Metrics
		}
		if telemetry.Remote != nil && !environmentOverrides("AO_TELEMETRY_REMOTE") {
			remote, err := parseTelemetryRemote(*telemetry.Remote)
			if err != nil {
				return configFileError(path, fmt.Errorf("invalid telemetry.remote %q: %w", *telemetry.Remote, err))
			}
			cfg.Telemetry.Remote = remote
		}
		if telemetry.PostHogKey != nil && !environmentOverrides("AO_TELEMETRY_POSTHOG_KEY") {
			cfg.Telemetry.PostHogKey = *telemetry.PostHogKey
		}
		if telemetry.PostHogHost != nil && !environmentOverrides("AO_TELEMETRY_POSTHOG_HOST") {
			cfg.Telemetry.PostHogHost = *telemetry.PostHogHost
		}
		if telemetry.DisabledEvents != nil && !environmentOverrides("AO_TELEMETRY_DISABLED_EVENTS") {
			cfg.Telemetry.DisabledEvents = filterBlankStrings(*telemetry.DisabledEvents)
		}
	}
	return nil
}

func configFileError(path string, err error) error {
	return fmt.Errorf("invalid daemon configuration file %s: %w", path, err)
}

func environmentOverrides(name string) bool {
	_, ok := os.LookupEnv(name)
	return ok
}

func applyPort(cfg *Config, port int, name string) error {
	if port < 1 || port > 65535 {
		return fmt.Errorf("invalid %s %d: out of range 1-65535", name, port)
	}
	cfg.Port = port
	return nil
}

func validateCommandArgs(name string, args []string, allowEmpty bool) error {
	if len(args) == 0 && allowEmpty {
		return nil
	}
	if len(args) == 0 || strings.TrimSpace(args[0]) == "" {
		return fmt.Errorf("invalid %s: command argv must contain a non-empty executable", name)
	}
	for _, arg := range args {
		if strings.ContainsRune(arg, '\x00') {
			return fmt.Errorf("invalid %s: command arguments must not contain NUL", name)
		}
	}
	return nil
}

func validateAllowedOrigins(origins []string, name string) ([]string, error) {
	trimmed := make([]string, 0, len(origins))
	for _, origin := range origins {
		origin = strings.TrimSpace(origin)
		if origin == "" {
			continue
		}
		if origin == "null" || origin == "*" {
			return nil, fmt.Errorf("invalid %s entry %q: wildcard and null origins are not allowed", name, origin)
		}
		trimmed = append(trimmed, origin)
	}
	return trimmed, nil
}

func filterBlankStrings(values []string) []string {
	filtered := make([]string, 0, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			filtered = append(filtered, value)
		}
	}
	return filtered
}
