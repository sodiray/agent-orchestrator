package sessionmanager

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// loadProjectEnv resolves a project's effective session environment: the
// entries of its configured EnvFile, overlaid by its inline Env. AO-internal
// vars are applied after this by spawnEnv and still win over both.
//
// A configured file that cannot be read is an error rather than an empty map.
// The whole point of the file is to carry credentials the daemon's own
// environment lacks, so dropping them silently would produce a session that
// starts cleanly and then fails deep inside whatever needed the key — the
// failure mode this replaces. buildProjectRules makes the same call for the
// same reason.
func loadProjectEnv(project domain.ProjectRecord) (map[string]string, error) {
	rel := strings.TrimSpace(project.Config.EnvFile)
	if rel == "" {
		return project.Config.Env, nil
	}

	path, err := projectRelativeFile(project.Path, rel)
	if err != nil {
		return nil, fmt.Errorf("envFile: %w", err)
	}
	file, err := os.Open(path) //nolint:gosec // path is project config validated as repo-relative
	if err != nil {
		return nil, fmt.Errorf("read envFile %s: %w", rel, err)
	}
	defer func() { _ = file.Close() }()

	env, err := parseEnvFile(file)
	if err != nil {
		return nil, fmt.Errorf("envFile %s: %w", rel, err)
	}
	// Inline Env is the more specific setting, so it overrides the file.
	for key, value := range project.Config.Env {
		env[key] = value
	}
	return env, nil
}

// parseEnvFile reads KEY=VALUE lines, skipping blanks and # comments. An
// optional leading `export ` is accepted, and one layer of matching surrounding
// quotes is removed from the value.
//
// Malformed lines and invalid keys are errors, reported with their line number.
// Skipping them would mean a mistyped variable name reaches the agent as a
// missing credential, which surfaces as a puzzling failure far from the typo.
func parseEnvFile(r io.Reader) (map[string]string, error) {
	env := map[string]string{}
	scanner := bufio.NewScanner(r)
	for line := 1; scanner.Scan(); line++ {
		text := strings.TrimSpace(scanner.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		text = strings.TrimPrefix(text, "export ")

		key, value, found := strings.Cut(text, "=")
		if !found {
			return nil, fmt.Errorf("line %d: expected KEY=VALUE", line)
		}
		key = strings.TrimSpace(key)
		if !validEnvFileKey(key) {
			return nil, fmt.Errorf("line %d: invalid variable name %q", line, key)
		}
		env[key] = unquoteEnvValue(strings.TrimSpace(value))
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return env, nil
}

// unquoteEnvValue removes one layer of matching surrounding quotes.
func unquoteEnvValue(value string) string {
	if len(value) < 2 {
		return value
	}
	quote := value[0]
	if (quote == '"' || quote == '\'') && value[len(value)-1] == quote {
		return value[1 : len(value)-1]
	}
	return value
}

// validEnvFileKey accepts the POSIX-portable variable-name shape. The tmux
// runtime rejects anything else when it exports the block, so catching it here
// names the offending line instead of failing at spawn with no location.
func validEnvFileKey(key string) bool {
	if key == "" {
		return false
	}
	for i, r := range key {
		switch {
		case r == '_':
		case r >= 'A' && r <= 'Z':
		case r >= 'a' && r <= 'z':
		case r >= '0' && r <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}
