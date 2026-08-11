package sessionmanager

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

func TestParseEnvFile(t *testing.T) {
	t.Parallel()

	input := strings.Join([]string{
		"# a comment",
		"",
		"   ",
		"PLAIN=value",
		"export EXPORTED=exported-value",
		`DOUBLE="quoted value"`,
		"SINGLE='quoted value'",
		"  SPACED  =  spaced value  ",
		"URL=postgres://user:pw@host:5432/db?sslmode=require",
		"EMPTY=",
		"# trailing comment",
	}, "\n")

	env, err := parseEnvFile(strings.NewReader(input))
	if err != nil {
		t.Fatalf("parseEnvFile: %v", err)
	}

	want := map[string]string{
		"PLAIN":    "value",
		"EXPORTED": "exported-value",
		"DOUBLE":   "quoted value",
		"SINGLE":   "quoted value",
		"SPACED":   "spaced value",
		// A value containing '=' must survive intact: connection strings are the
		// common case and splitting on the last '=' would silently truncate one.
		"URL":   "postgres://user:pw@host:5432/db?sslmode=require",
		"EMPTY": "",
	}
	if len(env) != len(want) {
		t.Fatalf("parsed %d vars, want %d: %v", len(env), len(want), env)
	}
	for key, value := range want {
		if env[key] != value {
			t.Errorf("%s = %q, want %q", key, env[key], value)
		}
	}
}

func TestParseEnvFileRejectsMalformedLines(t *testing.T) {
	t.Parallel()

	cases := map[string]struct{ input, wantErr string }{
		"no delimiter":     {"GOOD=1\nthis is not an assignment\n", "line 2"},
		"invalid key":      {"GOOD=1\nBAD-KEY=2\n", "line 2"},
		"leading digit":    {"1BAD=2\n", "line 1"},
		"empty key":        {"=orphaned\n", "line 1"},
		"space in the key": {"TWO WORDS=1\n", "line 1"},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := parseEnvFile(strings.NewReader(tc.input))
			if err == nil {
				t.Fatalf("parseEnvFile(%q) succeeded, want an error naming the line", tc.input)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not locate the problem (want %q)", err, tc.wantErr)
			}
		})
	}
}

func TestLoadProjectEnvWithoutEnvFileReturnsInlineEnv(t *testing.T) {
	t.Parallel()

	project := domain.ProjectRecord{
		Path:   t.TempDir(),
		Config: domain.ProjectConfig{Env: map[string]string{"INLINE": "1"}},
	}
	env, err := loadProjectEnv(project)
	if err != nil {
		t.Fatalf("loadProjectEnv: %v", err)
	}
	if env["INLINE"] != "1" {
		t.Errorf("INLINE = %q, want 1", env["INLINE"])
	}
}

func TestLoadProjectEnvMergesFileUnderInlineEnv(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	write(t, filepath.Join(dir, ".env.example"), "FROM_FILE=file\nSHARED=file\n")

	project := domain.ProjectRecord{
		Path: dir,
		Config: domain.ProjectConfig{
			EnvFile: ".env.example",
			Env:     map[string]string{"SHARED": "inline", "FROM_INLINE": "inline"},
		},
	}
	env, err := loadProjectEnv(project)
	if err != nil {
		t.Fatalf("loadProjectEnv: %v", err)
	}

	if env["FROM_FILE"] != "file" {
		t.Errorf("FROM_FILE = %q, want file", env["FROM_FILE"])
	}
	if env["FROM_INLINE"] != "inline" {
		t.Errorf("FROM_INLINE = %q, want inline", env["FROM_INLINE"])
	}
	// Inline Env is the more specific setting and must win.
	if env["SHARED"] != "inline" {
		t.Errorf("SHARED = %q, want inline to override the file", env["SHARED"])
	}
}

// A configured-but-unreadable env file must fail the spawn. Returning an empty
// map would start a session that looks healthy and then fails wherever the
// missing credential is first needed, which is the failure this feature exists
// to prevent.
func TestLoadProjectEnvFailsOnMissingFile(t *testing.T) {
	t.Parallel()

	project := domain.ProjectRecord{
		Path:   t.TempDir(),
		Config: domain.ProjectConfig{EnvFile: ".env.absent"},
	}
	if _, err := loadProjectEnv(project); err == nil {
		t.Fatal("loadProjectEnv succeeded with a missing env file, want an error")
	}
}

func TestLoadProjectEnvRejectsPathEscape(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	project := domain.ProjectRecord{
		Path:   dir,
		Config: domain.ProjectConfig{EnvFile: "../outside.env"},
	}
	if _, err := loadProjectEnv(project); err == nil {
		t.Fatal("loadProjectEnv accepted a path outside the project root")
	}
}

func write(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
