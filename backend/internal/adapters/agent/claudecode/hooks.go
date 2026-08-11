package claudecode

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/aoagents/agent-orchestrator/backend/internal/adapters/agent/hooksjson"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

const (
	claudeSettingsDirName  = ".claude"
	claudeSettingsFileName = "settings.local.json"
	claudeLegacyHookPrefix = "ao hooks claude-code "
	claudeHookTimeout      = 30
)

// claudeStartupMatcher is referenced by pointer so SessionStart serializes with
// its required "startup" matcher.
var claudeStartupMatcher = "startup"

// claudeManagedHooks is the source of truth for the hooks AO installs:
// SessionStart (under the "startup" matcher), UserPromptSubmit, the tool-use
// trio (PreToolUse, PostToolUse, PostToolUseFailure), PermissionRequest,
// Stop, Notification, and SessionEnd. They report normalized session metadata
// and activity-state signals back into AO's store (see DeriveActivityState).
// Notification and SessionEnd carry no matcher: each installs once and fires
// for every sub-type, and the handler filters on the payload's
// notification_type / reason field. The tool-use hooks also carry no matcher
// (fire for every tool): their payloads carry tool_name/tool_use_id, which
// lifecycle uses to clear a stale sticky `blocked` only when the specific
// approved tool finishes — the daemon-side precedence rule is what makes these
// signals safe against parallel-subagent traffic (the naive mapping without it
// was reverted in PR #5's review). PermissionRequest fires when a permission
// dialog appears and carries the blocking tool_name; `ao hooks` writes nothing
// to stdout, so installing it never injects a permission decision.
var claudeHookEvents = []string{
	"session-start",
	"user-prompt-submit",
	"pre-tool-use",
	"post-tool-use",
	"post-tool-use-failure",
	"permission-request",
	"stop",
	"notification",
	"subagent-stop",
	"session-end",
}

func claudeManagedHooks(executablePath string) []hooksjson.HookSpec {
	commandPrefix := claudeHookCommandPrefix(executablePath)
	return []hooksjson.HookSpec{
		{Event: "SessionStart", Matcher: &claudeStartupMatcher, Command: commandPrefix + "session-start"},
		{Event: "UserPromptSubmit", Command: commandPrefix + "user-prompt-submit"},
		{Event: "PreToolUse", Command: commandPrefix + "pre-tool-use"},
		{Event: "PostToolUse", Command: commandPrefix + "post-tool-use"},
		{Event: "PostToolUseFailure", Command: commandPrefix + "post-tool-use-failure"},
		{Event: "PermissionRequest", Command: commandPrefix + "permission-request"},
		{Event: "Stop", Command: commandPrefix + "stop"},
		{Event: "Notification", Command: commandPrefix + "notification"},
		{Event: "SubagentStop", Command: commandPrefix + "subagent-stop"},
		{Event: "SessionEnd", Command: commandPrefix + "session-end"},
	}
}

func claudeHooks(executablePath string) hooksjson.Manager {
	return hooksjson.Manager{
		Label:            "claude-code",
		CommandPrefix:    claudeHookCommandPrefix(executablePath),
		IsManagedCommand: isClaudeHookCommand,
		Timeout:          claudeHookTimeout,
		Path:             claudeSettingsPath,
		Managed:          claudeManagedHooks(executablePath),
	}
}

func claudeSettingsPath(workspacePath string) string {
	return filepath.Join(workspacePath, claudeSettingsDirName, claudeSettingsFileName)
}

func claudeHookExecutable(cfg ports.WorkspaceHookConfig) (string, error) {
	executablePath := strings.TrimSpace(cfg.ExecutablePath)
	if executablePath == "" {
		var err error
		executablePath, err = os.Executable()
		if err != nil {
			return "", fmt.Errorf("resolve AO executable: %w", err)
		}
	}
	if !filepath.IsAbs(executablePath) {
		return "", fmt.Errorf("AO executable path is not absolute: %s", executablePath)
	}
	return executablePath, nil
}

func claudeHookCommandPrefix(executablePath string) string {
	if runtime.GOOS == "windows" {
		return `"` + strings.ReplaceAll(executablePath, `"`, `\"`) + `" hooks claude-code `
	}
	return "'" + strings.ReplaceAll(executablePath, "'", "'\\''") + "' hooks claude-code "
}

func isClaudeHookCommand(command string) bool {
	if strings.HasPrefix(command, claudeLegacyHookPrefix) {
		return true
	}
	if !strings.HasPrefix(command, "'") && !strings.HasPrefix(command, `"`) {
		return false
	}
	for _, event := range claudeHookEvents {
		if strings.HasSuffix(command, " hooks claude-code "+event) {
			return true
		}
	}
	return false
}

// GetAgentHooks installs AO's Claude Code hooks, preserving user-defined hooks and unrelated settings.
func (p *Plugin) GetAgentHooks(ctx context.Context, cfg ports.WorkspaceHookConfig) error {
	executablePath, err := claudeHookExecutable(cfg)
	if err != nil {
		return fmt.Errorf("claude-code.GetAgentHooks: %w", err)
	}
	return claudeHooks(executablePath).Install(ctx, cfg.WorkspacePath)
}

// UninstallHooks removes AO's Claude Code hooks, leaving user-defined hooks untouched.
func (p *Plugin) UninstallHooks(ctx context.Context, workspacePath string) error {
	executablePath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("claude-code.UninstallHooks: resolve AO executable: %w", err)
	}
	return claudeHooks(executablePath).Uninstall(ctx, workspacePath)
}

// AreHooksInstalled reports whether any AO Claude Code hook is present.
func (p *Plugin) AreHooksInstalled(ctx context.Context, workspacePath string) (bool, error) {
	executablePath, err := os.Executable()
	if err != nil {
		return false, fmt.Errorf("claude-code.AreHooksInstalled: resolve AO executable: %w", err)
	}
	return claudeHooks(executablePath).AreInstalled(ctx, workspacePath)
}
