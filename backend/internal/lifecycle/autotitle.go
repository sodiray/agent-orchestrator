package lifecycle

import (
	"context"
	"strings"
	"time"
	"unicode"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// autoTitleMaxRunes matches the display-name limit enforced at spawn.
const autoTitleMaxRunes = 20

// TitleFromPrompt condenses a user's first message into a display name: the
// first line, whitespace collapsed, capped at the display-name limit on a word
// boundary where one is available. Returns "" when nothing usable remains, so
// the caller leaves the existing name alone rather than blanking it.
func TitleFromPrompt(prompt string) string {
	line := strings.TrimSpace(prompt)
	if idx := strings.IndexAny(line, "\r\n"); idx >= 0 {
		line = line[:idx]
	}
	line = strings.Join(strings.Fields(line), " ")
	line = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, line)
	line = strings.TrimSpace(line)
	if line == "" {
		return ""
	}
	runes := []rune(line)
	if len(runes) <= autoTitleMaxRunes {
		return line
	}
	clipped := string(runes[:autoTitleMaxRunes])
	// Prefer a word boundary, but only when it keeps most of the budget:
	// clipping "implementthewholething now" back to "im" would read worse than
	// a hard cut mid-word.
	if cut := strings.LastIndexAny(clipped, " -_/"); cut >= autoTitleMaxRunes/2 {
		clipped = clipped[:cut]
	}
	return strings.TrimRight(strings.TrimSpace(clipped), " -_/")
}

// autoTitle names a session after its first user prompt, when its project opts
// in.
//
// It fires exactly once per session: the stored prompt is both the record of
// what the session was asked to do and the marker that it has already been
// named. Any name the session carries afterwards is a human's, and a tool that
// renamed a session out from under the person who named it would be worse than
// one that never named it at all.
//
// Best-effort by construction: this runs on the hook path, where a failure must
// never disturb the activity signal the hook actually came to deliver.
func (m *Manager) autoTitle(ctx context.Context, id domain.SessionID, prompt string) {
	if m.projects == nil || strings.TrimSpace(prompt) == "" {
		return
	}
	title := TitleFromPrompt(prompt)
	if title == "" {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	rec, ok, err := m.store.GetSession(ctx, id)
	if err != nil || !ok {
		return
	}
	// Already carries a prompt => already named itself once. Leave it alone.
	if strings.TrimSpace(rec.Metadata.Prompt) != "" {
		return
	}
	project, ok, err := m.projects.GetProject(ctx, string(rec.ProjectID))
	if err != nil || !ok || !project.Config.AutoTitle {
		return
	}

	rec.Metadata.Prompt = prompt
	rec.DisplayName = title
	rec.UpdatedAt = time.Now().UTC()
	_ = m.store.UpdateSession(ctx, rec)
}
