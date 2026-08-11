package lifecycle

import "testing"

func TestTitleFromPrompt(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{"short prompt is used whole", "fix the login bug", "fix the login bug"},
		{"first line only", "add retries\nand also logging", "add retries"},
		{"whitespace collapsed", "  fix   the   test  ", "fix the test"},
		{"long prompt clips on a word boundary", "Fix the flaky login test in auth", "Fix the flaky login"},
		{"unbroken prompt is hard-cut rather than gutted", "implementthewholedamnthing now", "implementthewholedam"},
		{"empty stays empty so the caller keeps the old name", "   ", ""},
		{"control characters are dropped", "fix\x07 the bug", "fix the bug"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := TitleFromPrompt(c.in); got != c.want {
				t.Errorf("TitleFromPrompt(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

func TestTitleFromPromptNeverExceedsDisplayLimit(t *testing.T) {
	long := "this is a considerably longer prompt than any display name allows"
	if got := []rune(TitleFromPrompt(long)); len(got) > autoTitleMaxRunes {
		t.Errorf("title %q is %d runes, limit is %d", string(got), len(got), autoTitleMaxRunes)
	}
}
