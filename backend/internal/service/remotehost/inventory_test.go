package remotehost

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestCommandInventoryProviderRejectsMalformedOversizedAndSlowOutput(t *testing.T) {
	tests := []struct {
		name      string
		mode      string
		timeout   time.Duration
		maxOutput int64
		want      string
	}{
		{name: "malformed", mode: "malformed", timeout: time.Second, maxOutput: 1024, want: "decode inventory JSON"},
		{name: "oversized", mode: "oversized", timeout: time.Second, maxOutput: 64, want: "output exceeds 64 bytes"},
		{name: "slow", mode: "slow", timeout: 10 * time.Millisecond, maxOutput: 1024, want: "timed out"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			provider, err := NewCommandInventoryProvider([]string{os.Args[0], "-test.run=TestCommandInventoryProviderHelper", "--", tc.mode}, tc.timeout, tc.maxOutput)
			if err != nil {
				t.Fatalf("new provider: %v", err)
			}
			_, err = provider.List(context.Background())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("List() error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestCommandInventoryProviderHelper(t *testing.T) {
	if len(os.Args) < 2 || os.Args[len(os.Args)-2] != "--" {
		return
	}
	switch os.Args[len(os.Args)-1] {
	case "malformed":
		_, _ = fmt.Fprint(os.Stdout, "not json")
	case "oversized":
		_, _ = fmt.Fprint(os.Stdout, strings.Repeat("x", 1024))
	case "slow":
		time.Sleep(time.Second)
	}
	os.Exit(0)
}
