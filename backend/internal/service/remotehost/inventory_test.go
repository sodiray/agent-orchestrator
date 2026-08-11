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
		// The helper is this test binary re-executed as a subprocess. Under -race
		// that costs a meaningful fraction of a second to start, so a 1s budget
		// here was racing process startup rather than exercising anything: the
		// malformed case timed out and reported a TIMEOUT error, which is not
		// the error it asserts. These two cases are about decoding and size
		// limits, so their timeout is incidental and should be far out of reach.
		{name: "malformed", mode: "malformed", timeout: 30 * time.Second, maxOutput: 1024, want: "decode inventory JSON"},
		{name: "oversized", mode: "oversized", timeout: 30 * time.Second, maxOutput: 64, want: "output exceeds 64 bytes"},
		// The slow case is the one where the timeout IS the subject, so it keeps
		// a short one.
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
