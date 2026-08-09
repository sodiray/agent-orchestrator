package remotedaemonhttp

import (
	"context"
	"errors"
	"strings"
)

func UnavailabilityReason(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "remote host did not respond before the timeout"
	}
	if strings.Contains(strings.ToLower(err.Error()), "connection refused") {
		return "remote daemon is not listening (connection refused)"
	}
	return err.Error()
}
