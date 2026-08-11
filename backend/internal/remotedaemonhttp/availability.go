package remotedaemonhttp

import (
	"context"
	"errors"
	"strings"
)

// UnavailabilityReason turns transport failures into stable, operator-readable
// remote-host status while preserving unexpected errors for diagnosis.
func UnavailabilityReason(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "remote host did not respond before the timeout"
	}
	if strings.Contains(strings.ToLower(err.Error()), "connection refused") {
		return "remote daemon is not listening (connection refused)"
	}
	return err.Error()
}
