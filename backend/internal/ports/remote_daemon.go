package ports

import (
	"context"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// RemoteDaemonProber checks whether an address serves the expected daemon.
type RemoteDaemonProber interface {
	Probe(ctx context.Context, address string) error
}

// RemoteSessionListFilter is the subset of the local session-list query that
// the federation boundary forwards to an owning remote daemon.
type RemoteSessionListFilter struct {
	Project          domain.ProjectID
	Active           *bool
	OrchestratorOnly bool
	Fresh            bool
}

// RemoteDaemonSessionLister reads remote-owned session views without deriving
// or persisting their display state locally.
type RemoteDaemonSessionLister interface {
	ListSessions(ctx context.Context, address string, filter RemoteSessionListFilter) ([]domain.RemoteSessionSnapshot, error)
}
