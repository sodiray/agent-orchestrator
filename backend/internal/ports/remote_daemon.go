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

// RemoteNotificationListPage is one owner-daemon notification page. Notification
// IDs and session IDs are deliberately bare here; federation qualifies both at
// the local daemon boundary.
type RemoteNotificationListPage struct {
	Notifications   []domain.NotificationRecord
	NextCursor      string
	UnreadCount     int
	UnresolvedCount int
}

// RemoteDaemonNotificationLister reads an owning daemon's notification view.
// The filter mirrors the public notification-list query without importing an
// HTTP controller into the federation boundary.
type RemoteDaemonNotificationLister interface {
	ListNotifications(ctx context.Context, address string, status domain.NotificationListStatus, limit int, cursor string) (RemoteNotificationListPage, error)
}
