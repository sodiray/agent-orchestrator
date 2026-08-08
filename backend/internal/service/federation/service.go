// Package federation aggregates sessions owned by registered remote daemons
// while preserving each owner's read model and display status.
package federation

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	notificationsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/notification"
	sessionsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/session"
)

const DefaultListTimeout = 2 * time.Second

type LocalSessions interface {
	List(ctx context.Context, filter sessionsvc.ListFilter) ([]domain.Session, error)
}

type LocalNotifications interface {
	List(ctx context.Context, filter notificationsvc.ListFilter) (notificationsvc.ListPage, error)
}

type RemoteHostStore interface {
	ListRemoteHosts(ctx context.Context) ([]domain.RemoteHost, error)
	GetRemoteHost(ctx context.Context, id domain.RemoteHostID) (domain.RemoteHost, bool, error)
	ReplaceRemoteSessionSnapshots(ctx context.Context, id domain.RemoteHostID, snapshots []domain.RemoteSessionSnapshot) error
	ListRemoteSessionSnapshots(ctx context.Context, id domain.RemoteHostID) ([]domain.RemoteSessionSnapshot, error)
}

// HostPresence keeps the local-only session-list path free of even a registry
// read when no remote host has been registered.
type HostPresence interface {
	HasRegisteredHosts() bool
}

type Deps struct {
	Local              LocalSessions
	Store              RemoteHostStore
	Presence           HostPresence
	Client             ports.RemoteDaemonSessionLister
	Notifications      LocalNotifications
	NotificationClient ports.RemoteDaemonNotificationLister
	Timeout            time.Duration
	Clock              func() time.Time
	Logger             *slog.Logger
}

type Service struct {
	local              LocalSessions
	store              RemoteHostStore
	presence           HostPresence
	client             ports.RemoteDaemonSessionLister
	notifications      LocalNotifications
	notificationClient ports.RemoteDaemonNotificationLister
	timeout            time.Duration
	clock              func() time.Time
	log                *slog.Logger
}

type ListedSession struct {
	Local  *domain.Session
	Remote *RemoteSession
}

type RemoteSession struct {
	HostID            domain.RemoteHostID
	SessionID         domain.SessionID
	View              []byte
	Available         bool
	UnavailableReason string
}

func New(deps Deps) *Service {
	timeout := deps.Timeout
	if timeout <= 0 {
		timeout = DefaultListTimeout
	}
	clock := deps.Clock
	if clock == nil {
		clock = time.Now
	}
	log := deps.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Service{
		local:              deps.Local,
		store:              deps.Store,
		presence:           deps.Presence,
		client:             deps.Client,
		notifications:      deps.Notifications,
		notificationClient: deps.NotificationClient,
		timeout:            timeout,
		clock:              clock,
		log:                log,
	}
}

// RemoteNotificationFailure names a host whose notification list could not be
// read. It is returned with the usable rows so the dashboard never mistakes a
// partial result for an all-clear.
type RemoteNotificationFailure struct {
	HostID domain.RemoteHostID
	Reason string
}

// NotificationListPage is the federation-aware notification response. It
// preserves the local service DTO while adding explicit remote failure state.
type NotificationListPage struct {
	Notifications   []notificationsvc.Notification
	NextCursor      string
	UnreadCount     int
	UnresolvedCount int
	RemoteFailures  []RemoteNotificationFailure
}

type listedNotificationHost struct {
	page    ports.RemoteNotificationListPage
	failure *RemoteNotificationFailure
}

// ListNotifications returns local notifications plus concurrently-read remote
// owner views. A remote failure is visible in RemoteFailures, never converted
// into an apparently empty notification list.
func (s *Service) ListNotifications(ctx context.Context, filter notificationsvc.ListFilter) (NotificationListPage, error) {
	if s.notifications == nil {
		return NotificationListPage{}, fmt.Errorf("local notification service is unavailable")
	}
	local, err := s.notifications.List(ctx, filter)
	if err != nil {
		return NotificationListPage{}, err
	}
	page := NotificationListPage{
		Notifications:   append([]notificationsvc.Notification(nil), local.Notifications...),
		NextCursor:      local.NextCursor,
		UnreadCount:     local.UnreadCount,
		UnresolvedCount: local.UnresolvedCount,
	}
	if s.presence == nil || !s.presence.HasRegisteredHosts() {
		return page, nil
	}
	hosts, err := s.store.ListRemoteHosts(ctx)
	if err != nil {
		return NotificationListPage{}, fmt.Errorf("list registered remote hosts: %w", err)
	}
	remote := make([]listedNotificationHost, len(hosts))
	var group sync.WaitGroup
	for index, host := range hosts {
		group.Add(1)
		go func(index int, host domain.RemoteHost) {
			defer group.Done()
			remote[index] = s.listNotificationHost(ctx, host, filter)
		}(index, host)
	}
	group.Wait()
	for index, host := range hosts {
		result := remote[index]
		if result.failure != nil {
			page.RemoteFailures = append(page.RemoteFailures, *result.failure)
			continue
		}
		page.UnreadCount += result.page.UnreadCount
		page.UnresolvedCount += result.page.UnresolvedCount
		for _, record := range result.page.Notifications {
			page.Notifications = append(page.Notifications, qualifiedRemoteNotification(host.HostID, record))
		}
		// A multi-owner page has no safe single-owner cursor. Keeping the
		// cursor empty is explicit: it never points a later request at only one
		// daemon and silently drops the others.
		if result.page.NextCursor != "" {
			page.NextCursor = ""
		}
	}
	sort.SliceStable(page.Notifications, func(left, right int) bool {
		if page.Notifications[left].CreatedAt.Equal(page.Notifications[right].CreatedAt) {
			return page.Notifications[left].ID > page.Notifications[right].ID
		}
		return page.Notifications[left].CreatedAt.After(page.Notifications[right].CreatedAt)
	})
	if len(page.Notifications) > filter.Limit && filter.Limit > 0 {
		page.Notifications = page.Notifications[:filter.Limit]
		page.NextCursor = ""
	}
	return page, nil
}

func (s *Service) listNotificationHost(ctx context.Context, host domain.RemoteHost, filter notificationsvc.ListFilter) listedNotificationHost {
	failure := func(reason string) listedNotificationHost {
		return listedNotificationHost{failure: &RemoteNotificationFailure{HostID: host.HostID, Reason: reason}}
	}
	if host.OperatorState == domain.RemoteHostStateStopped || host.OperatorState == domain.RemoteHostStateDestroyed {
		return failure(fmt.Sprintf("remote host is %s", host.OperatorState))
	}
	if s.notificationClient == nil {
		return failure("remote notification client is unavailable")
	}
	listCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	page, err := s.notificationClient.ListNotifications(listCtx, host.Address, filter.Status, filter.Limit, filter.Cursor)
	if err != nil {
		s.log.Warn("remote notification list failed", "hostId", host.HostID, "address", host.Address, "err", err)
		return failure(err.Error())
	}
	return listedNotificationHost{page: page}
}

func qualifiedRemoteNotification(hostID domain.RemoteHostID, record domain.NotificationRecord) notificationsvc.Notification {
	record.ID = domain.QualifyNotificationID(hostID, record.ID)
	record.SessionID = domain.QualifySessionID(hostID, record.SessionID)
	return notificationsvc.Notification{
		NotificationRecord: record,
		Target: notificationsvc.Target{
			Kind:      notificationTargetKind(record),
			SessionID: record.SessionID,
			PRURL:     record.PRURL,
		},
	}
}

func notificationTargetKind(record domain.NotificationRecord) notificationsvc.TargetKind {
	if record.PRURL != "" {
		return notificationsvc.TargetPR
	}
	return notificationsvc.TargetSession
}

// List returns local sessions unchanged and adds remote sessions using their
// owner's view. A remote-host failure becomes unavailable session entries from
// the durable last-known snapshots; it never fails the local board.
func (s *Service) List(ctx context.Context, filter sessionsvc.ListFilter) ([]ListedSession, error) {
	local, err := s.local.List(ctx, filter)
	if err != nil {
		return nil, err
	}
	out := localListedSessions(local)
	if s.presence == nil || !s.presence.HasRegisteredHosts() {
		return out, nil
	}
	hosts, err := s.store.ListRemoteHosts(ctx)
	if err != nil {
		return nil, fmt.Errorf("list registered remote hosts: %w", err)
	}
	remote := make([][]ListedSession, len(hosts))
	var group sync.WaitGroup
	for index, host := range hosts {
		group.Add(1)
		go func(index int, host domain.RemoteHost) {
			defer group.Done()
			remote[index] = s.listHost(ctx, host, filter)
		}(index, host)
	}
	group.Wait()
	for _, sessions := range remote {
		out = append(out, sessions...)
	}
	return out, nil
}

func (s *Service) Resolve(ctx context.Context, id domain.RemoteHostID) (domain.RemoteHost, bool, error) {
	return s.store.GetRemoteHost(ctx, id)
}

// HasRegisteredHosts keeps streaming surfaces on the local-only path when the
// registry is empty. It deliberately consults the in-memory presence counter,
// not SQLite.
func (s *Service) HasRegisteredHosts() bool {
	return s != nil && s.presence != nil && s.presence.HasRegisteredHosts()
}

// RemoteHosts returns the current registered hosts for a federation stream.
// Callers must check HasRegisteredHosts first so an empty registry remains a
// strict no-op.
func (s *Service) RemoteHosts(ctx context.Context) ([]domain.RemoteHost, error) {
	return s.store.ListRemoteHosts(ctx)
}

func (s *Service) listHost(ctx context.Context, host domain.RemoteHost, filter sessionsvc.ListFilter) []ListedSession {
	if host.OperatorState == domain.RemoteHostStateStopped || host.OperatorState == domain.RemoteHostStateDestroyed {
		reason := fmt.Sprintf("remote host is %s", host.OperatorState)
		s.log.Warn("remote session list unavailable", "hostId", host.HostID, "reason", reason)
		return s.unavailableSnapshots(ctx, host, reason)
	}
	if s.client == nil {
		reason := "remote session client is unavailable"
		s.log.Error("remote session list unavailable", "hostId", host.HostID, "reason", reason)
		return s.unavailableSnapshots(ctx, host, reason)
	}
	listCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	snapshots, err := s.client.ListSessions(listCtx, host.Address, remoteSessionFilter(filter))
	if err != nil {
		reason := err.Error()
		s.log.Warn("remote session list failed", "hostId", host.HostID, "address", host.Address, "err", err)
		return s.unavailableSnapshots(ctx, host, reason)
	}
	observedAt := s.clock().UTC()
	for index := range snapshots {
		snapshots[index].HostID = host.HostID
		snapshots[index].ObservedAt = observedAt
	}
	if err := s.store.ReplaceRemoteSessionSnapshots(ctx, host.HostID, snapshots); err != nil {
		s.log.Error("store remote session snapshots failed", "hostId", host.HostID, "err", err)
	}
	return availableSnapshots(host.HostID, snapshots)
}

func (s *Service) unavailableSnapshots(ctx context.Context, host domain.RemoteHost, reason string) []ListedSession {
	snapshots, err := s.store.ListRemoteSessionSnapshots(ctx, host.HostID)
	if err != nil {
		s.log.Error("load unavailable remote session snapshots failed", "hostId", host.HostID, "err", err)
		return nil
	}
	return unavailableSnapshotSessions(host.HostID, snapshots, reason)
}

func localListedSessions(sessions []domain.Session) []ListedSession {
	out := make([]ListedSession, 0, len(sessions))
	for index := range sessions {
		out = append(out, ListedSession{Local: &sessions[index]})
	}
	return out
}

func availableSnapshots(hostID domain.RemoteHostID, snapshots []domain.RemoteSessionSnapshot) []ListedSession {
	out := make([]ListedSession, 0, len(snapshots))
	for _, snapshot := range snapshots {
		out = append(out, ListedSession{Remote: &RemoteSession{
			HostID:    hostID,
			SessionID: snapshot.SessionID,
			View:      snapshot.View,
			Available: true,
		}})
	}
	return out
}

func unavailableSnapshotSessions(hostID domain.RemoteHostID, snapshots []domain.RemoteSessionSnapshot, reason string) []ListedSession {
	out := make([]ListedSession, 0, len(snapshots))
	for _, snapshot := range snapshots {
		out = append(out, ListedSession{Remote: &RemoteSession{
			HostID:            hostID,
			SessionID:         snapshot.SessionID,
			View:              snapshot.View,
			UnavailableReason: reason,
		}})
	}
	return out
}

func remoteSessionFilter(filter sessionsvc.ListFilter) ports.RemoteSessionListFilter {
	return ports.RemoteSessionListFilter{
		Project:          filter.ProjectID,
		Active:           filter.Active,
		OrchestratorOnly: filter.OrchestratorOnly,
		Fresh:            filter.Fresh,
	}
}
