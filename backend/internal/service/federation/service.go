// Package federation aggregates sessions owned by registered remote daemons
// while preserving each owner's read model and display status.
package federation

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/remotedaemonhttp"
	notificationsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/notification"
	projectsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/project"
	sessionsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/session"
)

const (
	DefaultListTimeout    = 2 * time.Second
	remoteHostListWorkers = 8
)

type LocalSessions interface {
	List(ctx context.Context, filter sessionsvc.ListFilter) ([]domain.Session, error)
}

type LocalNotifications interface {
	List(ctx context.Context, filter notificationsvc.ListFilter) (notificationsvc.ListPage, error)
}

type LocalProjects interface {
	List(ctx context.Context) ([]projectsvc.Summary, error)
}

type RemoteHostStore interface {
	ListRemoteHosts(ctx context.Context) ([]domain.RemoteHost, error)
	GetRemoteHost(ctx context.Context, id domain.RemoteHostID) (domain.RemoteHost, bool, error)
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
	Projects           LocalProjects
	ProjectClient      ports.RemoteDaemonProjectLister
	Notifications      LocalNotifications
	NotificationClient ports.RemoteDaemonNotificationLister
	Timeout            time.Duration
	Logger             *slog.Logger
}

type Service struct {
	local              LocalSessions
	store              RemoteHostStore
	presence           HostPresence
	client             ports.RemoteDaemonSessionLister
	projects           LocalProjects
	projectClient      ports.RemoteDaemonProjectLister
	notifications      LocalNotifications
	notificationClient ports.RemoteDaemonNotificationLister
	timeout            time.Duration
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
	log := deps.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Service{
		local:              deps.Local,
		store:              deps.Store,
		presence:           deps.Presence,
		client:             deps.Client,
		projects:           deps.Projects,
		projectClient:      deps.ProjectClient,
		notifications:      deps.Notifications,
		notificationClient: deps.NotificationClient,
		timeout:            timeout,
		log:                log,
	}
}

type listedProjectHost struct {
	host     domain.RemoteHost
	projects []ports.RemoteProjectSummary
	reason   string
}

// ListProjects merges project rows by project id for the board. Project ids are
// an explicit display grouping key only; all session routing remains based on
// the host-qualified session id exposed by List.
func (s *Service) ListProjects(ctx context.Context) ([]projectsvc.Summary, error) {
	if s.projects == nil {
		return nil, fmt.Errorf("local project service is unavailable")
	}
	local, err := s.projects.List(ctx)
	if err != nil {
		return nil, err
	}
	if s.presence == nil || !s.presence.HasRegisteredHosts() {
		return local, nil
	}
	hosts, err := s.store.ListRemoteHosts(ctx)
	if err != nil {
		return nil, fmt.Errorf("list registered remote hosts: %w", err)
	}
	remote := make([]listedProjectHost, len(hosts))
	forEachRemoteHost(ctx, hosts, func(index int, host domain.RemoteHost) {
		remote[index] = s.listProjectHost(ctx, host)
	})
	return mergeProjects(local, remote), nil
}

func (s *Service) listProjectHost(ctx context.Context, host domain.RemoteHost) listedProjectHost {
	if host.OperatorState == domain.RemoteHostStateStopped || host.OperatorState == domain.RemoteHostStateDestroyed {
		reason := fmt.Sprintf("remote host is %s", host.OperatorState)
		return listedProjectHost{host: host, projects: s.unavailableProjects(ctx, host, reason), reason: reason}
	}
	if s.projectClient == nil {
		reason := "remote project client is unavailable"
		return listedProjectHost{host: host, projects: s.unavailableProjects(ctx, host, reason), reason: reason}
	}
	listCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	projects, err := s.projectClient.ListProjects(listCtx, host.Address)
	if err != nil {
		s.log.Warn("remote project list failed", "hostId", host.HostID, "address", host.Address, "err", err)
		reason := remotedaemonhttp.UnavailabilityReason(err)
		return listedProjectHost{host: host, projects: s.unavailableProjects(ctx, host, reason), reason: reason}
	}
	return listedProjectHost{host: host, projects: projects}
}

type projectCandidate struct {
	summary projectsvc.Summary
	source  projectsvc.Source
	local   bool
}

func mergeProjects(local []projectsvc.Summary, remote []listedProjectHost) []projectsvc.Summary {
	byID := make(map[domain.ProjectID][]projectCandidate, len(local))
	for _, project := range local {
		byID[project.ID] = append(byID[project.ID], projectCandidate{
			summary: project,
			source:  projectsvc.Source{Name: project.Name, Path: project.Path, Kind: string(project.Kind), Available: true},
			local:   true,
		})
	}
	for _, result := range remote {
		for _, project := range result.projects {
			summary := remoteProjectSummary(project)
			byID[summary.ID] = append(byID[summary.ID], projectCandidate{
				summary: summary,
				source:  projectsvc.Source{HostID: string(result.host.HostID), Name: summary.Name, Path: summary.Path, Kind: string(summary.Kind), Available: result.reason == "", UnavailableReason: result.reason},
			})
		}
	}
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	out := make([]projectsvc.Summary, 0, len(ids))
	for _, id := range ids {
		candidates := byID[domain.ProjectID(id)]
		sort.SliceStable(candidates, func(left, right int) bool {
			if candidates[left].local != candidates[right].local {
				return candidates[left].local
			}
			return candidates[left].source.HostID < candidates[right].source.HostID
		})
		chosen := candidates[0].summary
		chosen.Sources = make([]projectsvc.Source, 0, len(candidates))
		for _, candidate := range candidates {
			chosen.Sources = append(chosen.Sources, candidate.source)
		}
		chosen.MetadataConflicts = projectMetadataConflicts(candidates)
		out = append(out, chosen)
	}
	return out
}

func remoteProjectSummary(project ports.RemoteProjectSummary) projectsvc.Summary {
	return projectsvc.Summary{ID: project.ID, Name: project.Name, Path: project.Path, Kind: project.Kind.WithDefault(), SessionPrefix: project.SessionPrefix, OrchestratorAgent: project.OrchestratorAgent, ResolveError: project.ResolveError}
}

func (s *Service) unavailableProjects(ctx context.Context, host domain.RemoteHost, reason string) []ports.RemoteProjectSummary {
	// A remote project row may not be available during an outage. Snapshot views
	// still carry projectId, which is enough to retain a workspace for every
	// cached session instead of allowing it to disappear from the board.
	snapshots, err := s.store.ListRemoteSessionSnapshots(ctx, host.HostID)
	if err != nil {
		s.log.Error("load unavailable remote project snapshots failed", "hostId", host.HostID, "err", err)
		return nil
	}
	ids := make(map[domain.ProjectID]struct{}, len(snapshots))
	for _, snapshot := range snapshots {
		var view struct {
			ProjectID domain.ProjectID `json:"projectId"`
		}
		if err := json.Unmarshal(snapshot.View, &view); err != nil || view.ProjectID == "" {
			continue
		}
		ids[view.ProjectID] = struct{}{}
	}
	projects := make([]ports.RemoteProjectSummary, 0, len(ids))
	for id := range ids {
		projects = append(projects, ports.RemoteProjectSummary{ID: id, Name: string(id), Kind: domain.ProjectKindSingleRepo})
	}
	sort.Slice(projects, func(left, right int) bool { return projects[left].ID < projects[right].ID })
	return projects
}

func projectMetadataConflicts(candidates []projectCandidate) []string {
	fields := []struct {
		name   string
		values []string
	}{
		{name: "name"}, {name: "path"}, {name: "kind"}, {name: "orchestratorAgent"},
	}
	for index := range candidates {
		if !candidates[index].source.Available {
			continue
		}
		fields[0].values = append(fields[0].values, candidates[index].summary.Name)
		fields[1].values = append(fields[1].values, candidates[index].summary.Path)
		fields[2].values = append(fields[2].values, string(candidates[index].summary.Kind))
		fields[3].values = append(fields[3].values, string(candidates[index].summary.OrchestratorAgent))
	}
	conflicts := make([]string, 0, len(fields))
	for _, field := range fields {
		values := make(map[string]struct{}, len(field.values))
		for _, value := range field.values {
			values[value] = struct{}{}
		}
		if len(values) > 1 {
			conflicts = append(conflicts, field.name)
		}
	}
	return conflicts
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
	forEachRemoteHost(ctx, hosts, func(index int, host domain.RemoteHost) {
		remote[index] = s.listNotificationHost(ctx, host, filter)
	})
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
	forEachRemoteHost(ctx, hosts, func(index int, host domain.RemoteHost) {
		remote[index] = s.listHost(ctx, host, filter)
	})
	for _, sessions := range remote {
		out = append(out, sessions...)
	}
	return out, nil
}

// forEachRemoteHost is the shared bounded fan-out for all board aggregation
// reads. Each callback supplies its own per-host request timeout, while this
// pool prevents a large registry from creating unbounded concurrent work.
func forEachRemoteHost(ctx context.Context, hosts []domain.RemoteHost, visit func(index int, host domain.RemoteHost)) {
	workers := min(len(hosts), remoteHostListWorkers)
	if workers == 0 {
		return
	}
	jobs := make(chan int)
	var group sync.WaitGroup
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			for index := range jobs {
				if ctx.Err() != nil {
					return
				}
				visit(index, hosts[index])
			}
		}()
	}
	for index := range hosts {
		select {
		case <-ctx.Done():
			close(jobs)
			group.Wait()
			return
		case jobs <- index:
		}
	}
	close(jobs)
	group.Wait()
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
		reason := remotedaemonhttp.UnavailabilityReason(err)
		s.log.Warn("remote session list failed", "hostId", host.HostID, "address", host.Address, "err", err)
		return s.unavailableSnapshots(ctx, host, reason)
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
