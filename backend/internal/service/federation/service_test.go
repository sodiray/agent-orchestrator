package federation

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	notificationsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/notification"
	sessionsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/session"
)

type fakeLocalSessions struct{ sessions []domain.Session }

func (f fakeLocalSessions) List(context.Context, sessionsvc.ListFilter) ([]domain.Session, error) {
	return f.sessions, nil
}

type fakeLocalNotifications struct {
	page  notificationsvc.ListPage
	calls int
}

func (f *fakeLocalNotifications) List(context.Context, notificationsvc.ListFilter) (notificationsvc.ListPage, error) {
	f.calls++
	return f.page, nil
}

type fakeNotificationClient struct {
	list  func(context.Context, string, domain.NotificationListStatus, int, string) (ports.RemoteNotificationListPage, error)
	calls int
}

func (f *fakeNotificationClient) ListNotifications(ctx context.Context, address string, status domain.NotificationListStatus, limit int, cursor string) (ports.RemoteNotificationListPage, error) {
	f.calls++
	return f.list(ctx, address, status, limit, cursor)
}

type fakePresence struct{ hasHosts bool }

func (f fakePresence) HasRegisteredHosts() bool { return f.hasHosts }

type fakeStore struct {
	mu                 sync.Mutex
	hosts              []domain.RemoteHost
	snapshots          map[domain.RemoteHostID][]domain.RemoteSessionSnapshot
	listHostsCalls     int
	replaceSnapshotErr error
}

func (f *fakeStore) ListRemoteHosts(context.Context) ([]domain.RemoteHost, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listHostsCalls++
	return append([]domain.RemoteHost(nil), f.hosts...), nil
}

func (f *fakeStore) GetRemoteHost(_ context.Context, id domain.RemoteHostID) (domain.RemoteHost, bool, error) {
	for _, host := range f.hosts {
		if host.HostID == id {
			return host, true, nil
		}
	}
	return domain.RemoteHost{}, false, nil
}

func (f *fakeStore) ReplaceRemoteSessionSnapshots(_ context.Context, id domain.RemoteHostID, snapshots []domain.RemoteSessionSnapshot) error {
	if f.replaceSnapshotErr != nil {
		return f.replaceSnapshotErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.snapshots[id] = append([]domain.RemoteSessionSnapshot(nil), snapshots...)
	return nil
}

func (f *fakeStore) ListRemoteSessionSnapshots(_ context.Context, id domain.RemoteHostID) ([]domain.RemoteSessionSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]domain.RemoteSessionSnapshot(nil), f.snapshots[id]...), nil
}

type fakeClient struct {
	list func(context.Context, string, ports.RemoteSessionListFilter) ([]domain.RemoteSessionSnapshot, error)
}

func (f fakeClient) ListSessions(ctx context.Context, address string, filter ports.RemoteSessionListFilter) ([]domain.RemoteSessionSnapshot, error) {
	return f.list(ctx, address, filter)
}

func testLocalSession() domain.Session {
	return domain.Session{SessionRecord: domain.SessionRecord{ID: "local-1", ProjectID: "local", Kind: domain.KindWorker}}
}

func testRemoteView(id string) []byte {
	return []byte(`{"id":"` + id + `","projectId":"remote","kind":"worker","status":"idle","prs":[]}`)
}

func TestListWithoutRemoteHostsIsLocalOnlyWithoutRegistryRead(t *testing.T) {
	store := &fakeStore{}
	svc := New(Deps{Local: fakeLocalSessions{sessions: []domain.Session{testLocalSession()}}, Store: store, Presence: fakePresence{}})
	sessions, err := svc.List(context.Background(), sessionsvc.ListFilter{})
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(sessions) != 1 || sessions[0].Local == nil || sessions[0].Local.ID != "local-1" {
		t.Fatalf("sessions = %#v", sessions)
	}
	if store.listHostsCalls != 0 {
		t.Fatalf("ListRemoteHosts calls = %d, want 0", store.listHostsCalls)
	}
}

func TestListAddsReachableRemoteSessions(t *testing.T) {
	host := domain.RemoteHost{HostID: "workstation", Address: "127.0.0.1:3001"}
	store := &fakeStore{hosts: []domain.RemoteHost{host}, snapshots: map[domain.RemoteHostID][]domain.RemoteSessionSnapshot{}}
	svc := New(Deps{
		Local:    fakeLocalSessions{sessions: []domain.Session{testLocalSession()}},
		Store:    store,
		Presence: fakePresence{hasHosts: true},
		Client: fakeClient{list: func(_ context.Context, address string, _ ports.RemoteSessionListFilter) ([]domain.RemoteSessionSnapshot, error) {
			if address != host.Address {
				t.Fatalf("address = %q", address)
			}
			return []domain.RemoteSessionSnapshot{{SessionID: "remote-7", View: testRemoteView("remote-7")}}, nil
		}},
	})
	sessions, err := svc.List(context.Background(), sessionsvc.ListFilter{})
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(sessions) != 2 || sessions[1].Remote == nil {
		t.Fatalf("sessions = %#v", sessions)
	}
	remote := sessions[1].Remote
	if remote.HostID != host.HostID || remote.SessionID != "remote-7" || !remote.Available {
		t.Fatalf("remote = %#v", remote)
	}
	if got := store.snapshots[host.HostID]; len(got) != 0 {
		t.Fatalf("board list refreshed snapshots = %#v", got)
	}
}

func TestListUsesSnapshotsWhenRemoteHostIsUnreachable(t *testing.T) {
	host := domain.RemoteHost{HostID: "workstation", Address: "127.0.0.1:3001"}
	store := &fakeStore{hosts: []domain.RemoteHost{host}, snapshots: map[domain.RemoteHostID][]domain.RemoteSessionSnapshot{
		host.HostID: {{HostID: host.HostID, SessionID: "remote-7", View: testRemoteView("remote-7")}},
	}}
	svc := New(Deps{
		Local:    fakeLocalSessions{sessions: []domain.Session{testLocalSession()}},
		Store:    store,
		Presence: fakePresence{hasHosts: true},
		Client: fakeClient{list: func(context.Context, string, ports.RemoteSessionListFilter) ([]domain.RemoteSessionSnapshot, error) {
			return nil, errors.New("connection refused")
		}},
	})
	sessions, err := svc.List(context.Background(), sessionsvc.ListFilter{})
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	remote := sessions[1].Remote
	if remote == nil || remote.Available || remote.UnavailableReason != "connection refused" {
		t.Fatalf("remote = %#v", remote)
	}
}

func TestListContinuesWhenOneRemoteHostIsDead(t *testing.T) {
	dead := domain.RemoteHost{HostID: "dead-host", Address: "dead:3001"}
	live := domain.RemoteHost{HostID: "live-host", Address: "live:3001"}
	store := &fakeStore{hosts: []domain.RemoteHost{dead, live}, snapshots: map[domain.RemoteHostID][]domain.RemoteSessionSnapshot{
		dead.HostID: {{HostID: dead.HostID, SessionID: "dead-1", View: testRemoteView("dead-1")}},
	}}
	svc := New(Deps{
		Local:    fakeLocalSessions{sessions: []domain.Session{testLocalSession()}},
		Store:    store,
		Presence: fakePresence{hasHosts: true},
		Client: fakeClient{list: func(_ context.Context, address string, _ ports.RemoteSessionListFilter) ([]domain.RemoteSessionSnapshot, error) {
			if address == dead.Address {
				return nil, errors.New("timeout")
			}
			return []domain.RemoteSessionSnapshot{{SessionID: "live-1", View: testRemoteView("live-1")}}, nil
		}},
	})
	sessions, err := svc.List(context.Background(), sessionsvc.ListFilter{})
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(sessions) != 3 || sessions[1].Remote.Available || !sessions[2].Remote.Available {
		t.Fatalf("sessions = %#v", sessions)
	}
}

func TestListUsesPerHostTimeout(t *testing.T) {
	host := domain.RemoteHost{HostID: "slow-host", Address: "slow:3001"}
	store := &fakeStore{hosts: []domain.RemoteHost{host}, snapshots: map[domain.RemoteHostID][]domain.RemoteSessionSnapshot{}}
	svc := New(Deps{
		Local:    fakeLocalSessions{},
		Store:    store,
		Presence: fakePresence{hasHosts: true},
		Timeout:  20 * time.Millisecond,
		Client: fakeClient{list: func(ctx context.Context, _ string, _ ports.RemoteSessionListFilter) ([]domain.RemoteSessionSnapshot, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}},
	})
	started := time.Now()
	if _, err := svc.List(context.Background(), sessionsvc.ListFilter{}); err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
		t.Fatalf("List() took %s", elapsed)
	}
}

func TestListNotificationsAddsReachableRemoteNotifications(t *testing.T) {
	now := time.Date(2026, 8, 8, 10, 0, 0, 0, time.UTC)
	host := domain.RemoteHost{HostID: "workstation", Address: "127.0.0.1:3001"}
	local := &fakeLocalNotifications{page: notificationsvc.ListPage{Notifications: []notificationsvc.Notification{{
		NotificationRecord: domain.NotificationRecord{ID: "local", SessionID: "local-1", ProjectID: "local", Type: domain.NotificationNeedsInput, Title: "local", Status: domain.NotificationUnread, CreatedAt: now},
		Target:             notificationsvc.Target{Kind: notificationsvc.TargetSession, SessionID: "local-1"},
	}}, UnreadCount: 1, UnresolvedCount: 1}}
	client := &fakeNotificationClient{list: func(_ context.Context, address string, _ domain.NotificationListStatus, _ int, _ string) (ports.RemoteNotificationListPage, error) {
		if address != host.Address {
			t.Fatalf("address = %q", address)
		}
		return ports.RemoteNotificationListPage{Notifications: []domain.NotificationRecord{{ID: "ntf_7", SessionID: "remote-7", ProjectID: "remote", Type: domain.NotificationNeedsInput, Title: "remote", Status: domain.NotificationUnread, CreatedAt: now.Add(time.Second)}}, UnreadCount: 1, UnresolvedCount: 1}, nil
	}}
	svc := New(Deps{Store: &fakeStore{hosts: []domain.RemoteHost{host}}, Presence: fakePresence{hasHosts: true}, Notifications: local, NotificationClient: client})
	page, err := svc.ListNotifications(context.Background(), notificationsvc.ListFilter{Status: notificationsvc.ListUnread, Limit: 100})
	if err != nil {
		t.Fatalf("ListNotifications() error = %v", err)
	}
	if len(page.Notifications) != 2 || page.Notifications[0].ID != "workstation~ntf_7" || page.Notifications[0].SessionID != "workstation~remote-7" || page.Notifications[0].Target.SessionID != "workstation~remote-7" {
		t.Fatalf("notifications = %#v", page.Notifications)
	}
	if page.UnreadCount != 2 || page.UnresolvedCount != 2 || len(page.RemoteFailures) != 0 {
		t.Fatalf("page = %#v", page)
	}
}

func TestListNotificationsSurfacesUnreachableHostReason(t *testing.T) {
	host := domain.RemoteHost{HostID: "dead-host", Address: "dead:3001"}
	local := &fakeLocalNotifications{page: notificationsvc.ListPage{UnreadCount: 1}}
	client := &fakeNotificationClient{list: func(context.Context, string, domain.NotificationListStatus, int, string) (ports.RemoteNotificationListPage, error) {
		return ports.RemoteNotificationListPage{}, errors.New("connection refused")
	}}
	svc := New(Deps{Store: &fakeStore{hosts: []domain.RemoteHost{host}}, Presence: fakePresence{hasHosts: true}, Notifications: local, NotificationClient: client})
	page, err := svc.ListNotifications(context.Background(), notificationsvc.ListFilter{Status: notificationsvc.ListUnread, Limit: 100})
	if err != nil {
		t.Fatalf("ListNotifications() error = %v", err)
	}
	if len(page.RemoteFailures) != 1 || page.RemoteFailures[0].HostID != host.HostID || page.RemoteFailures[0].Reason != "connection refused" || page.UnreadCount != 1 {
		t.Fatalf("page = %#v", page)
	}
}

func TestListNotificationsWithoutRemoteHostsDoesNoRemoteWork(t *testing.T) {
	store := &fakeStore{}
	local := &fakeLocalNotifications{page: notificationsvc.ListPage{UnreadCount: 1}}
	client := &fakeNotificationClient{list: func(context.Context, string, domain.NotificationListStatus, int, string) (ports.RemoteNotificationListPage, error) {
		t.Fatal("remote notification client must not be called")
		return ports.RemoteNotificationListPage{}, nil
	}}
	svc := New(Deps{Store: store, Presence: fakePresence{}, Notifications: local, NotificationClient: client})
	if _, err := svc.ListNotifications(context.Background(), notificationsvc.ListFilter{Status: notificationsvc.ListUnread, Limit: 100}); err != nil {
		t.Fatalf("ListNotifications() error = %v", err)
	}
	if store.listHostsCalls != 0 || client.calls != 0 || local.calls != 1 {
		t.Fatalf("store calls=%d client calls=%d local calls=%d", store.listHostsCalls, client.calls, local.calls)
	}
}
