package federation

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	sessionsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/session"
)

type fakeLocalSessions struct{ sessions []domain.Session }

func (f fakeLocalSessions) List(context.Context, sessionsvc.ListFilter) ([]domain.Session, error) {
	return f.sessions, nil
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
	if got := store.snapshots[host.HostID]; len(got) != 1 || got[0].ObservedAt.IsZero() {
		t.Fatalf("snapshots = %#v", got)
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
