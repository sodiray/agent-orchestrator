package remotehost

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	federationsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/federation"
	sessionsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/session"
)

type memoryStore struct {
	mu        sync.Mutex
	hosts     map[domain.RemoteHostID]domain.RemoteHost
	snapshots map[domain.RemoteHostID][]domain.RemoteSessionSnapshot
	listCalls atomic.Int64
}

func (s *memoryStore) CreateRemoteHost(_ context.Context, host domain.RemoteHost) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.hosts[host.HostID]; exists {
		return false, nil
	}
	s.hosts[host.HostID] = host
	return true, nil
}

func (s *memoryStore) ListRemoteHosts(_ context.Context) ([]domain.RemoteHost, error) {
	s.listCalls.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	hosts := make([]domain.RemoteHost, 0, len(s.hosts))
	for _, host := range s.hosts {
		hosts = append(hosts, host)
	}
	return hosts, nil
}

func (s *memoryStore) GetRemoteHost(_ context.Context, id domain.RemoteHostID) (domain.RemoteHost, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	host, found := s.hosts[id]
	return host, found, nil
}

func (s *memoryStore) RecordRemoteHostProbe(_ context.Context, id domain.RemoteHostID, at time.Time, succeeded bool, reason string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	host, found := s.hosts[id]
	if !found {
		return false, nil
	}
	host.LastProbeAt = at
	host.LastProbeSucceeded = succeeded
	host.LastProbeError = reason
	host.UpdatedAt = at
	s.hosts[id] = host
	return true, nil
}

func (s *memoryStore) SetRemoteHostOperatorState(_ context.Context, id domain.RemoteHostID, state domain.RemoteHostState, at time.Time) (domain.RemoteHost, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	host, found := s.hosts[id]
	if !found {
		return domain.RemoteHost{}, false, nil
	}
	host.OperatorState = state
	host.UpdatedAt = at
	s.hosts[id] = host
	return host, true, nil
}

func (s *memoryStore) DeleteRemoteHost(_ context.Context, id domain.RemoteHostID) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, found := s.hosts[id]; !found {
		return false, nil
	}
	delete(s.hosts, id)
	delete(s.snapshots, id)
	return true, nil
}

func (s *memoryStore) ReplaceRemoteSessionSnapshots(_ context.Context, id domain.RemoteHostID, snapshots []domain.RemoteSessionSnapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapshots == nil {
		s.snapshots = map[domain.RemoteHostID][]domain.RemoteSessionSnapshot{}
	}
	s.snapshots[id] = append([]domain.RemoteSessionSnapshot(nil), snapshots...)
	return nil
}

func (s *memoryStore) ListRemoteSessionSnapshots(_ context.Context, id domain.RemoteHostID) ([]domain.RemoteSessionSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]domain.RemoteSessionSnapshot(nil), s.snapshots[id]...), nil
}

type concurrentProber struct {
	slowStarted chan struct{}
	fastStarted chan struct{}
	release     chan struct{}
	slowOnce    sync.Once
	fastOnce    sync.Once
}

func (p *concurrentProber) Probe(ctx context.Context, address string) error {
	if address == "slow:3001" {
		p.slowOnce.Do(func() { close(p.slowStarted) })
		select {
		case <-p.release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	p.fastOnce.Do(func() { close(p.fastStarted) })
	return nil
}

type successfulProber struct {
	calls atomic.Int64
}

func (p *successfulProber) Probe(context.Context, string) error {
	p.calls.Add(1)
	return nil
}

type blockingProber struct {
	started  chan struct{}
	finished chan struct{}
	once     sync.Once
}

func (p *blockingProber) Probe(ctx context.Context, _ string) error {
	p.once.Do(func() { close(p.started) })
	<-ctx.Done()
	close(p.finished)
	return ctx.Err()
}

type failingProber struct {
	calls int
}

type switchableProber struct {
	mu  sync.Mutex
	err error
}

func (p *switchableProber) Probe(context.Context, string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

func (p *switchableProber) setError(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.err = err
}

type snapshotLister struct {
	mu        sync.Mutex
	snapshots []domain.RemoteSessionSnapshot
	err       error
	calls     atomic.Int64
}

func (l *snapshotLister) ListSessions(_ context.Context, _ string, filter ports.RemoteSessionListFilter) ([]domain.RemoteSessionSnapshot, error) {
	if filter != (ports.RemoteSessionListFilter{}) {
		return nil, errors.New("snapshot refresh applied a board filter")
	}
	l.calls.Add(1)
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return nil, l.err
	}
	return append([]domain.RemoteSessionSnapshot(nil), l.snapshots...), nil
}

func (l *snapshotLister) setSnapshots(snapshots []domain.RemoteSessionSnapshot) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.snapshots = append([]domain.RemoteSessionSnapshot(nil), snapshots...)
}

func (l *snapshotLister) setError(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.err = err
}

type concurrentSnapshotLister struct {
	slowStarted chan struct{}
	fastStarted chan struct{}
	release     chan struct{}
	slowOnce    sync.Once
	fastOnce    sync.Once
}

func (l *concurrentSnapshotLister) ListSessions(ctx context.Context, address string, _ ports.RemoteSessionListFilter) ([]domain.RemoteSessionSnapshot, error) {
	if address == "slow:3001" {
		l.slowOnce.Do(func() { close(l.slowStarted) })
		select {
		case <-l.release:
			return []domain.RemoteSessionSnapshot{{SessionID: "slow-1", View: testRemoteView("slow-1")}}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	l.fastOnce.Do(func() { close(l.fastStarted) })
	return []domain.RemoteSessionSnapshot{{SessionID: "fast-1", View: testRemoteView("fast-1")}}, nil
}

func testRemoteView(id string) []byte {
	return []byte(`{"id":"` + id + `","projectId":"remote","kind":"worker","status":"idle","prs":[]}`)
}

type remoteHostTestLocalSessions struct{}

func (remoteHostTestLocalSessions) List(context.Context, sessionsvc.ListFilter) ([]domain.Session, error) {
	return nil, nil
}

func (p *failingProber) Probe(context.Context, string) error {
	p.calls++
	return errors.New("connection refused")
}

func TestFailedProbeOnlyProducesUnreachable(t *testing.T) {
	now := time.Date(2026, 8, 7, 0, 0, 0, 0, time.UTC)
	store := &memoryStore{hosts: map[domain.RemoteHostID]domain.RemoteHost{}}
	prober := &failingProber{}
	svc := New(Deps{Store: store, Prober: prober, Clock: func() time.Time { return now }})

	host, err := svc.Register(context.Background(), RegisterInput{HostID: "lab-host", Address: "127.0.0.1:3001"})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if host.State != domain.RemoteHostStateUnreachable {
		t.Fatalf("state = %q, want unreachable", host.State)
	}
	if host.LastProbeError != "connection refused" {
		t.Fatalf("last probe error = %q", host.LastProbeError)
	}
}

func TestHealthProbeDoesNotClearOperatorState(t *testing.T) {
	now := time.Date(2026, 8, 7, 0, 0, 0, 0, time.UTC)
	store := &memoryStore{hosts: map[domain.RemoteHostID]domain.RemoteHost{
		"lab-host": {HostID: "lab-host", Address: "127.0.0.1:3001", OperatorState: domain.RemoteHostStateStopped},
	}}
	prober := &failingProber{}
	svc := New(Deps{Store: store, Prober: prober, Clock: func() time.Time { return now }})
	if err := svc.LoadPresence(context.Background()); err != nil {
		t.Fatalf("load presence: %v", err)
	}

	svc.probeAll(context.Background())
	host, found, err := store.GetRemoteHost(context.Background(), "lab-host")
	if err != nil || !found {
		t.Fatalf("get host: found=%v err=%v", found, err)
	}
	if host.OperatorState != domain.RemoteHostStateStopped || host.CurrentState() != domain.RemoteHostStateStopped {
		t.Fatalf("state = %q/%q, want stopped", host.OperatorState, host.CurrentState())
	}
	if prober.calls != 0 {
		t.Fatalf("probe calls = %d, want 0", prober.calls)
	}
}

func TestRegisterProbeSnapshotsSessionsBeforeAnyBoardList(t *testing.T) {
	store := &memoryStore{hosts: map[domain.RemoteHostID]domain.RemoteHost{}, snapshots: map[domain.RemoteHostID][]domain.RemoteSessionSnapshot{}}
	prober := &switchableProber{}
	lister := &snapshotLister{snapshots: []domain.RemoteSessionSnapshot{{SessionID: "remote-7", View: testRemoteView("remote-7")}}}
	svc := New(Deps{Store: store, Prober: prober, SessionLister: lister})
	if _, err := svc.Register(context.Background(), RegisterInput{HostID: "lab-host", Address: "127.0.0.1:3001"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if got := lister.calls.Load(); got != 1 {
		t.Fatalf("snapshot refresh calls after registration = %d, want 1", got)
	}
	prober.setError(errors.New("connection refused"))
	lister.setError(errors.New("connection refused"))
	svc.probeAll(context.Background())
	board := federationsvc.New(federationsvc.Deps{
		Local:    remoteHostTestLocalSessions{},
		Store:    store,
		Presence: svc,
		Client:   lister,
	})
	sessions, err := board.List(context.Background(), sessionsvc.ListFilter{})
	if err != nil {
		t.Fatalf("board list: %v", err)
	}
	if len(sessions) != 1 || sessions[0].Remote == nil || sessions[0].Remote.Available || sessions[0].Remote.UnavailableReason != "connection refused" {
		t.Fatalf("board sessions = %#v", sessions)
	}
}

func TestSuccessfulProbeRefreshesSnapshots(t *testing.T) {
	store := &memoryStore{hosts: map[domain.RemoteHostID]domain.RemoteHost{}, snapshots: map[domain.RemoteHostID][]domain.RemoteSessionSnapshot{}}
	lister := &snapshotLister{snapshots: []domain.RemoteSessionSnapshot{{SessionID: "remote-1", View: testRemoteView("remote-1")}}}
	svc := New(Deps{Store: store, Prober: &successfulProber{}, SessionLister: lister})
	if _, err := svc.Register(context.Background(), RegisterInput{HostID: "lab-host", Address: "127.0.0.1:3001"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	lister.setSnapshots([]domain.RemoteSessionSnapshot{{SessionID: "remote-2", View: testRemoteView("remote-2")}})
	svc.probeAll(context.Background())
	snapshots, err := store.ListRemoteSessionSnapshots(context.Background(), "lab-host")
	if err != nil {
		t.Fatalf("list snapshots: %v", err)
	}
	if len(snapshots) != 1 || snapshots[0].SessionID != "remote-2" || snapshots[0].ObservedAt.IsZero() {
		t.Fatalf("snapshots = %#v", snapshots)
	}
	if got := lister.calls.Load(); got != 2 {
		t.Fatalf("snapshot refresh calls = %d, want 2", got)
	}
}

func TestProbeAllRunsSlowAndFastHostsConcurrently(t *testing.T) {
	store := &memoryStore{hosts: map[domain.RemoteHostID]domain.RemoteHost{
		"slow-host": {HostID: "slow-host", Address: "slow:3001"},
		"fast-host": {HostID: "fast-host", Address: "fast:3001"},
	}}
	prober := &concurrentProber{slowStarted: make(chan struct{}), fastStarted: make(chan struct{}), release: make(chan struct{})}
	svc := New(Deps{Store: store, Prober: prober, SessionLister: &snapshotLister{}, ProbeTimeout: time.Second})
	if err := svc.LoadPresence(context.Background()); err != nil {
		t.Fatalf("load presence: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.probeAll(context.Background())
	}()
	select {
	case <-prober.slowStarted:
	case <-time.After(time.Second):
		t.Fatal("slow probe did not start")
	}
	select {
	case <-prober.fastStarted:
	case <-time.After(time.Second):
		t.Fatal("fast probe waited for slow probe")
	}
	close(prober.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("concurrent probes did not finish")
	}
}

func TestProbeAllRefreshesSnapshotsWithoutSlowHostBlockingAnother(t *testing.T) {
	store := &memoryStore{hosts: map[domain.RemoteHostID]domain.RemoteHost{
		"slow-host": {HostID: "slow-host", Address: "slow:3001"},
		"fast-host": {HostID: "fast-host", Address: "fast:3001"},
	}, snapshots: map[domain.RemoteHostID][]domain.RemoteSessionSnapshot{}}
	lister := &concurrentSnapshotLister{slowStarted: make(chan struct{}), fastStarted: make(chan struct{}), release: make(chan struct{})}
	svc := New(Deps{Store: store, Prober: &successfulProber{}, SessionLister: lister, ProbeTimeout: time.Second})
	if err := svc.LoadPresence(context.Background()); err != nil {
		t.Fatalf("load presence: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.probeAll(context.Background())
	}()
	select {
	case <-lister.slowStarted:
	case <-time.After(time.Second):
		t.Fatal("slow snapshot refresh did not start")
	}
	select {
	case <-lister.fastStarted:
	case <-time.After(time.Second):
		t.Fatal("fast snapshot refresh waited for slow host")
	}
	close(lister.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("concurrent snapshot refreshes did not finish")
	}
}

func TestHealthProbeWorkerStartsAndStopsWithRegisteredHosts(t *testing.T) {
	store := &memoryStore{hosts: map[domain.RemoteHostID]domain.RemoteHost{}}
	lister := &snapshotLister{}
	svc := New(Deps{Store: store, Prober: &successfulProber{}, SessionLister: lister})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	svc.RunHealthProbes(ctx, 10*time.Millisecond)
	if got := store.listCalls.Load(); got != 0 {
		t.Fatalf("registry reads = %d, want 0 with no hosts", got)
	}
	svc.probeMu.Lock()
	worker := svc.probeWorker
	svc.probeMu.Unlock()
	if worker != nil {
		t.Fatal("zero-host daemon started a health-probe worker")
	}
	if got := lister.calls.Load(); got != 0 {
		t.Fatalf("snapshot refresh calls = %d, want 0 with no hosts", got)
	}
	if _, err := svc.Register(context.Background(), RegisterInput{HostID: "lab-host", Address: "127.0.0.1:3001"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	waitForRemoteHostTest(t, time.Second, func() bool { return store.listCalls.Load() > 0 })
	if err := svc.Deregister(context.Background(), "lab-host"); err != nil {
		t.Fatalf("deregister: %v", err)
	}
	readsAfterStop := store.listCalls.Load()
	time.Sleep(30 * time.Millisecond)
	if got := store.listCalls.Load(); got != readsAfterStop {
		t.Fatalf("registry reads after last host removal = %d, want %d", got, readsAfterStop)
	}
	svc.probeMu.Lock()
	worker = svc.probeWorker
	svc.probeMu.Unlock()
	if worker != nil {
		t.Fatal("last-host removal left a health-probe worker running")
	}
}

func TestDeregisterWaitsForInFlightHealthProbe(t *testing.T) {
	store := &memoryStore{hosts: map[domain.RemoteHostID]domain.RemoteHost{
		"lab-host": {HostID: "lab-host", Address: "127.0.0.1:3001"},
	}}
	prober := &blockingProber{started: make(chan struct{}), finished: make(chan struct{})}
	svc := New(Deps{Store: store, Prober: prober, ProbeTimeout: time.Second})
	if err := svc.LoadPresence(context.Background()); err != nil {
		t.Fatalf("load presence: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	svc.RunHealthProbes(ctx, time.Hour)
	select {
	case <-prober.started:
	case <-time.After(time.Second):
		t.Fatal("health probe did not start")
	}
	if err := svc.Deregister(context.Background(), "lab-host"); err != nil {
		t.Fatalf("deregister: %v", err)
	}
	select {
	case <-prober.finished:
	case <-time.After(time.Second):
		t.Fatal("deregister returned before the in-flight probe stopped")
	}
}

func waitForRemoteHostTest(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("timed out waiting for condition")
		case <-ticker.C:
		}
	}
}
