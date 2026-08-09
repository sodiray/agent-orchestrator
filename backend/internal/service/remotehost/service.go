package remotehost

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/config"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/apierr"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/remotedaemonhttp"
)

const (
	DefaultProbeInterval = 30 * time.Second
	DefaultProbeTimeout  = config.DefaultRemoteHostProbeTimeout

	remoteHostProbeWorkers = 8
	inventoryAbsenceReads  = 2
)

type Store interface {
	CreateRemoteHost(ctx context.Context, host domain.RemoteHost) (bool, error)
	UpsertRemoteHost(ctx context.Context, host domain.RemoteHost) (bool, error)
	ListRemoteHosts(ctx context.Context) ([]domain.RemoteHost, error)
	GetRemoteHost(ctx context.Context, id domain.RemoteHostID) (domain.RemoteHost, bool, error)
	RecordRemoteHostProbe(ctx context.Context, id domain.RemoteHostID, at time.Time, succeeded bool, failureReason string) (bool, error)
	ReplaceRemoteSessionSnapshots(ctx context.Context, id domain.RemoteHostID, snapshots []domain.RemoteSessionSnapshot) error
	SetRemoteHostOperatorState(ctx context.Context, id domain.RemoteHostID, state domain.RemoteHostState, at time.Time) (domain.RemoteHost, bool, error)
	DeleteRemoteHost(ctx context.Context, id domain.RemoteHostID) (bool, error)
}

type Manager interface {
	Register(ctx context.Context, in RegisterInput) (Host, error)
	List(ctx context.Context) ([]Host, error)
	Inventory(ctx context.Context) (Inventory, error)
	HasHostInventory() bool
	Get(ctx context.Context, id domain.RemoteHostID) (Host, error)
	UpdateState(ctx context.Context, id domain.RemoteHostID, state domain.RemoteHostState) (Host, error)
	Deregister(ctx context.Context, id domain.RemoteHostID) error
}

type RegisterInput struct {
	HostID  string `json:"hostId"`
	Address string `json:"address"`
	Label   string `json:"label,omitempty"`
}

type Host struct {
	HostID             string                 `json:"hostId"`
	Address            string                 `json:"address"`
	Label              string                 `json:"label,omitempty"`
	State              domain.RemoteHostState `json:"state"`
	LastProbeAt        time.Time              `json:"lastProbeAt"`
	LastProbeSucceeded bool                   `json:"lastProbeSucceeded"`
	LastProbeError     string                 `json:"lastProbeError,omitempty"`
	CreatedAt          time.Time              `json:"createdAt"`
	UpdatedAt          time.Time              `json:"updatedAt"`
	InventoryStale     bool                   `json:"inventoryStale,omitempty"`
	InventoryError     string                 `json:"inventoryError,omitempty"`
}

type Inventory struct {
	Hosts  []Host
	Stale  bool
	Reason string
}

type Deps struct {
	Store             Store
	Prober            ports.RemoteDaemonProber
	SessionLister     ports.RemoteDaemonSessionLister
	Clock             func() time.Time
	Logger            *slog.Logger
	ProbeTimeout      time.Duration
	Inventory         InventoryProvider
	InventoryInterval time.Duration
}

type Service struct {
	store         Store
	prober        ports.RemoteDaemonProber
	sessionLister ports.RemoteDaemonSessionLister
	clock         func() time.Time
	log           *slog.Logger
	count         atomic.Int64

	probeTimeout       time.Duration
	probeMu            sync.Mutex
	probeCtx           context.Context
	probeInterval      time.Duration
	inventoryInterval  time.Duration
	probeWorker        *healthProbeWorker
	inventory          InventoryProvider
	inventoryRefreshMu sync.Mutex
	inventoryMu        sync.RWMutex
	inventoryHosts     map[domain.RemoteHostID]InventoryHost
	inventoryProbes    map[domain.RemoteHostID]inventoryProbe
	inventoryAbsences  map[domain.RemoteHostID]int
	inventoryStale     bool
	inventoryReason    string
}

type inventoryProbe struct {
	at        time.Time
	succeeded bool
	reason    string
}

type healthProbeWorker struct {
	cancel context.CancelFunc
	done   chan struct{}
}

var _ Manager = (*Service)(nil)

func New(deps Deps) *Service {
	clock := deps.Clock
	if clock == nil {
		clock = time.Now
	}
	log := deps.Logger
	if log == nil {
		log = slog.Default()
	}
	probeTimeout := deps.ProbeTimeout
	if probeTimeout <= 0 {
		probeTimeout = DefaultProbeTimeout
	}
	inventoryInterval := deps.InventoryInterval
	if inventoryInterval <= 0 {
		inventoryInterval = DefaultProbeInterval
	}
	return &Service{store: deps.Store, prober: deps.Prober, sessionLister: deps.SessionLister, clock: clock, log: log, probeTimeout: probeTimeout, inventory: deps.Inventory, inventoryInterval: inventoryInterval, inventoryHosts: map[domain.RemoteHostID]InventoryHost{}, inventoryProbes: map[domain.RemoteHostID]inventoryProbe{}, inventoryAbsences: map[domain.RemoteHostID]int{}}
}

// LoadPresence initializes the in-memory registered-host count during daemon
// startup. The federation list uses it to leave the local-only request path
// completely free of registry reads.
func (s *Service) LoadPresence(ctx context.Context) error {
	hosts, err := s.store.ListRemoteHosts(ctx)
	if err != nil {
		s.log.Error("load remote host presence failed", "err", err)
		return err
	}
	s.count.Store(int64(len(hosts)))
	if s.inventory != nil {
		s.refreshInventory(ctx)
	}
	return nil
}

// HasRegisteredHosts reports whether federation needs to read the registry.
func (s *Service) HasRegisteredHosts() bool {
	return s.count.Load() > 0
}

func (s *Service) HasHostInventory() bool {
	return s.HasRegisteredHosts() || s.inventory != nil
}

func (s *Service) Register(ctx context.Context, in RegisterInput) (Host, error) {
	id := domain.RemoteHostID(strings.TrimSpace(in.HostID))
	if err := domain.ValidateRemoteHostID(id); err != nil {
		return Host{}, apierr.Invalid("INVALID_REMOTE_HOST_ID", err.Error(), nil)
	}
	address := strings.TrimSpace(in.Address)
	if err := domain.ValidateRemoteHostAddress(address); err != nil {
		return Host{}, apierr.Invalid("INVALID_REMOTE_HOST_ADDRESS", err.Error(), nil)
	}
	label := strings.TrimSpace(in.Label)
	if len(label) > 120 {
		return Host{}, apierr.Invalid("INVALID_REMOTE_HOST_LABEL", "Remote host label must be at most 120 characters", nil)
	}
	now := s.clock().UTC()
	host := domain.RemoteHost{
		HostID:         id,
		Address:        address,
		Label:          label,
		LastProbeAt:    now,
		LastProbeError: "probe has not completed",
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	s.inventoryRefreshMu.Lock()
	created, err := s.store.UpsertRemoteHost(ctx, host)
	if err != nil {
		s.inventoryRefreshMu.Unlock()
		s.log.Error("register remote host failed", "hostId", id, "err", err)
		return Host{}, apierr.Internal("REMOTE_HOST_REGISTER_FAILED", "Failed to register remote host")
	}
	if created {
		s.count.Add(1)
	}
	s.inventoryMu.Lock()
	delete(s.inventoryAbsences, id)
	s.inventoryMu.Unlock()
	s.inventoryRefreshMu.Unlock()
	s.startHealthProbeWorker()
	return s.probe(ctx, host)
}

func (s *Service) List(ctx context.Context) ([]Host, error) {
	hosts, err := s.store.ListRemoteHosts(ctx)
	if err != nil {
		s.log.Error("list remote hosts failed", "err", err)
		return nil, apierr.Internal("REMOTE_HOSTS_LIST_FAILED", "Failed to load remote hosts")
	}
	out := make([]Host, 0, len(hosts))
	for _, host := range hosts {
		out = append(out, hostView(host))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].HostID < out[j].HostID })
	return out, nil
}

func (s *Service) Inventory(ctx context.Context) (Inventory, error) {
	hosts, err := s.store.ListRemoteHosts(ctx)
	if err != nil {
		s.log.Error("list remote hosts failed", "err", err)
		return Inventory{}, apierr.Internal("REMOTE_HOSTS_LIST_FAILED", "Failed to load remote hosts")
	}
	inventoryHosts, inventoryProbes, stale, reason := s.inventorySnapshot()
	registered := make(map[domain.RemoteHostID]domain.RemoteHost, len(hosts))
	for _, host := range hosts {
		registered[host.HostID] = host
	}
	out := make([]Host, 0, len(hosts)+len(inventoryHosts))
	for _, host := range hosts {
		if _, listed := inventoryHosts[host.HostID]; listed {
			continue
		}
		out = append(out, hostView(host))
	}
	for id, listed := range inventoryHosts {
		out = append(out, s.inventoryHostView(listed, registered[id], inventoryProbes[id], stale, reason))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].HostID < out[j].HostID })
	return Inventory{Hosts: out, Stale: stale, Reason: reason}, nil
}

func (s *Service) Get(ctx context.Context, id domain.RemoteHostID) (Host, error) {
	if err := domain.ValidateRemoteHostID(id); err != nil {
		return Host{}, apierr.Invalid("INVALID_REMOTE_HOST_ID", err.Error(), nil)
	}
	host, found, err := s.store.GetRemoteHost(ctx, id)
	if err != nil {
		s.log.Error("get remote host failed", "hostId", id, "err", err)
		return Host{}, apierr.Internal("REMOTE_HOST_LOAD_FAILED", "Failed to load remote host")
	}
	if !found {
		return Host{}, apierr.NotFound("REMOTE_HOST_NOT_FOUND", "Unknown remote host")
	}
	return hostView(host), nil
}

func (s *Service) UpdateState(ctx context.Context, id domain.RemoteHostID, state domain.RemoteHostState) (Host, error) {
	if err := domain.ValidateRemoteHostID(id); err != nil {
		return Host{}, apierr.Invalid("INVALID_REMOTE_HOST_ID", err.Error(), nil)
	}
	host, found, err := s.store.GetRemoteHost(ctx, id)
	if err != nil {
		s.log.Error("get remote host for state update failed", "hostId", id, "err", err)
		return Host{}, apierr.Internal("REMOTE_HOST_LOAD_FAILED", "Failed to load remote host")
	}
	if !found {
		return Host{}, apierr.NotFound("REMOTE_HOST_NOT_FOUND", "Unknown remote host")
	}
	if host.OperatorState == domain.RemoteHostStateDestroyed && state != domain.RemoteHostStateDestroyed {
		return Host{}, apierr.Conflict("REMOTE_HOST_DESTROYED", "Destroyed remote hosts cannot be resumed", nil)
	}
	switch state {
	case domain.RemoteHostStateStopped, domain.RemoteHostStateDestroyed:
		host, found, err := s.store.SetRemoteHostOperatorState(ctx, id, state, s.clock().UTC())
		if err != nil {
			s.log.Error("set remote host operator state failed", "hostId", id, "state", state, "err", err)
			return Host{}, apierr.Internal("REMOTE_HOST_STATE_UPDATE_FAILED", "Failed to update remote host state")
		}
		if !found {
			return Host{}, apierr.NotFound("REMOTE_HOST_NOT_FOUND", "Unknown remote host")
		}
		return hostView(host), nil
	case domain.RemoteHostStateAvailable:
		resumed, _, err := s.store.SetRemoteHostOperatorState(ctx, id, "", s.clock().UTC())
		if err != nil {
			s.log.Error("resume remote host probing failed", "hostId", id, "err", err)
			return Host{}, apierr.Internal("REMOTE_HOST_STATE_UPDATE_FAILED", "Failed to update remote host state")
		}
		return s.probe(ctx, resumed)
	default:
		return Host{}, apierr.Invalid("INVALID_REMOTE_HOST_STATE", "Remote host state must be available, stopped, or destroyed", nil)
	}
}

func (s *Service) Deregister(ctx context.Context, id domain.RemoteHostID) error {
	if err := domain.ValidateRemoteHostID(id); err != nil {
		return apierr.Invalid("INVALID_REMOTE_HOST_ID", err.Error(), nil)
	}
	deleted, err := s.store.DeleteRemoteHost(ctx, id)
	if err != nil {
		s.log.Error("deregister remote host failed", "hostId", id, "err", err)
		return apierr.Internal("REMOTE_HOST_DEREGISTER_FAILED", "Failed to deregister remote host")
	}
	if !deleted {
		return apierr.NotFound("REMOTE_HOST_NOT_FOUND", "Unknown remote host")
	}
	remaining := int64(0)
	for current := s.count.Load(); current > 0; current = s.count.Load() {
		if s.count.CompareAndSwap(current, current-1) {
			remaining = current - 1
			break
		}
	}
	if remaining == 0 && s.inventory == nil {
		s.stopHealthProbeWorker()
	}
	return nil
}

// RunHealthProbes configures the daemon-lifetime probe worker. It starts no
// goroutine until at least one host is registered; Register starts the worker
// later if the daemon booted with an empty registry.
func (s *Service) RunHealthProbes(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = DefaultProbeInterval
	}
	s.probeMu.Lock()
	s.probeCtx = ctx
	s.probeInterval = interval
	s.startHealthProbeWorkerLocked()
	s.probeMu.Unlock()
}

func (s *Service) startHealthProbeWorker() {
	s.probeMu.Lock()
	s.startHealthProbeWorkerLocked()
	s.probeMu.Unlock()
}

func (s *Service) startHealthProbeWorkerLocked() {
	if !s.HasHostInventory() || s.probeWorker != nil || s.probeCtx == nil || s.probeCtx.Err() != nil {
		return
	}
	ctx, cancel := context.WithCancel(s.probeCtx)
	worker := &healthProbeWorker{cancel: cancel, done: make(chan struct{})}
	s.probeWorker = worker
	go s.runHealthProbes(ctx, s.probeInterval, worker)
}

func (s *Service) stopHealthProbeWorker() {
	s.probeMu.Lock()
	worker := s.probeWorker
	if worker != nil {
		worker.cancel()
		s.probeWorker = nil
	}
	s.probeMu.Unlock()
	if worker != nil {
		<-worker.done
	}
}

func (s *Service) runHealthProbes(ctx context.Context, interval time.Duration, worker *healthProbeWorker) {
	defer close(worker.done)
	defer s.finishHealthProbeWorker(worker)
	if !s.HasHostInventory() {
		return
	}
	s.probeAll(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var inventoryTicks <-chan time.Time
	var inventoryTicker *time.Ticker
	if s.inventory != nil {
		inventoryTicker = time.NewTicker(s.inventoryInterval)
		inventoryTicks = inventoryTicker.C
		defer inventoryTicker.Stop()
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-inventoryTicks:
			s.refreshInventory(ctx)
		case <-ticker.C:
			if !s.HasHostInventory() {
				return
			}
			s.probeAll(ctx)
		}
	}
}

func (s *Service) finishHealthProbeWorker(worker *healthProbeWorker) {
	s.probeMu.Lock()
	defer s.probeMu.Unlock()
	if s.probeWorker != worker {
		return
	}
	s.probeWorker = nil
}

func (s *Service) probeAll(ctx context.Context) {
	if !s.HasHostInventory() {
		return
	}
	hosts, err := s.store.ListRemoteHosts(ctx)
	if err != nil {
		s.log.Error("list remote hosts for health probe failed", "err", err)
		return
	}
	inventoryHosts, _, _, _ := s.inventorySnapshot()
	type probeJob func(context.Context) error
	jobsToRun := make([]probeJob, 0, len(hosts)+len(inventoryHosts))
	registered := make(map[domain.RemoteHostID]struct{}, len(hosts))
	for _, host := range hosts {
		registered[host.HostID] = struct{}{}
		listed, inInventory := inventoryHosts[host.HostID]
		if inInventory {
			if listed.Lifecycle == InventoryLifecycleStopped {
				continue
			}
			host := host
			jobsToRun = append(jobsToRun, func(ctx context.Context) error { return s.probeInventory(ctx, listed, host) })
			continue
		}
		host := host
		jobsToRun = append(jobsToRun, func(ctx context.Context) error {
			_, err := s.probe(ctx, host)
			return err
		})
	}
	for id, listed := range inventoryHosts {
		if _, exists := registered[id]; exists || listed.Lifecycle == InventoryLifecycleStopped {
			continue
		}
		listed := listed
		jobsToRun = append(jobsToRun, func(ctx context.Context) error { return s.probeInventory(ctx, listed, domain.RemoteHost{}) })
	}
	workers := min(len(jobsToRun), remoteHostProbeWorkers)
	if workers == 0 {
		return
	}
	jobs := make(chan probeJob)
	var group sync.WaitGroup
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			for job := range jobs {
				if ctx.Err() != nil {
					return
				}
				if err := job(ctx); err != nil {
					s.log.Error("remote host health probe recording failed", "err", err)
				}
			}
		}()
	}
	for _, job := range jobsToRun {
		select {
		case <-ctx.Done():
			close(jobs)
			group.Wait()
			return
		case jobs <- job:
		}
	}
	close(jobs)
	group.Wait()
}

func (s *Service) probe(ctx context.Context, host domain.RemoteHost) (Host, error) {
	if host.OperatorState == domain.RemoteHostStateStopped || host.OperatorState == domain.RemoteHostStateDestroyed {
		return hostView(host), nil
	}
	if s.prober == nil {
		err := fmt.Errorf("remote daemon prober is unavailable")
		s.log.Error("remote host health probe unavailable", "hostId", host.HostID, "err", err)
		return Host{}, apierr.Internal("REMOTE_HOST_PROBER_UNAVAILABLE", "Remote host health probing is unavailable")
	}
	probeCtx, cancel := context.WithTimeout(ctx, s.probeTimeout)
	defer cancel()
	err := s.prober.Probe(probeCtx, host.Address)
	if ctx.Err() != nil {
		s.log.Info("remote host health probe canceled", "hostId", host.HostID, "address", host.Address, "reason", ctx.Err())
		return Host{}, ctx.Err()
	}
	now := s.clock().UTC()
	failureReason := ""
	if err != nil {
		failureReason = remotedaemonhttp.UnavailabilityReason(err)
		s.log.Warn("remote host health probe failed", "hostId", host.HostID, "address", host.Address, "err", err)
	}
	recorded, recordErr := s.store.RecordRemoteHostProbe(ctx, host.HostID, now, err == nil, failureReason)
	if recordErr != nil {
		s.log.Error("record remote host health probe failed", "hostId", host.HostID, "err", recordErr)
		return Host{}, apierr.Internal("REMOTE_HOST_PROBE_RECORD_FAILED", "Failed to record remote host health")
	}
	if !recorded {
		return Host{}, apierr.NotFound("REMOTE_HOST_NOT_FOUND", "Unknown remote host")
	}
	host.LastProbeAt = now
	host.LastProbeSucceeded = err == nil
	host.LastProbeError = failureReason
	host.UpdatedAt = now
	if err == nil {
		s.refreshSnapshots(probeCtx, host, now)
	}
	return hostView(host), nil
}

func (s *Service) refreshSnapshots(ctx context.Context, host domain.RemoteHost, observedAt time.Time) {
	if s.sessionLister == nil {
		s.log.Error("remote session snapshot refresh unavailable", "hostId", host.HostID, "reason", "remote session lister is unavailable")
		return
	}
	snapshots, err := s.sessionLister.ListSessions(ctx, host.Address, ports.RemoteSessionListFilter{})
	if err != nil {
		s.log.Warn("remote session snapshot refresh failed", "hostId", host.HostID, "address", host.Address, "err", err)
		return
	}
	for index := range snapshots {
		snapshots[index].HostID = host.HostID
		snapshots[index].ObservedAt = observedAt
	}
	if err := s.store.ReplaceRemoteSessionSnapshots(ctx, host.HostID, snapshots); err != nil {
		s.log.Error("store remote session snapshots failed", "hostId", host.HostID, "err", err)
	}
}

func (s *Service) refreshInventory(ctx context.Context) {
	if s.inventory == nil {
		return
	}
	hosts, err := s.inventory.List(ctx)
	if err != nil {
		s.inventoryMu.Lock()
		s.inventoryAbsences = map[domain.RemoteHostID]int{}
		s.inventoryStale = true
		s.inventoryReason = err.Error()
		s.inventoryMu.Unlock()
		s.log.Warn("host inventory refresh failed", "err", err)
		return
	}
	next := make(map[domain.RemoteHostID]InventoryHost, len(hosts))
	for _, host := range hosts {
		next[host.HostID] = host
	}
	s.inventoryRefreshMu.Lock()
	defer s.inventoryRefreshMu.Unlock()
	registered, err := s.store.ListRemoteHosts(ctx)
	if err != nil {
		s.log.Error("list remote hosts for inventory reconciliation failed", "err", err)
	}
	s.inventoryMu.Lock()
	previous := s.inventoryHosts
	s.inventoryHosts = next
	for id, host := range next {
		if previousHost, found := previous[id]; !found || previousHost.Address != host.Address {
			delete(s.inventoryProbes, id)
		}
	}
	for id := range s.inventoryProbes {
		if _, found := next[id]; !found {
			delete(s.inventoryProbes, id)
		}
	}
	s.inventoryStale = false
	s.inventoryReason = ""
	if err != nil {
		s.inventoryAbsences = map[domain.RemoteHostID]int{}
		s.inventoryMu.Unlock()
		return
	}
	prune := s.advanceInventoryAbsences(registered, next)
	s.inventoryMu.Unlock()
	for _, id := range prune {
		deleted, deleteErr := s.store.DeleteRemoteHost(ctx, id)
		if deleteErr != nil {
			s.log.Error("prune remote host absent from inventory failed", "hostId", id, "err", deleteErr)
			continue
		}
		if !deleted {
			continue
		}
		for current := s.count.Load(); current > 0; current = s.count.Load() {
			if s.count.CompareAndSwap(current, current-1) {
				break
			}
		}
	}
}

func (s *Service) advanceInventoryAbsences(registered []domain.RemoteHost, inventory map[domain.RemoteHostID]InventoryHost) []domain.RemoteHostID {
	prune := make([]domain.RemoteHostID, 0)
	for _, host := range registered {
		if _, listed := inventory[host.HostID]; listed {
			delete(s.inventoryAbsences, host.HostID)
			continue
		}
		s.inventoryAbsences[host.HostID]++
		if s.inventoryAbsences[host.HostID] < inventoryAbsenceReads {
			continue
		}
		delete(s.inventoryAbsences, host.HostID)
		prune = append(prune, host.HostID)
	}
	return prune
}

func (s *Service) inventorySnapshot() (map[domain.RemoteHostID]InventoryHost, map[domain.RemoteHostID]inventoryProbe, bool, string) {
	s.inventoryMu.RLock()
	defer s.inventoryMu.RUnlock()
	hosts := make(map[domain.RemoteHostID]InventoryHost, len(s.inventoryHosts))
	for id, host := range s.inventoryHosts {
		hosts[id] = host
	}
	probes := make(map[domain.RemoteHostID]inventoryProbe, len(s.inventoryProbes))
	for id, probe := range s.inventoryProbes {
		probes[id] = probe
	}
	return hosts, probes, s.inventoryStale, s.inventoryReason
}

func (s *Service) probeInventory(ctx context.Context, listed InventoryHost, registered domain.RemoteHost) error {
	address := listed.Address
	if address == "" {
		address = registered.Address
	}
	if address == "" {
		s.recordInventoryProbe(listed.HostID, false, "inventory does not provide a remote daemon address")
		return nil
	}
	if s.prober == nil {
		return errors.New("remote daemon prober is unavailable")
	}
	probeCtx, cancel := context.WithTimeout(ctx, s.probeTimeout)
	defer cancel()
	err := s.prober.Probe(probeCtx, address)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	reason := ""
	if err != nil {
		reason = remotedaemonhttp.UnavailabilityReason(err)
		s.log.Warn("remote host health probe failed", "hostId", listed.HostID, "address", address, "err", err)
	}
	s.recordInventoryProbe(listed.HostID, err == nil, reason)
	if registered.HostID == "" {
		return nil
	}
	now := s.clock().UTC()
	if _, recordErr := s.store.RecordRemoteHostProbe(ctx, registered.HostID, now, err == nil, reason); recordErr != nil {
		return fmt.Errorf("record remote host health: %w", recordErr)
	}
	if err == nil {
		registered.Address = address
		s.refreshSnapshots(probeCtx, registered, now)
	}
	return nil
}

func (s *Service) recordInventoryProbe(id domain.RemoteHostID, succeeded bool, reason string) {
	s.inventoryMu.Lock()
	s.inventoryProbes[id] = inventoryProbe{at: s.clock().UTC(), succeeded: succeeded, reason: reason}
	s.inventoryMu.Unlock()
}

func (s *Service) inventoryHostView(listed InventoryHost, registered domain.RemoteHost, probe inventoryProbe, stale bool, inventoryReason string) Host {
	address := listed.Address
	if address == "" {
		address = registered.Address
	}
	label := registered.Label
	if strings.TrimSpace(label) == "" {
		label = listed.Label
	}
	host := domain.RemoteHost{HostID: listed.HostID, Address: address, Label: label, LastProbeAt: probe.at, LastProbeSucceeded: probe.succeeded, LastProbeError: probe.reason, CreatedAt: registered.CreatedAt, UpdatedAt: registered.UpdatedAt}
	if listed.Lifecycle == InventoryLifecycleStopped {
		host.OperatorState = domain.RemoteHostStateStopped
	}
	if listed.Lifecycle == InventoryLifecycleRunning && address == "" && probe.reason == "" {
		host.LastProbeError = "inventory does not provide a remote daemon address"
	}
	view := hostView(host)
	view.InventoryStale = stale
	view.InventoryError = inventoryReason
	if view.InventoryStale && view.LastProbeError == "" {
		view.LastProbeError = inventoryReason
	}
	return view
}

func hostView(host domain.RemoteHost) Host {
	return Host{
		HostID:             string(host.HostID),
		Address:            host.Address,
		Label:              host.Label,
		State:              host.CurrentState(),
		LastProbeAt:        host.LastProbeAt,
		LastProbeSucceeded: host.LastProbeSucceeded,
		LastProbeError:     host.LastProbeError,
		CreatedAt:          host.CreatedAt,
		UpdatedAt:          host.UpdatedAt,
	}
}
