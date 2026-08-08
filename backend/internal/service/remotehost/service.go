package remotehost

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/apierr"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

const DefaultProbeInterval = 30 * time.Second

type Store interface {
	CreateRemoteHost(ctx context.Context, host domain.RemoteHost) (bool, error)
	ListRemoteHosts(ctx context.Context) ([]domain.RemoteHost, error)
	GetRemoteHost(ctx context.Context, id domain.RemoteHostID) (domain.RemoteHost, bool, error)
	RecordRemoteHostProbe(ctx context.Context, id domain.RemoteHostID, at time.Time, succeeded bool, failureReason string) (bool, error)
	SetRemoteHostOperatorState(ctx context.Context, id domain.RemoteHostID, state domain.RemoteHostState, at time.Time) (domain.RemoteHost, bool, error)
	DeleteRemoteHost(ctx context.Context, id domain.RemoteHostID) (bool, error)
}

type Manager interface {
	Register(ctx context.Context, in RegisterInput) (Host, error)
	List(ctx context.Context) ([]Host, error)
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
}

type Deps struct {
	Store  Store
	Prober ports.RemoteDaemonProber
	Clock  func() time.Time
	Logger *slog.Logger
}

type Service struct {
	store  Store
	prober ports.RemoteDaemonProber
	clock  func() time.Time
	log    *slog.Logger
	count  atomic.Int64
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
	return &Service{store: deps.Store, prober: deps.Prober, clock: clock, log: log}
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
	return nil
}

// HasRegisteredHosts reports whether federation needs to read the registry.
func (s *Service) HasRegisteredHosts() bool {
	return s.count.Load() > 0
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
	created, err := s.store.CreateRemoteHost(ctx, host)
	if err != nil {
		s.log.Error("register remote host failed", "hostId", id, "err", err)
		return Host{}, apierr.Internal("REMOTE_HOST_REGISTER_FAILED", "Failed to register remote host")
	}
	if !created {
		return Host{}, apierr.Conflict("REMOTE_HOST_ALREADY_REGISTERED", "A remote host with this id is already registered", nil)
	}
	s.count.Add(1)
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
	return out, nil
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
	for current := s.count.Load(); current > 0; current = s.count.Load() {
		if s.count.CompareAndSwap(current, current-1) {
			break
		}
	}
	return nil
}

func (s *Service) RunHealthProbes(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = DefaultProbeInterval
	}
	s.probeAll(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.probeAll(ctx)
		}
	}
}

func (s *Service) probeAll(ctx context.Context) {
	hosts, err := s.store.ListRemoteHosts(ctx)
	if err != nil {
		s.log.Error("list remote hosts for health probe failed", "err", err)
		return
	}
	for _, host := range hosts {
		if _, err := s.probe(ctx, host); err != nil {
			s.log.Error("remote host health probe recording failed", "hostId", host.HostID, "err", err)
		}
	}
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
	err := s.prober.Probe(ctx, host.Address)
	now := s.clock().UTC()
	failureReason := ""
	if err != nil {
		failureReason = err.Error()
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
	return hostView(host), nil
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
