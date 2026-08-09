package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	remotehostsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/remotehost"
)

var _ remotehostsvc.Store = (*Store)(nil)

func (s *Store) CreateRemoteHost(ctx context.Context, host domain.RemoteHost) (bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	result, err := s.writeDB.ExecContext(ctx, `
INSERT INTO remote_hosts (
    host_id, address, label, operator_state, last_probe_at, last_probe_succeeded,
    last_probe_error, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (host_id) DO NOTHING`, host.HostID, host.Address, host.Label, host.OperatorState,
		host.LastProbeAt, host.LastProbeSucceeded, host.LastProbeError, host.CreatedAt, host.UpdatedAt)
	if err != nil {
		return false, fmt.Errorf("insert remote host %s: %w", host.HostID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read insert remote host result %s: %w", host.HostID, err)
	}
	return affected > 0, nil
}

func (s *Store) UpsertRemoteHost(ctx context.Context, host domain.RemoteHost) (bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	result, err := s.writeDB.ExecContext(ctx, `
INSERT INTO remote_hosts (
    host_id, address, label, operator_state, last_probe_at, last_probe_succeeded,
    last_probe_error, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (host_id) DO NOTHING`, host.HostID, host.Address, host.Label, host.OperatorState,
		host.LastProbeAt, host.LastProbeSucceeded, host.LastProbeError, host.CreatedAt, host.UpdatedAt)
	if err != nil {
		return false, fmt.Errorf("insert remote host %s: %w", host.HostID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read insert remote host result %s: %w", host.HostID, err)
	}
	if affected > 0 {
		return true, nil
	}
	if _, err := s.writeDB.ExecContext(ctx, `
UPDATE remote_hosts
SET address = ?, label = ?, operator_state = ?, last_probe_at = ?, last_probe_succeeded = ?,
    last_probe_error = ?, updated_at = ?
WHERE host_id = ?`, host.Address, host.Label, host.OperatorState, host.LastProbeAt,
		host.LastProbeSucceeded, host.LastProbeError, host.UpdatedAt, host.HostID); err != nil {
		return false, fmt.Errorf("update remote host %s: %w", host.HostID, err)
	}
	return false, nil
}

func (s *Store) ListRemoteHosts(ctx context.Context) ([]domain.RemoteHost, error) {
	rows, err := s.readDB.QueryContext(ctx, `
SELECT host_id, address, label, operator_state, last_probe_at, last_probe_succeeded,
       last_probe_error, created_at, updated_at
FROM remote_hosts ORDER BY host_id`)
	if err != nil {
		return nil, fmt.Errorf("list remote hosts: %w", err)
	}
	defer func() { _ = rows.Close() }()
	hosts := []domain.RemoteHost{}
	for rows.Next() {
		host, err := scanRemoteHost(rows)
		if err != nil {
			return nil, err
		}
		hosts = append(hosts, host)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate remote hosts: %w", err)
	}
	return hosts, nil
}

func (s *Store) GetRemoteHost(ctx context.Context, id domain.RemoteHostID) (domain.RemoteHost, bool, error) {
	row := s.readDB.QueryRowContext(ctx, `
SELECT host_id, address, label, operator_state, last_probe_at, last_probe_succeeded,
       last_probe_error, created_at, updated_at
FROM remote_hosts WHERE host_id = ?`, id)
	host, err := scanRemoteHost(row)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.RemoteHost{}, false, nil
	}
	if err != nil {
		return domain.RemoteHost{}, false, fmt.Errorf("get remote host %s: %w", id, err)
	}
	return host, true, nil
}

func (s *Store) RecordRemoteHostProbe(ctx context.Context, id domain.RemoteHostID, at time.Time, succeeded bool, failureReason string) (bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	result, err := s.writeDB.ExecContext(ctx, `
UPDATE remote_hosts
SET last_probe_at = ?, last_probe_succeeded = ?, last_probe_error = ?, updated_at = ?
WHERE host_id = ?`, at, succeeded, failureReason, at, id)
	if err != nil {
		return false, fmt.Errorf("record remote host probe %s: %w", id, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read remote host probe result %s: %w", id, err)
	}
	return affected > 0, nil
}

func (s *Store) SetRemoteHostOperatorState(ctx context.Context, id domain.RemoteHostID, state domain.RemoteHostState, at time.Time) (domain.RemoteHost, bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	row := s.writeDB.QueryRowContext(ctx, `
UPDATE remote_hosts
SET operator_state = ?, updated_at = ?
WHERE host_id = ?
RETURNING host_id, address, label, operator_state, last_probe_at, last_probe_succeeded,
          last_probe_error, created_at, updated_at`, state, at, id)
	host, err := scanRemoteHost(row)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.RemoteHost{}, false, nil
	}
	if err != nil {
		return domain.RemoteHost{}, false, fmt.Errorf("set remote host state %s: %w", id, err)
	}
	return host, true, nil
}

func (s *Store) DeleteRemoteHost(ctx context.Context, id domain.RemoteHostID) (bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	result, err := s.writeDB.ExecContext(ctx, `DELETE FROM remote_hosts WHERE host_id = ?`, id)
	if err != nil {
		return false, fmt.Errorf("delete remote host %s: %w", id, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read delete remote host result %s: %w", id, err)
	}
	return affected > 0, nil
}

func (s *Store) ReplaceRemoteSessionSnapshots(ctx context.Context, id domain.RemoteHostID, snapshots []domain.RemoteSessionSnapshot) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin replace remote session snapshots for %s: %w", id, err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `DELETE FROM remote_session_snapshots WHERE host_id = ?`, id); err != nil {
		return fmt.Errorf("clear remote session snapshots for %s: %w", id, err)
	}
	for _, snapshot := range snapshots {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO remote_session_snapshots (host_id, session_id, view_json, observed_at)
VALUES (?, ?, ?, ?)`, id, snapshot.SessionID, string(snapshot.View), snapshot.ObservedAt); err != nil {
			return fmt.Errorf("insert remote session snapshot %s/%s: %w", id, snapshot.SessionID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit remote session snapshots for %s: %w", id, err)
	}
	return nil
}

func (s *Store) ListRemoteSessionSnapshots(ctx context.Context, id domain.RemoteHostID) ([]domain.RemoteSessionSnapshot, error) {
	rows, err := s.readDB.QueryContext(ctx, `
SELECT host_id, session_id, view_json, observed_at
FROM remote_session_snapshots
WHERE host_id = ?
ORDER BY session_id`, id)
	if err != nil {
		return nil, fmt.Errorf("list remote session snapshots for %s: %w", id, err)
	}
	defer func() { _ = rows.Close() }()
	out := []domain.RemoteSessionSnapshot{}
	for rows.Next() {
		var snapshot domain.RemoteSessionSnapshot
		var view string
		if err := rows.Scan(&snapshot.HostID, &snapshot.SessionID, &view, &snapshot.ObservedAt); err != nil {
			return nil, fmt.Errorf("scan remote session snapshot: %w", err)
		}
		snapshot.View = []byte(view)
		out = append(out, snapshot)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate remote session snapshots for %s: %w", id, err)
	}
	return out, nil
}

type remoteHostScanner interface {
	Scan(dest ...any) error
}

func scanRemoteHost(row remoteHostScanner) (domain.RemoteHost, error) {
	var host domain.RemoteHost
	if err := row.Scan(&host.HostID, &host.Address, &host.Label, &host.OperatorState, &host.LastProbeAt,
		&host.LastProbeSucceeded, &host.LastProbeError, &host.CreatedAt, &host.UpdatedAt); err != nil {
		return domain.RemoteHost{}, fmt.Errorf("scan remote host: %w", err)
	}
	return host, nil
}
