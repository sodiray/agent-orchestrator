-- name: InsertRemoteHost :execrows
INSERT INTO remote_hosts (
    host_id, address, label, operator_state, last_probe_at, last_probe_succeeded,
    last_probe_error, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (host_id) DO NOTHING;

-- name: SelectRemoteHosts :many
SELECT * FROM remote_hosts ORDER BY host_id;

-- name: SelectRemoteHost :one
SELECT * FROM remote_hosts WHERE host_id = ?;

-- name: UpdateRemoteHostProbe :execrows
UPDATE remote_hosts
SET last_probe_at = ?, last_probe_succeeded = ?, last_probe_error = ?, updated_at = ?
WHERE host_id = ?;

-- name: UpdateRemoteHostOperatorState :one
UPDATE remote_hosts
SET operator_state = ?, updated_at = ?
WHERE host_id = ?
RETURNING *;

-- name: DeleteRemoteHost :execrows
DELETE FROM remote_hosts WHERE host_id = ?;
