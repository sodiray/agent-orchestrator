-- +goose Up
-- +goose StatementBegin
CREATE TABLE remote_hosts (
    host_id              TEXT PRIMARY KEY,
    address              TEXT NOT NULL,
    label                TEXT NOT NULL DEFAULT '',
    operator_state       TEXT NOT NULL DEFAULT '' CHECK (operator_state IN ('', 'stopped', 'destroyed')),
    last_probe_at        TIMESTAMP NOT NULL,
    last_probe_succeeded BOOLEAN NOT NULL DEFAULT FALSE,
    last_probe_error     TEXT NOT NULL DEFAULT '',
    created_at           TIMESTAMP NOT NULL,
    updated_at           TIMESTAMP NOT NULL
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS remote_hosts;
-- +goose StatementEnd
