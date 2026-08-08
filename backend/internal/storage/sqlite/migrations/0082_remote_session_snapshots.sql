-- +goose Up
-- +goose StatementBegin
CREATE TABLE remote_session_snapshots (
    host_id     TEXT NOT NULL REFERENCES remote_hosts(host_id) ON DELETE CASCADE,
    session_id  TEXT NOT NULL,
    view_json   TEXT NOT NULL,
    observed_at TIMESTAMP NOT NULL,
    PRIMARY KEY (host_id, session_id)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS remote_session_snapshots;
-- +goose StatementEnd
