# AO CLI

The `ao` CLI is a thin Go/Cobra client for the local Agent Orchestrator daemon.
It starts, discovers, inspects, and stops the daemon through the loopback HTTP
surface and the `running.json` handshake. It must not open SQLite directly or
call runtime, workspace, tracker, or agent adapters in-process.

When using the CLI directly from a shell, make sure the daemon is running first
with `ao start` or by opening the desktop app. Product commands such as
`ao agent ls` and `ao spawn` call the loopback daemon and will fail with a
"daemon is not running" error if no `running.json` points at a live process. From
a source checkout, build and run the local binary explicitly, for example:

```bash
cd backend
go build -o ./bin/ao ./cmd/ao
./bin/ao agent ls
```

## Current commands

Every product command resolves to a daemon HTTP route. Run `ao <command>
--help` for the authoritative flag shape.

### Daemon control

| Command                       | Purpose                                                                                                                           |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `ao start`                    | Start the daemon in the background and wait for `/readyz`.                                                                        |
| `ao stop`                     | Gracefully stop the daemon via loopback `POST /shutdown` after verifying daemon identity.                                         |
| `ao status` / `--json`        | Report daemon state from `running.json`, process liveness, `/healthz`, and `/readyz`.                                             |
| `ao doctor` / `--json`        | Check config, data directory, DB-file presence, daemon state, `git`, and (on Darwin/Linux) `tmux`; on Windows conpty is built in. |
| `ao completion <shell>`       | Generate completions for `bash`, `zsh`, `fish`, or `powershell`.                                                                  |
| `ao version` / `ao --version` | Print build metadata.                                                                                                             |
| `ao daemon`                   | Hidden internal daemon entrypoint used by `ao start`.                                                                             |

### Product commands

| Command                             | Daemon route                                   |
| ----------------------------------- | ---------------------------------------------- |
| `ao project add`                    | `POST /api/v1/projects`                        |
| `ao project ls`                     | `GET /api/v1/projects`                         |
| `ao project get <id>`               | `GET /api/v1/projects/{id}`                    |
| `ao project set-config <id>`        | `PUT /api/v1/projects/{id}/config`             |
| `ao project rm <id>`                | `DELETE /api/v1/projects/{id}`                 |
| `ao agent ls`                       | `GET /api/v1/agents`                           |
| `ao agent ls --refresh`             | `POST /api/v1/agents/refresh`                  |
| `ao spawn`                          | `POST /api/v1/sessions`                        |
| `ao session ls`                     | `GET /api/v1/sessions`                         |
| `ao session get <id>`               | `GET /api/v1/sessions/{id}`                    |
| `ao session kill <id>`              | `POST /api/v1/sessions/{id}/kill`              |
| `ao session restore <id>`           | `POST /api/v1/sessions/{id}/restore`           |
| `ao session rename <id> <name>`     | `PATCH /api/v1/sessions/{id}`                  |
| `ao session cleanup`                | `POST /api/v1/sessions/cleanup`                |
| `ao session claim-pr <id> <pr-ref>` | `POST /api/v1/sessions/{id}/pr/claim`          |
| `ao orchestrator ls`                | `GET /api/v1/orchestrators`                    |
| `ao send`                           | `POST /api/v1/sessions/{id}/send`              |
| `ao preview [url]`                  | `POST /api/v1/sessions/{id}/preview`           |
| `ao preview start/status/stop`      | `POST/GET/DELETE /api/v1/sessions/{id}/preview/server` |
| `ao browser ...`                    | `GET /api/v1/browser/status`, `POST /api/v1/browser/commands` |
| `ao hooks <agent> <event>`          | `POST /api/v1/sessions/{id}/activity` (hidden) |

`ao agent ls` prints the daemon-supported agent catalog with local install/auth
readiness. Use `--refresh` to rerun the bounded local probes and `--json` to
print the raw inventory response.

`ao spawn` resolves project context in this order: explicit `--project`,
`AO_PROJECT_ID`, `AO_SESSION_ID` (by fetching the current session from the
daemon), then the current working directory matched against registered project
paths. If `AO_SESSION_ID` is set but the session cannot be fetched, pass
`--project` explicitly.

If `--agent` / `--harness` is omitted, `ao spawn` uses the resolved project's
`worker.agent` config. Before spawning, the CLI refreshes the advisory agent
catalog and fails early when the selected agent is unsupported, not installed,
or unauthorized. It warns-but-continues when auth remains unknown because daemon
spawn remains the authoritative runtime validation point. Use
`--skip-agent-check` to bypass only this CLI-side preflight.

`ao preview` resolves its session from the `AO_SESSION_ID` environment variable
(it is meant to run inside a session), not a flag. With no argument it
autodetects an `index.html` in the session workspace; with a URL argument it
opens that URL verbatim (`file://`, `http`, `https`).

`ao preview start [configuration]` loads `.ao/launch.json` from the session
workspace, starts that exact command under a session-owned supervisor, selects
or records its loopback port, waits for readiness, and opens application
targets in the Browser panel. `status` reports bounded recent logs and `stop`
terminates the managed process tree. Multiple configurations must be selected
by name; AO does not assign confidence scores to arbitrary localhost servers.
This is an optional, reusable project configuration, not a prerequisite for
preview. Agents must not create it automatically. Static HTML and Markdown use
the direct file preview and must not cause package-manager scaffolding,
dependency installation, or a development server to be introduced.

When a browser-displayable file is the requested artifact, agents should call
`ao preview <workspace-path>` immediately after creating or materially updating
the primary output. Markdown, HTML, PDF, SVG, and common images can be served
directly. Supporting assets must not replace an active application preview.

`ao browser` also resolves its target from `AO_SESSION_ID`, but controls the
session-owned live Electron browser rather than only setting its preview URL.
The target-isolated command set includes `status`, `open`, `snapshot`, `click`,
`fill`, `type`, `press`, `hover`, `scroll`, `select`, `check`, `uncheck`, `get`,
`highlight`, `unhighlight`, `tabs`, `tab new`, `tab select`, `tab close`,
`wait`, `screenshot`, `network start/status/list/stop/clear`, `console`, and
`errors`. Logical tab IDs remain stable for the session, and allowed popups
become AO browser tabs rather than separate OS-browser windows. The AO desktop
app must be open because Electron owns the `WebContentsView`.
References from a snapshot are invalidated after navigation or DOM replacement;
they are also invalidated when changing tabs. Take another snapshot when a
command reports `STALE_REFERENCE`.
Browser waits cover load completion, text or selector appearance and
disappearance, URL matching, fixed delays, and a configurable DOM-stability
window for HMR-driven verification.
Browser tabs in the same worker share a memory-only Electron profile. Different
workers receive distinct partitions, so cookies, authentication, local storage,
and session storage do not leak between their browser runtimes.
Network capture is disabled by default and must be started explicitly. It is
scoped to the active tab at start time, expires after 60 seconds by default
(maximum 300), retains at most 200 in-memory entries, and is cleared with the
tab/session. Captured data is metadata-only: request and response bodies are
never read, sensitive headers are omitted, and URL credentials, fragments, and
query values are redacted.

`go run .` in `backend/` remains a compatibility wrapper around the daemon.

PR actions are available through `ao pr merge` and
`ao pr resolve-comments`. Review actions are available through `ao review ls`,
`ao review trigger` (also `execute` and `restart`), `ao review cancel` (also
`stop`), and `ao review submit`.

## Configuration

The daemon first applies built-in defaults, then optionally reads
`$AO_DATA_DIR/config.yaml` (default `~/.ao/data/config.yaml`), and finally
applies environment variables. A present environment variable overrides the
file value, including an empty variable that intentionally restores that
setting's normal default behavior. A missing file preserves the environment-only behavior; an invalid
or group/world-writable file stops startup. The file is trusted operator input,
including its optional inventory command, and must not be writable by anyone
other than its owner.

`AO_DATA_DIR` remains environment-only because it locates the configuration
file itself. `AO_APP_RUN_ID` and `AO_TELEMETRY_APP_VERSION` are supervisor
launch metadata and are environment-only as well.

The CLI and daemon share these environment settings:

| Var                   | Default              | Purpose                                                                                        |
| --------------------- | -------------------- | ---------------------------------------------------------------------------------------------- |
| `AO_LISTEN`           | `loopback`           | Daemon listener: `loopback`, or `unix:<path>` for a Unix-domain socket.                       |
| `AO_PORT`             | `3001`               | Loopback daemon port; ignored when `AO_LISTEN` selects a Unix socket.                          |
| `AO_RUN_FILE`         | `~/.ao/running.json` | PID/listener handshake.                                                                        |
| `AO_DATA_DIR`         | `~/.ao/data`         | SQLite data directory.                                                                         |
| `AO_REQUEST_TIMEOUT`  | `60s`                | REST request timeout.                                                                          |
| `AO_SHUTDOWN_TIMEOUT` | `10s`                | Graceful shutdown cap.                                                                         |
| `AO_REMOTE_HOST_PROBE_TIMEOUT` | `10s`          | Per-host health probe and snapshot timeout.                                                    |
| `AO_HOST_INVENTORY_COMMAND` | unset             | JSON argv array for the optional host inventory command.                                       |
| `AO_HOST_INVENTORY_INTERVAL` | `30s`           | Host inventory refresh interval.                                                               |
| `AO_HOST_INVENTORY_TIMEOUT` | `10s`            | Host inventory command timeout.                                                                |
| `AO_HOST_INVENTORY_MAX_OUTPUT` | `1048576`      | Maximum inventory-command stdout bytes.                                                        |
| `AO_AGENT`            | `claude-code`        | Compatibility agent adapter.                                                                   |
| `AO_ALLOWED_ORIGINS`  | `app://renderer`     | Comma-separated CORS origins.                                                                  |
| `AO_TELEMETRY_EVENTS` | `off`                | Local event capture.                                                                           |
| `AO_TELEMETRY_METRICS` | `off`               | Local metric capture.                                                                          |
| `AO_TELEMETRY_REMOTE` | `off`                | Remote telemetry exporter: `off` or `posthog`.                                                 |
| `AO_TELEMETRY_POSTHOG_KEY` | unset           | Remote telemetry project key.                                                                  |
| `AO_TELEMETRY_POSTHOG_HOST` | `https://us.i.posthog.com` | Remote telemetry ingestion host.                                             |
| `AO_TELEMETRY_DISABLED_EVENTS` | unset        | Comma-separated telemetry event-stream kill switch.                                            |
| `AO_KEEP_DAEMON`      | unset (off)          | Keep the desktop app's daemon running after the window closes; stop only via `ao stop`. (fork) |

The optional YAML file has this complete schema. All fields other than
`version` are optional; durations use Go duration syntax. `hostInventory.command`
is an argv vector, never a shell command string. An empty command array disables
inventory. `listen` is `loopback` or `unix:<path>`; TCP hosts are never accepted.

```yaml
version: 1
listen: loopback
port: 3001
requestTimeout: 60s
shutdownTimeout: 10s
remoteHostProbeTimeout: 10s
hostInventory:
  command: [inventory, list, --json]
  interval: 30s
  timeout: 10s
  maxOutput: 1048576
runFile: /absolute/path/to/running.json
agent: claude-code
allowedOrigins: [app://renderer]
telemetry:
  events: false
  metrics: false
  remote: off
  postHogKey: ""
  postHogHost: https://us.i.posthog.com
  disabledEvents: []
```

The default daemon listener always binds `127.0.0.1`; no TCP host override is
available because the daemon has no authentication, CORS boundary, or TLS for
public traffic. `AO_LISTEN=unix:/absolute/path` instead selects a Unix-domain
socket. A Unix socket is a stronger boundary than loopback TCP: it has no
network reachability and access is controlled by filesystem permissions. The
daemon creates a missing socket parent directory with mode `0700` and sets the
socket file to mode `0600`. `running.json` records `socketPath` instead of a
TCP port for this mode.

## Manual smoke test

```bash
cd backend
go build -o /tmp/ao ./cmd/ao

tmp=$(mktemp -d)
export AO_RUN_FILE="$tmp/running.json"
export AO_DATA_DIR="$tmp/data"
export AO_PORT=3037

/tmp/ao status --json
/tmp/ao doctor
/tmp/ao start
/tmp/ao status --json
/tmp/ao stop
/tmp/ao status --json
rm -rf "$tmp"
```

## Adding new commands

Add a product command only when a daemon HTTP route owns the corresponding
mutation/read; the CLI must call that route rather than reimplementing daemon
behavior. Commands not yet exposed but with backend routes in place include
`ao events ...` (over the CDC/SSE endpoint) and CLI parity for PR/review
actions.

Do not port old in-process TypeScript CLI behavior that mixed command handling
with storage and runtime implementation details.
