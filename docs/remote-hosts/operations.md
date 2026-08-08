# Remote-host operation and failure model

## Durable placement

A container recreation destroys paths that are not mounted on durable storage.
For a daemon in a container, the operator must explicitly place its state on
durable storage rather than relying on process defaults.

The current daemon configuration uses these environment variables:

| Variable | Role | Remote-host requirement |
| --- | --- | --- |
| `AO_DATA_DIR` | Durable daemon data directory; default `~/.ao/data` | Set it to a durable mounted path. It protects the board, session database, worktrees, prompts, and other daemon-managed state. |
| `AO_RUN_FILE` | Daemon run-file path; default `~/.ao/running.json` | Set it to a durable mounted path compatible with the daemon data placement. |
| `HOME` | Default home used by the daemon and agent tooling | Make it durable when agent or tool configuration belongs there. |
| `CODEX_HOME` | Location consulted for Codex-managed state | Make it durable when that harness is in use. |

The variable names above are the names currently recognized by the code. The
defaults under `~/.ao` are safe only when that home directory itself is durable.
Persisting only the source checkout is insufficient: recovery also needs the
daemon's database, worktree records, and session prompt state.

```mermaid
flowchart TD
    Container[Remote container] --> Daemon[Daemon]
    Daemon --> Data[AO_DATA_DIR]
    Daemon --> Run[AO_RUN_FILE]
    Agent[Agent binaries] --> Home[HOME]
    Agent --> CodexHome[CODEX_HOME]

    Data --> Durable[Durable storage]
    Run --> Durable
    Home --> Durable
    CodexHome --> Durable
```

The container image also carries tmux and the selected agent binaries. Those
are execution prerequisites, not a replacement for persistent state.

## Host availability is visible state

A remote host can become unreachable. Its sessions must remain represented on
the local board with an explanation rather than disappearing from the list.
Host reachability is an observation about the forwarding path and remote daemon,
not a durable session fact asserted by the remote daemon while it is absent.

| Host state | Meaning | Board behavior |
| --- | --- | --- |
| Available | The local daemon can reach the remote daemon and obtain current data. | Show current remote read models and permit supported operations. |
| Unreachable | The host was registered but cannot currently be reached. | Retain its sessions, mark them gone or unavailable with the reachability explanation, and prevent operations that require the owner. |
| Stopped | The remote host is intentionally or temporarily offline while its durable storage survives. | Show the recoverable stopped state. On return, reconnect, refresh the read models, and restore normal operations. |
| Destroyed | The remote host and its durable state are final. | Show a final destroyed state; it is not expected to restore. |

The local daemon must retain enough host-registration and last-known session
metadata to distinguish `stopped` from `destroyed` and to explain why a session
cannot presently be operated. It must not silently turn an unreachable host into
a deleted session, nor infer that its runtime is dead from a failed reachability
probe.

## Recovery and operator boundary

When a stopped host returns, its own daemon reconciles its runtimes and durable
facts under the existing lifecycle rules. The local daemon then reconnects,
re-subscribes to SSE, replaces stale aggregated read models, and resumes proxy
routing. It never reconstructs remote worktrees or session state locally.

The operator owns the remote machine lifecycle, durable mounts, agent
credentials and configuration, and the secure forwarding path. The daemon owns
only the contract to an already reachable daemon address and the resulting
federation behavior.
