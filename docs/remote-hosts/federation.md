# Federation boundary

## Local daemon is the federation point

Federation belongs in the local daemon behind its existing API. The frontend
continues to address one daemon, while the local daemon lists locally owned and
remotely owned sessions together and proxies a session-scoped request to the
daemon named by the session's host.

This placement preserves the frontend's one-base-url model. Today its query
keys and routes use a bare session id, terminal WebSockets derive from that one
base, and browser previews are indexed by session id alone. Requiring the
frontend to thread host identity through each of those surfaces would make the
host topology a UI concern. It is instead resolved at the daemon API boundary.

A separate gateway process is intentionally excluded. It would add another
deployable component, data directory, run file, and port to coordinate with a
daemon that already owns those operational responsibilities, without adding a
new capability.

```mermaid
sequenceDiagram
    participant UI as Electron frontend
    participant Local as Local daemon
    participant Forward as Forwarded path
    participant Remote as Remote daemon

    UI->>Local: list sessions
    Local->>Remote: list remote sessions
    Remote-->>Local: remote session read models
    Local-->>UI: sessions with composite identity

    UI->>Local: send or steer (hostId, sessionId)
    Local->>Forward: proxy session-scoped request
    Forward->>Remote: daemon API request
    Remote-->>Forward: result
    Forward-->>Local: result
    Local-->>UI: result
```

## Composite identity

Session ids are assigned as `{project}-{num}` and are unique only within the
daemon that assigned them. A federated session is therefore identified as
`(hostId, sessionId)`, with the local daemon also having a stable local host id.
The pair is the identity used for aggregation, routing, cache invalidation, and
terminal attachment. Bare ids may remain an implementation detail inside an
owning daemon, but must never select a session across the federation.

This prevents a session on one host from shadowing a same-named session on
another host and prevents a terminal stream from being routed to the wrong
machine.

## Reads, writes, and events

Federation is not a read-only board. For a remote session, the local daemon
proxies the same session-scoped operations that it accepts for a local session:

- Session reads and derived display status.
- Prompts, sends, chat steering, input resolution, and interruption.
- Lifecycle operations supported by the owning daemon.
- SSE-backed changes and notification visibility.
- `/mux` terminal attachment, input, resize, and output.

Each remote daemon remains the source of truth for its durable session facts.
The local daemon consumes its session reads and SSE changes, qualifies them by
`hostId`, and publishes a single local event surface for the frontend. It does
not copy a remote daemon's SQLite state into its own database or recompute a
remote session's display status from partial facts.

```mermaid
flowchart LR
    Hook[Agent lifecycle hook] -->|loopback HTTP| RemoteLCM[Remote lifecycle manager]
    RemoteLCM -->|write durable activity_state| RemoteDB[(Remote SQLite)]
    RemoteDB -->|CDC| RemoteSSE[Remote SSE]
    RemoteSSE -->|forwarded path| LocalFederation[Local federation]
    LocalFederation -->|host-qualified event| LocalSSE[Local SSE]
    LocalSSE --> UI[Frontend]

    RemoteDB --> RemoteService[Remote session service]
    RemoteService -->|derived display status| LocalFederation
```

`activity_state` follows this same path. It is observed and written only by the
owning remote daemon's lifecycle flow, then carried as part of its read model
and events. The local daemon displays the owning daemon's derived display
status; it does not invent a separate remote status reducer.

## Terminal paths

The normal desktop terminal path remains available through the local daemon's
`/mux` endpoint. Once a terminal attachment is associated with a composite
identity, the local daemon relays the terminal protocol to the owning daemon's
`/mux` endpoint over the forwarded path. This keeps the frontend on one
WebSocket base while preserving terminal I/O for remote sessions.

That relay is not the only terminal path, and it must never become a required
intermediary. **Direct access survives the abstraction: any session, local or
remote, can be opened in a real terminal and talked to without the orchestrator
mediating.** A remote host's normal machine-level terminal access can reach its
local runtime directly; federation must not remove or replace that capability.

The current daemon distinguishes TUI sessions, which run an agent in a terminal
runtime, from Chat sessions, which do not have an agent terminal runtime. The
invariant requires the eventual remote-host API to make the available direct
terminal surface explicit for every session mode rather than implying that all
sessions already expose an agent tmux attachment.
