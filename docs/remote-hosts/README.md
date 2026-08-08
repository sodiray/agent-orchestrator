# Remote Hosts

Remote hosts extend one local Agent Orchestrator daemon with sessions owned by
other, independently running daemons. This is a high-level architecture record:
implementation detail remains in code and the existing architecture documents.

## Reading order

| Doc | What it covers |
| --- | --- |
| [model.md](model.md) | The remote-host model, co-location rule, and reachability boundary. |
| [federation.md](federation.md) | Identity, local-daemon federation, API proxying, events, activity, and terminals. |
| [operations.md](operations.md) | Durable state, availability states, and operator responsibilities. |

## Invariants

- A remote host is a complete, self-consistent daemon installation, not a
  remote runtime controlled by another daemon.
- Runtime, workspace, source control, agent process, and daemon are co-located
  on the machine that owns a session.
- Federation is read and write: it carries session reads, prompts, steering,
  interruption, events, and terminal I/O.
- Session identity across the federation is `(hostId, sessionId)`.
- Direct access survives the abstraction: any session, local or remote, can be
  opened in a real terminal and talked to without the orchestrator mediating.

## Scope

These documents describe the intended architecture. They do not introduce a
remote runtime adapter, a network listener beyond the daemon listeners already
defined by the daemon architecture, or a new standalone gateway process.
