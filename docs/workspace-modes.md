# Session workspace modes

`ao spawn` creates an isolated workspace by default. For a Git project this is
an AO-managed worktree on a session branch, so parallel sessions do not edit
the same checkout.

Use `--workspace-mode project-root` only for a dedicated operator machine where
the registered project directory is itself the working environment and
long-running services already serve from that checkout. In this mode AO starts
the session in the project directory without creating a worktree or creating or
switching a branch. The session therefore works on whichever branch the
checkout already has.

Only one active `project-root` session is allowed per project. Two agents in
the same directory can race on source files, generated output, and local
services, so AO refuses the second spawn. `--branch` is incompatible with this
mode. Scratch projects also reject it because their AO-managed directory is
already the session workspace.

`ao session cleanup` never reclaims a project-root directory. AO persists a
workspace marker for these sessions and treats the directory as operator-owned
on every cleanup, kill, rollback, and daemon-recovery path.

Claude Code's project-scoped settings, including AO's activity hooks, are also
shared in `project-root` mode. AO merges its entries with existing settings,
but every Claude Code process started from that directory sees them. Hooks from
sessions AO does not own exit silently without reporting activity.
