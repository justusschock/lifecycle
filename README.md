# lifecycle

`lifecycle` is a restartable lifecycle engine for Go. You describe phases
with dependencies and idempotent enter, exit and rollback actions; the
library validates the graph, runs independent phases concurrently, persists
every action boundary, and resumes an interrupted run without repeating
completed work. It is for services that start and stop dependent resources
in order, for workflows that must survive crashes, and for tools that need
DAG validation. A separate `handoff` package moves state between owners
through integrity-checked artifacts.

## Features

- DAG validation: duplicates, unknown or self dependencies and cycles are
  rejected up front; stable topological levels and a deterministic digest.
- Automatic `Engine`: entry in dependency order, exit in reverse order,
  rollback of entered phases after an entry failure, bounded parallelism and
  no level barriers.
- Caller-driven `Coordinator` that tells you when a phase may run and
  durably records the outcome.
- Durable state: a snapshot is saved before and after every action through
  a fenced, exclusive `Store`/`Session` contract; crashed runs resume exactly.
- Strict validation of loaded snapshots, including detection of plan changes.
- Deterministic, `errors.Is`-friendly error reporting.
- `memory.Store`, an in-memory reference store.
- `handoff.Protocol`: generic export → send → receive → adopt with SHA-256
  content identity checked at every boundary.

## Installation

```sh
go get github.com/justusschock/lifecycle@latest
```

Requires Go 1.25 or newer. The only dependency outside the standard library
is [`github.com/justusschock/concurrency`](https://github.com/justusschock/concurrency).

## Quick start

The package example from [`example_test.go`](example_test.go) as a program:

```go
package main

import (
	"context"
	"fmt"

	lifecycle "github.com/justusschock/lifecycle"
	"github.com/justusschock/lifecycle/memory"
)

func main() {
	step := func(message string) lifecycle.Action {
		return func(context.Context) error {
			fmt.Println(message)
			return nil
		}
	}
	plan, err := lifecycle.NewPlan([]lifecycle.Phase{
		{Name: "storage", Enter: step("open storage"), Exit: step("close storage"), Rollback: step("discard storage")},
		{Name: "workers", DependsOn: []string{"storage"}, Enter: step("start workers"), Exit: step("stop workers"), Rollback: step("discard workers")},
		{Name: "http", DependsOn: []string{"workers"}, Enter: step("listen"), Exit: step("stop listening"), Rollback: step("discard listener")},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	engine, err := lifecycle.NewEngine(plan, memory.NewStore(), lifecycle.Options{MaxParallel: 2})
	if err != nil {
		fmt.Println(err)
		return
	}
	snapshot, err := engine.Run(context.Background(), "server-1")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(snapshot.Status)
}
```

Output:

```text
open storage
start workers
listen
stop listening
stop workers
close storage
succeeded
```

## Packages

| Package | Purpose |
|---|---|
| [`github.com/justusschock/lifecycle`](https://pkg.go.dev/github.com/justusschock/lifecycle) | Plans, topologies, `Engine`, `Coordinator`, snapshots, the `Store`/`Session` contract and sentinel errors. |
| [`github.com/justusschock/lifecycle/memory`](https://pkg.go.dev/github.com/justusschock/lifecycle/memory) | In-memory `Store` for tests and process-local workflows. |
| [`github.com/justusschock/lifecycle/handoff`](https://pkg.go.dev/github.com/justusschock/lifecycle/handoff) | Integrity-checked artifacts and the generic transfer protocol. |

Packages under `internal/` are not part of the API and may change in any
release.

## Usage

### Plans and topologies

A `Phase` has a unique name, zero or more dependencies and three idempotent
actions (`Enter`, `Exit`, `Rollback`). `NewPlan` rejects empty names,
duplicate phases, unknown, duplicate or self dependencies, nil actions and
cycles. Input order does not matter: the plan computes stable topological
levels and sorts phase names lexically within each level, so multiple roots,
forks and joins need no special API. The plan owns copies of all dependency
slices, and `Levels` returns fresh copies.

`NewTopology` applies the same rules to callback-free `PhaseSpec` values.
Use it to validate a graph for your own scheduler, or with a `Coordinator`.

`Digest` is a deterministic SHA-256 digest of the phase names and sorted
dependencies. Persisted runs record it to detect a changed plan. Actions are
not part of the digest: if an action change is incompatible with existing
snapshots, encode a version in the phase name or migrate the snapshots.

### Running a lifecycle with Engine

`Engine.Run` performs a complete lifecycle for one run ID:

1. Admit lexically sorted phases whose dependencies have durably entered.
2. Start a dependent as soon as its own dependencies finish, without waiting
   for unrelated phases in the same level.
3. After every entry succeeded, exit each phase once all of its dependents
   have durably finished exiting (successfully or not).
4. If an entry fails, durably move the run to `rolling_back`, cancel running
   peers with the failure as cause, admit no new entries, and roll back the
   successfully entered phases in reverse dependency order.

At most `Options.MaxParallel` actions run at once; zero means 1 and negative
values are rejected. Exit and rollback are best-effort: a failed cleanup
action is recorded with a stage-specific status and does not stop cleanup of
other phases. The run becomes `failed` once all cleanup has resolved. An
exit failure does not trigger rollback.

### Driving phases yourself with Coordinator

A `Coordinator` owns the fenced session and durable state of one run.
`EnterPhase` blocks until dependencies have durably entered and capacity is
available. The returned `Execution` reports through `ShouldRun` whether the
work still has to be done, exposes a cancellation-aware `Context`, and must
be finished with `Complete`:

```go
topology, err := lifecycle.NewTopology([]lifecycle.PhaseSpec{
	{Name: "prepare"},
	{Name: "publish", DependsOn: []string{"prepare"}},
})
if err != nil {
	return err
}
coordinator, err := lifecycle.OpenCoordinator(ctx, topology, store, "deployment-42", lifecycle.Options{})
if err != nil {
	return err
}
defer coordinator.Close(context.WithoutCancel(ctx))

execution, err := coordinator.EnterPhase(ctx, "prepare")
if err != nil {
	return err
}
if execution.ShouldRun() {
	if err := execution.Complete(ctx, prepare(execution.Context())); err != nil {
		return err
	}
}
```

Direct users own the traversal: after all entries succeed, call `ExitPhase`
in reverse dependency order; after an entry failure, retry any durable
`entering` work and call `RollbackPhase` in reverse dependency order.
`ExitPhase` is unavailable until every entry has resolved, which keeps every
persisted snapshot recoverable.

### Persistence and recovery

The engine saves a snapshot before every action (in-progress status,
incremented `Attempts`) and after it (completed or stage-specific failure).
The in-memory state advances only after `Save` succeeds: if the pre-action
save fails, the action is not called; if a later save fails, the durable
state stays restartable and `Run` returns the last durably saved snapshot
with the error.

Actions must be idempotent: a process can crash after an action's side
effects but before its completion is durable, so an in-progress action is
retried when the run resumes. Completed actions are never repeated.

| Durable phase status | Recovery behaviour |
| --- | --- |
| `pending` | Run `Enter` when all dependencies are durably `entered`. |
| `entering` | Retry `Enter`. |
| `entered` | Skip `Enter`; later run `Exit`, or `Rollback` after an entry failure. |
| `enter_failed` | Preserve the entry failure while outstanding rollback completes. |
| `exiting` | Retry `Exit`. |
| `exited` | Skip `Enter` and `Exit`. |
| `exit_failed` | Preserve the exit failure and continue cleanup toward dependencies. |
| `rolling_back` | Retry `Rollback`. |
| `rolled_back` | Skip `Rollback`. |
| `rollback_failed` | Preserve the rollback failure and continue cleanup toward dependencies. |

Each status has a `PhaseStatus` constant, from `PhasePending` to
`PhaseRollbackFailed`. `PhaseFailed` is a deprecated alias of
`PhaseEnterFailed` kept for compatibility; new code should use
`PhaseEnterFailed`.

A recovered run in `rolling_back` retries every durable `entering` phase
before rollback begins. Loaded snapshots are validated before any action
runs: run ID, plan digest, exact phase set, statuses, attempt counts, error
text, reachable run state and dependency ordering. A plan mismatch returns
`ErrPlanChanged` joined with `ErrInvalidSnapshot`; a terminal run
(`succeeded`, `rolled_back`, `failed`) returns `ErrRunTerminal`.

Each `PhaseState` records `Status`, `Attempts` (actions started across
entry, exit and rollback), `LastError` (text of the last persisted action
error) and `UpdatedAt` (time of the last transition, from `Options.Clock`,
or `time.Now` when it is nil).

### Store contract

```go
type Store interface {
	Acquire(ctx context.Context, runID string) (lifecycle.Session, error)
}

type Session interface {
	RunID() string
	Load(context.Context) (lifecycle.Snapshot, error)
	Save(context.Context, lifecycle.Snapshot) error
	Release(context.Context) error
}
```

`Acquire` establishes one exclusive owner for the run across every process
using the backend and returns `ErrRunOwned` while another owner is active.
Each session carries a fencing generation: once released or superseded, its
operations, especially `Save`, fail with `ErrOwnershipLost`. `Load` returns
`ErrSnapshotNotFound` for a new run. `Save` atomically replaces the whole
snapshot, acknowledges only once it is recoverable, and must not retain the
caller's map. The engine releases with a context that is independent of the
cancellation of `Run`, so cancelling an action does not strand ownership.

The fence protects snapshot writes, not external side effects. Callbacks
that change another system must carry that system's own fencing token.

`memory.Store` implements this contract in process: it clones snapshots at
every boundary and fences stale sessions, and its zero value is ready to
use. It is not durable and cannot coordinate separate processes; production
backends need a lease or transaction and a durable fencing token.
`Store.Load` and `Store.Save` are conveniences for inspecting and seeding a
run that no session owns.

### Errors

| Error | Meaning |
| --- | --- |
| `ErrInvalidPhase` | A phase, option, run ID, plan, topology or store is invalid, or a stage operation is not available in the run's state. |
| `ErrDuplicatePhase` | More than one phase uses the same name. |
| `ErrUnknownDependency` | A dependency name is not in the plan. |
| `ErrDependencyFailed` | A phase cannot enter because a dependency failed entry; joined with that failure. |
| `ErrCycle` | The graph contains a cycle or self-dependency. |
| `ErrSnapshotNotFound` | The store has no snapshot for the run. |
| `ErrInvalidSnapshot` | A loaded or saved snapshot violates the lifecycle state contract. |
| `ErrPlanChanged` | A snapshot's digest differs from the plan; also matches `ErrInvalidSnapshot`. |
| `ErrRunTerminal` | The run has already reached a terminal state. |
| `ErrRunOwned` | Another session owns the run. |
| `ErrOwnershipLost` | A released or superseded session, or a closed `Coordinator`, was used. |
| `ErrInvalidArtifact` | A handoff artifact, receipt or collaborator is structurally invalid. |
| `ErrIntegrity` | Artifact data, digest, ID or receipt identity do not agree. |

### Handoff

```go
type Exporter[S any] interface {
	Export(context.Context, S) (handoff.Artifact, error)
}

type Transport interface {
	Send(context.Context, handoff.Artifact) (handoff.Receipt, error)
	Receive(context.Context, handoff.Receipt) (handoff.Artifact, error)
}

type Adopter[D any] interface {
	Adopt(context.Context, handoff.Artifact) (D, error)
}
```

`Protocol.Transfer` runs `Export` → `Send` → `Receive` → `Adopt`. It
validates the artifact after export and after receive, checks the receipt,
and requires the exported, receipt and received artifact IDs to match.
`Adopt` is never called after an integrity failure. Errors are wrapped with
`handoff export`, `handoff send`, `handoff receive` or `handoff adopt`.

An artifact's `Digest` is SHA-256 over a canonical, length-prefixed envelope
of media type, version, data and metadata sorted by key; `ID` is its hex
encoding. `NewArtifact`, `Clone` and the protocol copy byte slices and maps
at every boundary. Because artifact fields are exported, call `Validate`
after modifying one.

```go
artifact, err := handoff.NewArtifact("application/octet-stream", 1, []byte("state"),
	map[string]string{"schema": "example/v1"})
if err != nil {
	return err
}
if err := artifact.Validate(); err != nil {
	return err
}
```

## Design and guarantees

- **Concurrency.** `Plan`, `Topology`, `Engine`, `Coordinator`, `Execution`,
  `memory.Store` and `handoff.Protocol` (given safe collaborators) are safe
  for concurrent use. Store calls for one run are serialised. Two concurrent
  `Run` calls for the same run do not queue: one owns the run and the other
  gets `ErrRunOwned`.
- **Cancellation.** Blocking calls honour their context and return its
  cause. Running actions receive a context that is cancelled with the cause
  of an entry failure, a persistence failure or `Coordinator.Close`.
- **Errors.** Use `errors.Is`: errors are wrapped and joined. Joined errors
  are ordered by topological level, then by phase name. Live action errors
  keep their identity; errors recovered after a restart keep only their
  persisted text.
- **No logging, no global state.** Results are reported through return
  values only.
- **Not provided:** a workflow DSL, timers or schedules, a durable store,
  retries beyond restart recovery, inferred compensation, domain-specific
  states, artifact encryption or transport, or panic recovery for callbacks.
  Callers own action idempotency, store selection and retry policy.
- **Stability.** The module is at `v0.x`: minor releases may break the API.
  Every break is listed in [CHANGELOG.md](CHANGELOG.md).

## Documentation

- API reference: [pkg.go.dev/github.com/justusschock/lifecycle](https://pkg.go.dev/github.com/justusschock/lifecycle)
- Requirements and design decisions: [docs/README.md](docs/README.md)
- Changes: [CHANGELOG.md](CHANGELOG.md)

## Contributing

Contributions are made through pull requests against `main`; nothing is
pushed to `main` directly. Read [AGENTS.md](AGENTS.md) first: it covers the
docs-first process, API rules, testing requirements and the checks to run
before opening a pull request.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) and
[NOTICE](NOTICE).
