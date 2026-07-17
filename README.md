# go-lifecycle

`go-lifecycle` provides a restartable lifecycle engine for arbitrary directed
acyclic graphs (DAGs) and a generic, integrity-checked handoff protocol. It is a
private Go module intended for reuse across projects.

The engine validates lifecycle topology up front, executes independent phases
with bounded concurrency, persists every action boundary, and recovers
interrupted entry, exit, or rollback work without repeating completed actions.
The `handoff` package composes export, transport, and adoption without adding
resource-specific ownership semantics.

Module path: `github.com/justusschock/go-lifecycle`

## Requirements and installation

Go 1.26 or newer is required. The only non-standard dependency is
[`github.com/justusschock/go-concurrency`](https://github.com/justusschock/go-concurrency),
pinned to `v0.1.0`.

Configure private-module access through normal GitHub authentication before
installing the module. Do not put credentials in commands, source files, or Go
environment settings.

```bash
go env -w GOPRIVATE='github.com/justusschock/*'
go get github.com/justusschock/go-lifecycle@v0.1.0
```

## Lifecycle plans

A `Phase` has a unique name, zero or more named dependencies, and three
idempotent actions:

```go
type Phase struct {
	Name       string
	DependsOn  []string
	Enter      lifecycle.Action
	Exit       lifecycle.Action
	Rollback   lifecycle.Action
}
```

`NewPlan` rejects empty names, duplicate phase names, missing dependencies,
duplicate or self dependencies, nil actions, and cycles. Input order does not
affect execution order: the plan computes stable topological levels, sorting
phase names lexically within every level. This supports multiple roots,
arbitrary forks, partial joins, repeated forks, and final joins without a
special branching API.

The plan owns copies of all dependency slices. `Levels` returns fresh copies, so
callers cannot mutate the validated graph through either the original input or
returned values.

```go
noop := func(context.Context) error { return nil }
plan, err := lifecycle.NewPlan([]lifecycle.Phase{
	{Name: "prepare", Enter: noop, Exit: noop, Rollback: noop},
	{Name: "left", DependsOn: []string{"prepare"}, Enter: noop, Exit: noop, Rollback: noop},
	{Name: "right", DependsOn: []string{"prepare"}, Enter: noop, Exit: noop, Rollback: noop},
	{Name: "publish", DependsOn: []string{"left", "right"}, Enter: noop, Exit: noop, Rollback: noop},
})
if err != nil {
	return err
}
```

### Plan digest

`Plan.Digest` is a deterministic SHA-256 digest of canonical phase names and
sorted dependency names. It detects topology changes when a persisted run is
resumed with a different plan.

Function values are deliberately excluded. Changing an action implementation
without changing phase names or dependencies does not change the digest. If an
action change is incompatible with existing snapshots, encode that version in
the phase name or otherwise migrate the stored snapshots explicitly.

## Running a lifecycle

Create an engine with a validated plan, a snapshot store, and an optional
parallelism limit:

```go
store := memory.NewStore()
engine, err := lifecycle.NewEngine(plan, store, lifecycle.Options{
	MaxParallel: 4,
})
if err != nil {
	return err
}

snapshot, err := engine.Run(context.Background(), "deployment-42")
if err != nil {
	return err
}
fmt.Println(snapshot.Status) // succeeded
```

`Run` performs a complete lifecycle:

1. Admit lexically sorted phases whose dependencies have durably entered.
2. Start a dependent as soon as its own dependencies finish, without waiting
   for unrelated phases in the same topological level.
3. After successful entry, exit phases when all of their dependents have
   durably resolved.
4. If entry fails, atomically move the run to `rolling_back`, stop admitting
   new entry work, and roll back successfully entered phases using the same
   reverse-dependency readiness rule.

At most `MaxParallel` callbacks run concurrently. Admission is lexical among
the phases ready at each scheduling decision; error joins use stable
topological-level and lexical order rather than callback completion order.
`Plan.Levels` remains useful for graph introspection but is not an execution
barrier.

Exit and rollback are durable best-effort traversals. A failed cleanup action
is recorded with stage-specific provenance and counts as resolved for admitting
its ancestors, so it does not suppress cleanup of a sibling or dependency. The
run becomes `failed` only after every eligible cleanup action is durably
resolved. An exit failure does not synthesize a rollback.

`MaxParallel` defaults to `1` when zero. Negative values are rejected. `Clock`
defaults to `time.Now` and can be replaced for deterministic timestamps.
`NewEngine` rejects nil or uninitialized plans and nil stores, including stores
held in a typed nil interface. A canonical empty plan created by `NewPlan(nil)`
is valid.

## Persistence and recovery

The engine saves a snapshot before and after every action. The pre-action save
sets the in-progress status and increments `Attempts`; the post-action save
records completion or a stage-specific failure. Store calls are serialized
even while independent phase callbacks run concurrently.

Actions must be idempotent. A process can crash after an action's side effects
but before its completed status is durable, so an in-progress action is retried
once the run resumes. Completed actions are not repeated.

| Durable phase status | Recovery behavior |
| --- | --- |
| `pending` | Run `Enter` when all dependencies are durably `entered`. |
| `entering` | Retry `Enter`. |
| `entered` | Skip `Enter`; later run `Exit`, or `Rollback` after entry failure. |
| `enter_failed` | Preserve the entry failure while outstanding rollback completes. |
| `exiting` | Retry `Exit`. |
| `exited` | Skip `Enter` and `Exit`. |
| `exit_failed` | Preserve the exit failure and continue cleanup toward dependencies. |
| `rolling_back` | Retry `Rollback`. |
| `rolled_back` | Skip `Rollback`. |
| `rollback_failed` | Preserve the rollback failure and continue cleanup toward dependencies. |

When a recovered run is already `rolling_back`, every durable `entering` phase
is retried before rollback scheduling begins. Phases that durably become
`entered` are then rolled back. Entry errors already recorded in the snapshot
are returned again in deterministic order.

The in-memory runner commits a transition only after `Store.Save` succeeds. If
persistence fails before an action, that action is not called. If persistence
fails after an action, the durable phase remains in progress and is therefore
restartable. The engine returns the persistence error and the last successfully
saved snapshot without incorrectly advancing the run to a terminal state.

Loaded snapshots are validated before any action runs: the requested run ID,
plan digest, exact planned phase set, phase and run statuses, reachable run
state, and terminal-state consistency must all agree. A `running` snapshot must
contain an admitted phase. Every `rolling_back`, `rolled_back`, or
rollback-derived `failed` snapshot must retain an `enter_failed` phase as the
durable reason rollback began. A plan mismatch returns both
`ErrInvalidSnapshot` and `ErrPlanChanged` through `errors.Is`. A `succeeded`
snapshot is valid only when every phase is `exited`. Calling `Run` for a valid
run already in `succeeded`, `rolled_back`, or `failed` state returns
`ErrRunTerminal`.

### Store contract

Implementations acquire a fenced, run-bound session:

```go
type Store interface {
	Acquire(context.Context, string) (lifecycle.Session, error)
}

type Session interface {
	RunID() string
	Load(context.Context) (lifecycle.Snapshot, error)
	Save(context.Context, lifecycle.Snapshot) error
	Release(context.Context) error
}
```

`Acquire` must establish one exclusive owner for that run across every process
using the backend. It returns `ErrRunOwned` while another owner is active. Each
session carries a monotonically fenced ownership generation: after release or
supersession, its operations—especially `Save`—must fail with
`ErrOwnershipLost`. A session may load and save only its reported `RunID`; a
cross-run snapshot is invalid.

The session fence protects ownership and persistence of lifecycle snapshots.
It does not fence arbitrary external side effects from a callback after an
external lease is superseded. Callbacks that mutate another system must also
propagate and validate that system's fencing token, or enforce equivalent
domain-level ownership.

`Session.Load` must return `ErrSnapshotNotFound` for an absent run. `Save` must
atomically replace the complete snapshot and durably acknowledge only after the
replacement is recoverable. Implementations must not retain or mutate
caller-owned phase maps. The engine always releases with a context independent
of cancellation of `Run`, so canceling an action does not strand ownership.

Two concurrent calls for the same run do not serialize inside `Engine`: one
owns the backend session and the other receives `ErrRunOwned`. This contract is
part of `Store`; process-local engine mutexes are not sufficient for a durable
or distributed implementation.

`memory.NewStore` is a mutex-protected reference implementation. It clones phase
maps on every boundary, fences stale sessions, and is useful for tests and
process-local workflows. It is not durable and cannot coordinate separate
processes. Production backends must implement the same session contract with a
portable lease or transaction and durable fencing token.

## Status and diagnostics

`Snapshot` contains the run ID, plan digest, run status, per-phase state, and
last update time. Each `PhaseState` records:

- `Status`: the durable state-machine position;
- `Attempts`: the number of actions started for that phase across entry, exit,
  and rollback;
- `LastError`: the most recently persisted action error text; and
- `UpdatedAt`: the transition timestamp.

Stable sentinel errors are exported from the root package:

| Error | Meaning |
| --- | --- |
| `ErrInvalidPhase` | A phase, engine option, run ID, plan, or store is invalid. |
| `ErrDuplicatePhase` | More than one phase uses the same name. |
| `ErrUnknownDependency` | A dependency name is absent from the plan. |
| `ErrCycle` | The graph contains a cycle or self-dependency. |
| `ErrSnapshotNotFound` | A store has no snapshot for the requested run. |
| `ErrInvalidSnapshot` | A loaded snapshot or run-bound save violates the lifecycle state contract. |
| `ErrPlanChanged` | A snapshot's digest differs from the supplied plan; also matches `ErrInvalidSnapshot`. |
| `ErrRunTerminal` | The requested run has already reached a terminal state. |
| `ErrRunOwned` | Another session exclusively owns the requested run. |
| `ErrOwnershipLost` | A released or superseded session attempted an operation. |
| `ErrInvalidArtifact` | A handoff artifact, receipt, or protocol collaborator is structurally invalid. |
| `ErrIntegrity` | Artifact data, digest, ID, or receipt identity does not agree. |

Use `errors.Is`, because action, rollback, handoff-stage, and persistence errors
may be wrapped or joined. A live action error preserves its Go error identity.
After a restart, only the persisted `LastError` string is available, so the
reconstructed error preserves text and deterministic ordering but cannot
preserve the original value's `errors.Is` identity.

## Generic handoff

The `handoff` subpackage moves generic state through three explicit roles:

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

`Protocol.Transfer` performs `Export` → `Send` → `Receive` → `Adopt`. It
validates the artifact after export and receive, validates receipt fields, and
requires the exported artifact, receipt, and received artifact IDs to match.
Adoption is never called after an integrity failure.

Artifacts use a canonical SHA-256 semantic-envelope digest as both their digest
and hex-encoded ID. The canonical input length-prefixes media type, version,
data, metadata count, and metadata keys and values sorted by key. Changing any
of those fields therefore changes identity, while Go map iteration order does
not. `NewArtifact`, `Clone`, and the protocol copy byte slices and metadata maps
at every boundary so collaborators do not share mutable backing storage. Since
artifact fields are exported, callers that modify an artifact must call
`Validate` before trusting or forwarding it.

```go
artifact, err := handoff.NewArtifact(
	"application/octet-stream",
	1,
	[]byte("state"),
	map[string]string{"schema": "example/v1"},
)
if err != nil {
	return err
}
if err := artifact.Validate(); err != nil {
	return err
}
```

`NewProtocol` rejects nil collaborators, including typed nil interfaces.
Transfer errors are wrapped with `handoff export`, `handoff send`, `handoff
receive`, or `handoff adopt`, while preserving the underlying error for
`errors.Is`.

This protocol transfers generic lifecycle artifacts. Leasing or transferring a
finite resource right is deliberately excluded and belongs in
`go-resources/handoff`.

## Scope and non-goals

This release intentionally does not provide:

- a workflow DSL, timers, schedules, or long-running orchestration;
- a durable or distributed `Store` implementation (the interface nevertheless
  requires exclusive fenced sessions from one);
- retries beyond restart recovery of interrupted idempotent actions;
- synthesized compensation or inferred rollback actions;
- domain-specific lifecycle states or resource ownership semantics;
- artifact encryption, authentication, storage, or transport implementations;
  or
- panic recovery for user callbacks.

Callers own action idempotency, durable-store selection, context lifetimes,
domain-specific adapters, security policy, and retry policy around terminal
failures.

## Development

Run formatting, static analysis, race tests, and repository hooks with Go 1.26
or newer:

```bash
gofmt -w *.go memory/*.go handoff/*.go
go vet ./...
go test -race ./...
pre-commit run --all-files
```

The dependency graph should contain only this module and `go-concurrency`:

```bash
go list -m all
```

Local implementation plans live under `docs/`; implementation reports live
under `.superpowers/`. Both are intentionally excluded from Git.

## License

Licensed under the [Apache License 2.0](LICENSE).
