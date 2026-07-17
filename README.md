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

1. Enter levels from roots toward joins.
2. Run siblings in a level concurrently, bounded by `MaxParallel`.
3. After successful entry, exit levels in reverse topological order.
4. If entry fails, cancel its level and roll back successfully entered phases
   in reverse topological order.

Within a level, errors are deterministic by the plan's lexical phase order.
Rollback is best effort: an ordinary rollback error does not prevent other
entered phases from receiving rollback, and all entry and rollback errors are
joined. An exit error ends the run as failed; it does not synthesize a rollback.

`MaxParallel` defaults to `1` when zero. Negative values are rejected. `Clock`
defaults to `time.Now` and can be replaced for deterministic timestamps.
`NewEngine` rejects a nil plan and nil stores, including stores held in a typed
nil interface.

## Persistence and recovery

The engine saves a snapshot before and after every action. The pre-action save
sets the in-progress status and increments `Attempts`; the post-action save
records completion or failure. Store calls are serialized even while phase
callbacks in one level run concurrently.

Actions must be idempotent. A process can crash after an action's side effects
but before its completed status is durable, so an in-progress action is retried
once the run resumes. Completed actions are not repeated.

| Durable phase status | Recovery behavior |
| --- | --- |
| `pending` | Run `Enter` when its level is reached. |
| `entering` | Retry `Enter`. |
| `entered` | Skip `Enter`; later run `Exit`, or `Rollback` after entry failure. |
| `exiting` | Retry `Exit`. |
| `exited` | Skip `Enter` and `Exit`. |
| `rolling_back` | Retry `Rollback`. |
| `rolled_back` | Skip `Rollback`. |
| `failed` | Preserve the completed action failure; the accompanying run status determines terminal or rollback recovery. |

The in-memory runner commits a transition only after `Store.Save` succeeds. If
persistence fails before an action, that action is not called. If persistence
fails after an action, the durable phase remains in progress and is therefore
restartable. The engine returns the persistence error and the last successfully
saved snapshot without incorrectly advancing the run to a terminal state.

Snapshots are tied to `Plan.Digest`. A mismatch returns `ErrPlanChanged` before
any action runs. Calling `Run` for a run already in `succeeded`, `rolled_back`,
or `failed` state returns `ErrRunTerminal`.

### Store contract

Implementations provide two operations:

```go
type Store interface {
	Load(context.Context, string) (lifecycle.Snapshot, error)
	Save(context.Context, lifecycle.Snapshot) error
}
```

`Load` must return `ErrSnapshotNotFound` for an absent run. A production store
should atomically replace the complete snapshot and durably acknowledge `Save`
only after the replacement is recoverable. Implementations should not retain or
mutate caller-owned phase maps.

`memory.NewStore` is a mutex-protected reference implementation. It clones phase
maps on every boundary and is useful for tests and process-local workflows, but
it is not durable across process restarts.

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
| `ErrPlanChanged` | A snapshot's digest differs from the supplied plan. |
| `ErrRunTerminal` | The requested run has already reached a terminal state. |
| `ErrInvalidArtifact` | A handoff artifact, receipt, or protocol collaborator is structurally invalid. |
| `ErrIntegrity` | Artifact data, digest, ID, or receipt identity does not agree. |

Use `errors.Is`, because action, rollback, handoff-stage, and persistence errors
may be wrapped or joined.

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

Artifacts use `SHA-256(Data)` as both their digest and hex-encoded ID. Media
type, version, and metadata are validated or transported but are not part of
the content digest. `NewArtifact`, `Clone`, and the protocol copy byte slices and
metadata maps at every boundary so collaborators do not share mutable backing
storage. Since artifact fields are exported, callers that modify an artifact
must call `Validate` before trusting or forwarding it.

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
- distributed locking or a distributed snapshot-store implementation;
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
