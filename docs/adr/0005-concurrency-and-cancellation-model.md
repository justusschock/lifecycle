# ADR 0005: Concurrency and cancellation model

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 (FR-LIFE-05 to FR-LIFE-08, FR-LIFE-13, FR-LIFE-15), ADR-0006 |

## Context

Recorded retrospectively for the first public release. Independent phases
should run concurrently, but every state change must be persisted in order
and admission decisions must see a consistent snapshot. Callers driving a
`Coordinator` block until a phase is ready. When an entry fails, actions
that are still running must be told to stop. Ownership of a run must be
released even when the caller's context is cancelled.

## Decision drivers

- D1 One consistent view of run state for every admission decision.
- D2 Bounded parallelism with no level barriers.
- D3 Blocking calls honour `context.Context` cancellation.
- D4 Running actions learn why they were cancelled.
- D5 Ownership is never stranded by cancellation.

## Considered options

1. **One mutex per `Coordinator` guarding the snapshot, with a broadcast
   channel that is closed and replaced on every change**
   - Pro: simple, linearisable state changes; store calls are serialised
     under the lock, so the persisted order equals the in-memory order.
   - Pro: waiters select on the broadcast channel and `ctx.Done()`, so they
     wake on any change or on cancellation.
   - Con: store latency is on the critical path of every admission.
2. **Actor goroutine owning the state, driven by request channels**
   - Pro: no shared-memory locking.
   - Con: more goroutines and lifecycle management; harder to reason about
     shutdown and cancellation of pending requests.
3. **Level-by-level execution with `sync.WaitGroup` barriers**
   - Pro: trivial to implement.
   - Con: a slow phase delays unrelated dependents; violates D2.

## Decision

We will guard each `Coordinator`'s snapshot with a single mutex and signal
changes by closing and replacing a channel. Admission checks readiness,
the `MaxParallel` bound (zero means 1) and duplicate admission under the
lock, persists the in-progress state, and only then returns an `Execution`.
Each `Execution` has a context derived from the admission context with
`context.WithCancelCause`; an entry failure cancels active executions with
the failure as cause, a persistence failure cancels them with the
persistence error, and `Close` cancels them with `ErrOwnershipLost`.
`Engine` admits ready phases in lexical order, runs each admitted action
through `concurrency.Go`, and re-evaluates readiness after every
completion. `Engine.Run` releases its session with
`context.WithoutCancel`, and `Coordinator.Close` can be retried after a
failed release. Callback panics are not recovered.

## Consequences

### Positive
- `Coordinator`, `Execution`, `Engine` and `memory.Store` are safe for
  concurrent use; tests run with the race detector.
- A newly ready phase starts as soon as its own dependencies finish.

### Negative
- Store calls are serialised per run; a slow store limits throughput.
- A panicking callback crashes the process; recovery is the caller's
  responsibility.

### Follow-ups
- None.

## Links

- [context.WithCancelCause](https://pkg.go.dev/context#WithCancelCause)
