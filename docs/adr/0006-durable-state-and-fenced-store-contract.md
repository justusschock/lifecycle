# ADR 0006: Durable state and fenced store contract

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 (FR-LIFE-10 to FR-LIFE-13, FR-LIFE-17), ADR-0005 |

## Context

Recorded retrospectively for the first public release. A process may crash
at any point, including after an action's side effects but before its
completion is recorded. A resumed run must neither skip unfinished work nor
repeat finished work, must detect that the plan changed underneath it, and
must not be driven by two processes at once. The library cannot know which
database or object store an application uses.

## Decision drivers

- D1 Exactly-once completion bookkeeping with at-least-once action
  execution.
- D2 Backend-agnostic persistence.
- D3 Exclusive ownership that survives process boundaries.
- D4 Corrupt or foreign snapshots are rejected before any action runs.

## Considered options

1. **Save the whole snapshot before and after every action through a
   fenced `Store`/`Session` interface; actions must be idempotent**
   - Pro: the in-progress status marks exactly which action to retry.
   - Pro: a fencing generation per session makes stale writers fail with
     `ErrOwnershipLost`.
   - Con: two writes per action; actions must be idempotent.
2. **Append-only event log of transitions**
   - Pro: full history.
   - Con: replay logic and compaction in every backend; more complex
     validation.
3. **Persist only after an action completes**
   - Pro: one write per action.
   - Con: a crash during an action is invisible; the action may be skipped
     or its in-flight state misread.

## Decision

We will persist a complete `Snapshot` (run ID, plan digest, run status,
per-phase `Status`, `Attempts`, `LastError`, `UpdatedAt`) before each action
(in-progress status, incremented attempts) and after it (completed or
stage-specific failure status). The in-memory state advances only after
`Save` succeeds. Backends implement `Store.Acquire`, which returns one
exclusive `Session` per run (`ErrRunOwned` otherwise), and `Session`
methods `Load`, `Save` and `Release`, which must fail with
`ErrOwnershipLost` once the session is released or superseded. `Load`
returns `ErrSnapshotNotFound` for a new run; `Save` atomically replaces the
whole snapshot and must not retain the caller's map. Every loaded snapshot
is validated against the topology (digest, phase set, statuses, attempts,
reachability, dependency ordering) before use. The snapshot is a Go value;
its serialised form is left to each backend. `memory.Store` is the
reference implementation; it is process-local and not durable.

## Consequences

### Positive
- Any backend with an atomic write and a lease or fencing token can host
  runs.
- Plan changes are detected via `ErrPlanChanged`.

### Negative
- The session fence protects snapshot writes, not external side effects;
  callbacks that mutate other systems must carry their own fencing token.
- There is no public conformance suite for backends yet.

### Follow-ups
- PRD-0001 Q1: consider a public Store conformance suite.

## Links

- [Fencing tokens](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html)
