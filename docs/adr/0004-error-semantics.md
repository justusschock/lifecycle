# ADR 0004: Error semantics

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 (FR-LIFE-12, FR-LIFE-14, FR-LIFE-19) |

## Context

Recorded retrospectively for the first public release. A single `Run` can
produce several errors at once: failures of concurrently running actions,
persistence errors, and errors recovered from a snapshot written by an
earlier process. Callers need to classify errors programmatically and get
the same output for the same failure, regardless of goroutine scheduling.

## Decision drivers

- D1 Errors must be classifiable with `errors.Is`.
- D2 Output must be deterministic for equal inputs.
- D3 Errors from user actions must not lose their identity while live.
- D4 Recovered errors can only be what the snapshot stores: text.

## Considered options

1. **Package-level sentinel errors, `%w` wrapping and `errors.Join`**
   - Pro: idiomatic, works with `errors.Is` through joins and wraps.
   - Pro: several failures are reported together.
   - Con: callers must use `errors.Is`, not `==`.
2. **Typed error structs with codes**
   - Pro: carry structured fields.
   - Con: heavier API; callers match on types for what are simple
     categories.
3. **Return only the first error**
   - Pro: simple.
   - Con: hides cleanup failures that the caller must know about, and the
     "first" error depends on scheduling.

## Decision

We will define sentinel errors in the root package (`ErrInvalidPhase`,
`ErrDuplicatePhase`, `ErrUnknownDependency`, `ErrDependencyFailed`,
`ErrCycle`, `ErrSnapshotNotFound`, `ErrInvalidSnapshot`, `ErrPlanChanged`,
`ErrRunTerminal`, `ErrRunOwned`, `ErrOwnershipLost`, and the handoff errors
`ErrInvalidArtifact` and `ErrIntegrity`), wrap them with `%w` for context,
and combine multiple failures with `errors.Join`. Joined errors are ordered
by topological level, then lexically by phase name. `ErrPlanChanged` is
always joined with `ErrInvalidSnapshot`. Live action errors are returned as
the original values; after a restart the error is reconstructed from the
persisted `LastError` text, preserving text (including empty text) and
order but not identity. Handoff errors are wrapped with the failing stage
(`handoff export`, `handoff send`, `handoff receive`, `handoff adopt`).

## Consequences

### Positive
- Callers write `errors.Is(err, lifecycle.ErrRunOwned)` and similar checks.
- Error output is reproducible in tests and logs.

### Negative
- A recovered error no longer matches the original sentinel of a user
  action.
- The handoff sentinels live in the root package, so `handoff` imports the
  root package; consumers already depend on that location.

### Follow-ups
- PRD-0001 Q2 tracks whether the handoff sentinels should move.

## Links

- [errors package](https://pkg.go.dev/errors)
