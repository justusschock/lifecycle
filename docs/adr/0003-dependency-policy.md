# ADR 0003: Dependency policy

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 (NFR-LIFE-03) |

## Context

Recorded retrospectively for the first public release. lifecycle is a
low-level building block imported by services and by other libraries
(executor, reconciliation, object-storage). Every dependency it adds
becomes a transitive dependency of all of them.

## Decision drivers

- D1 Keep the transitive dependency graph minimal and auditable.
- D2 Reuse shared concurrency primitives instead of re-implementing them.
- D3 No dependency that pulls in logging, configuration or I/O frameworks.

## Considered options

1. **Standard library plus `github.com/justusschock/concurrency`**
   - Pro: one small, first-party dependency; the engine uses its
     `concurrency.Go` futures to run admitted actions.
   - Con: couples releases to concurrency's API.
2. **Standard library only**
   - Pro: zero dependencies.
   - Con: duplicates goroutine/future helpers already maintained in
     concurrency.
3. **Third-party workflow or DAG libraries**
   - Pro: more features.
   - Con: large transitive graphs, and none provides the fenced, durable
     recovery semantics this library needs.

## Decision

We will depend only on the standard library and
`github.com/justusschock/concurrency`. No third-party module is used.
Adding a dependency requires a new ADR.

## Consequences

### Positive
- `go list -m all` shows two modules; consumers inherit nothing else.

### Negative
- A concurrency breaking change requires a coordinated update.

### Follow-ups
- Dependabot proposes module updates weekly.

## Links

- [concurrency](https://github.com/justusschock/concurrency)
