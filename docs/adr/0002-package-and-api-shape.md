# ADR 0002: Package and API shape

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0005, ADR-0008 |

## Context

Recorded retrospectively for the first public release. The library serves
three kinds of callers: those who want a lifecycle run to completion
automatically, those who want to execute the actions themselves but reuse the
durable state machine, and those who only need DAG validation (for example a
scheduler that keeps its own executor). It also offers a generic state
handoff that is independent of lifecycle runs.

## Decision drivers

- D1 Each kind of caller should use only the API it needs.
- D2 One state machine; no second implementation of readiness rules.
- D3 Validation happens once, at construction.
- D4 Configuration should be simple to read and extend.

## Considered options

1. **Layered types: `Topology` → `Coordinator`; `Plan` (topology + actions) →
   `Engine`**
   - Pro: callers pick their layer; `Engine` is a thin driver over
     `Coordinator`, so the rules live in one place.
   - Pro: `Topology` and `Plan` are immutable once validated.
   - Con: more exported types than a single entry point.
2. **Single `Engine` with callbacks only**
   - Pro: smallest API.
   - Con: callers that run actions themselves, or only need validation,
     must fake callbacks.
3. **Functional options (`WithMaxParallel(...)`) instead of an `Options`
   struct**
   - Pro: extensible without zero-value ambiguity.
   - Con: more API surface for a single setting; a struct with a documented
     zero value (`MaxParallel: 0` means 1) is clearer here.

For handoff, the options were generic role interfaces (`Exporter[S]`,
`Transport`, `Adopter[D]`, `Protocol[S, D]`) versus `any`-typed interfaces;
generics keep source and destination types checked at compile time.

## Decision

We will expose layered types: `NewTopology`/`Topology` (callback-free graph),
`NewPlan`/`Plan` (graph plus `Enter`/`Exit`/`Rollback` actions),
`OpenCoordinator`/`Coordinator`/`Execution` (caller-driven durable state
machine) and `NewEngine`/`Engine` (automatic driver over a `Coordinator`).
Persistence is behind the `Store` and `Session` interfaces, with
`memory.Store` as the reference implementation. Configuration uses the plain
`Options` struct. The `handoff` package uses generic role interfaces composed
by `Protocol[S, D]`.

## Consequences

### Positive
- go-shape uses `Plan`/`Engine`; dtc uses only `NewTopology`; both share one
  set of validation rules.
- Adding a field to `Options` is source compatible.

### Negative
- Callers must choose between `Engine` and `Coordinator`; the README explains
  when to use each.

### Follow-ups
- None.

## Links

- PRD-0001 §6 (API sketch)
