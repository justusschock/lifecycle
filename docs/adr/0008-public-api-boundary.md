# ADR 0008: Public API boundary

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0002, ADR-0010 |

## Context

Recorded retrospectively for the first public release. Before publishing,
every exported package and identifier was reviewed against what consumers
need. Known consumers are go-executor, go-object-storage and
go-reconciliation (the `handoff` package and the root error sentinels),
go-shape (`NewPlan`, `Phase`, `NewEngine`, `Options`, `RunSucceeded`,
`memory.NewStore`) and dtc (`NewTopology`, `PhaseSpec`).

## Decision drivers

- D1 The public surface is exactly what users need; everything else is
  unexported or under `internal/`.
- D2 Nothing a consumer uses is removed.
- D3 No deprecated names in the first public release.
- D4 No API that exists only so tests can control time.

## Considered options

1. **Keep every exported identifier as is**
   - Pro: no churn.
   - Con: ships a deprecated alias and a test-only clock hook.
2. **Trim to the consumer-used subset only**
   - Pro: smallest surface.
   - Con: removes `Coordinator`, `Execution`, `Plan.Levels` and other
     documented capabilities that are part of the library's purpose.
3. **Keep the documented capabilities; remove deprecated and test-only
   API; move shared helpers to `internal/`**
   - Pro: clean surface without losing capability.
   - Con: two breaking changes, acceptable while the major version is 0.

## Decision

We choose option 3.

Public:

- `github.com/justusschock/go-lifecycle`: `Action`, `Phase`, `Plan`,
  `NewPlan`, `PhaseSpec`, `Topology`, `NewTopology`, `Engine`, `NewEngine`,
  `Coordinator`, `OpenCoordinator`, `Execution`, `Options`, `Store`,
  `Session`, `Snapshot`, `PhaseState`, `PhaseStatus` and `RunStatus` with
  their constants, and all `Err*` sentinels.
- `github.com/justusschock/go-lifecycle/memory`: `Store`, `NewStore`, and
  the `Store.Load`/`Store.Save` conveniences for inspecting and seeding runs.
- `github.com/justusschock/go-lifecycle/handoff`: `Artifact`,
  `NewArtifact`, `Receipt`, `Exporter`, `Transport`, `Adopter`, `Protocol`,
  `NewProtocol`.

Changed for the release:

- Removed `PhaseFailed`, a deprecated alias of `PhaseEnterFailed`
  (no consumer uses it).
- Removed `Options.Clock`. It existed only to fix snapshot timestamps in
  tests and no consumer set it; timestamps now come from `time.Now`, and
  tests control time with `testing/synctest` (ADR-0010).
- Moved the duplicated typed-nil detection (`isNilStore`, `isNilSession`,
  `isNilCollaborator`) into `internal/nilcheck`.
- Removed the unused unexported `validateDependencyOrdering` wrapper.

`internal/` is not part of the API and may change in any release.

## Consequences

### Positive
- Every public identifier is documented and tested.
- Consumers compile unchanged.

### Negative
- Code that used `PhaseFailed` or `Options.Clock` must change; both are
  listed in the changelog.

### Follow-ups
- A future public Store conformance suite (PRD-0001 Q1) would be public,
  not internal.

## Links

- [CHANGELOG.md](../../CHANGELOG.md)
