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
- D3 Existing callers keep compiling: identifiers that were already
  exported, including deprecated ones, stay unless they leak an
  implementation detail.
- D4 Implementation details shared between packages are not exported.

## Considered options

1. **Keep every exported identifier; move shared helpers to `internal/`**
   - Pro: no breaking change for existing callers; `Options.Clock` remains
     available to callers that need their own time source.
   - Con: ships the deprecated `PhaseFailed` alias and a clock hook that
     the library's own tests no longer need.
2. **Trim to the consumer-used subset only**
   - Pro: smallest surface.
   - Con: removes `Coordinator`, `Execution`, `Plan.Levels` and other
     documented capabilities that are part of the library's purpose.
3. **Keep the documented capabilities but remove the deprecated alias and
   `Options.Clock`; move shared helpers to `internal/`**
   - Pro: slightly smaller surface; tests control time with
     `testing/synctest` instead.
   - Con: two breaking changes for no functional gain, and callers lose a
     supported way to supply timestamps.

## Decision

We choose option 1.

Public:

- `github.com/justusschock/go-lifecycle`: `Action`, `Phase`, `Plan`,
  `NewPlan`, `PhaseSpec`, `Topology`, `NewTopology`, `Engine`, `NewEngine`,
  `Coordinator`, `OpenCoordinator`, `Execution`, `Options` (including
  `Options.Clock`, which defaults to `time.Now` when nil), `Store`,
  `Session`, `Snapshot`, `PhaseState`, `PhaseStatus` and `RunStatus` with
  their constants (including the deprecated `PhaseFailed` alias of
  `PhaseEnterFailed`), and all `Err*` sentinels.
- `github.com/justusschock/go-lifecycle/memory`: `Store`, `NewStore`, and
  the `Store.Load`/`Store.Save` conveniences for inspecting and seeding runs.
- `github.com/justusschock/go-lifecycle/handoff`: `Artifact`,
  `NewArtifact`, `Receipt`, `Exporter`, `Transport`, `Adopter`, `Protocol`,
  `NewProtocol`.

Changed for the release (no exported identifier is removed):

- Moved the duplicated typed-nil detection (`isNilStore`, `isNilSession`,
  `isNilCollaborator`) into `internal/nilcheck`.
- Removed the unused unexported `validateDependencyOrdering` wrapper.

`internal/` is not part of the API and may change in any release.

## Consequences

### Positive
- Every public identifier is documented and tested.
- Consumers and other existing callers compile unchanged.

### Negative
- The deprecated `PhaseFailed` alias stays in the API; it can only be
  removed in a later, changelog-listed breaking release.
- `Options.Clock` stays public although the library's own tests use
  `testing/synctest` rather than an injected clock (ADR-0010).

### Follow-ups
- A future public Store conformance suite (PRD-0001 Q1) would be public,
  not internal.

## Links

- [CHANGELOG.md](../../CHANGELOG.md)
