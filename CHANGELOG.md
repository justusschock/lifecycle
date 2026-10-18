# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the major version is `0`, a minor release may break the API.

## [Unreleased]

## [0.1.0] - 2026-10-18

First public release.

### Added

- Validated phase graphs: `NewPlan` and the callback-free `NewTopology`
  reject duplicates, unknown or self dependencies and cycles, and expose
  stable topological `Levels` and a deterministic `Digest`.
- `Engine`: automatic entry in dependency order, exit in reverse order,
  rollback of entered phases after an entry failure, and bounded
  parallelism through `Options.MaxParallel`.
- `Coordinator` (`OpenCoordinator`): a caller-driven lifecycle with
  `EnterPhase`, `ExitPhase`, `RollbackPhase` and an `Execution` that
  durably records each outcome.
- Durable, resumable runs: a `Snapshot` is saved before and after every
  action through the fenced, exclusive `Store`/`Session` contract, and an
  interrupted run resumes without repeating completed work.
- Strict validation of loaded snapshots, including plan-change detection
  (`ErrPlanChanged`).
- Sentinel errors for `errors.Is`, such as `ErrCycle`, `ErrRunOwned`,
  `ErrOwnershipLost` and `ErrIntegrity`.
- Package `memory`: `memory.Store`, an in-memory reference store.
- Package `handoff`: `Protocol.Transfer` runs export, send, receive and
  adopt, with SHA-256 artifact identity (`NewArtifact`, `Validate`) checked
  at every boundary.
- Requirements and architecture decision records under `docs/`.

Requires Go 1.25 or newer.

[Unreleased]: https://github.com/justusschock/lifecycle/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/justusschock/lifecycle/releases/tag/v0.1.0
