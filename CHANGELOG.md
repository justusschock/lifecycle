# Changelog

All notable changes to this project are documented in this file. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the major version is 0, minor releases may contain breaking changes;
they are listed here.

## [Unreleased]

Preparation for the first public release.

### Changed

- **Breaking:** the minimum Go version is now 1.25 (was 1.22), because the
  tests use `testing/synctest` (ADR-0009).
- **Breaking:** removed `Options.Clock`. Snapshot timestamps now always come
  from `time.Now`; tests control time with `testing/synctest` (ADR-0008).
- **Breaking:** removed the deprecated `PhaseFailed` alias; use
  `PhaseEnterFailed` (ADR-0008).
- Typed-nil detection shared by the root and `handoff` packages moved to
  `internal/nilcheck`; `internal/` is not part of the API.
- Concurrency tests run inside `testing/synctest` bubbles, so "still
  blocked" assertions are deterministic and no test waits on real time.
- CI runs on Go 1.25, `oldstable` and `stable` with the race detector and
  golangci-lint, with every action pinned to a commit SHA; Dependabot keeps
  modules and actions current.

### Added

- Package documentation and doc comments for every exported identifier.
- A runnable package example used as the README quick start.
- `NOTICE`, `AGENTS.md`, a retrospective PRD and ADRs under `docs/`.

## [0.2.0] - 2026-07-17

### Added

- `Topology`, `NewTopology` and `PhaseSpec`: a callback-free, validated
  dependency graph shared with `Plan`.
- `Coordinator`, `OpenCoordinator` and `Execution`: a caller-driven,
  durable lifecycle state machine; `Engine` now drives runs through it.
- Coordinator-driven exit, rollback and recovery.

### Fixed

- Exit is admitted only after every entry has resolved.
- Coordinator recovery after transient persistence failures and retried
  terminalization.

## [0.1.0] - 2026-07-17

### Added

- Initial release: `Plan`, `Engine`, fenced `Store`/`Session` contract,
  `memory.Store` and the `handoff` package.

[Unreleased]: https://github.com/justusschock/go-lifecycle/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/justusschock/go-lifecycle/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/justusschock/go-lifecycle/releases/tag/v0.1.0
