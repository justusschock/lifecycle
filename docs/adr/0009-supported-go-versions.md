# ADR 0009: Supported Go versions

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 (NFR-LIFE-04), ADR-0010 |

## Context

Recorded retrospectively for the first public release. The module declared
`go 1.22` and CI tested 1.22 through 1.26. The test suite now uses
`testing/synctest`, whose stable API (`synctest.Test`, `synctest.Wait`)
first shipped in Go 1.25. Upstream Go supports the two most recent major
releases.

## Decision drivers

- D1 Tests must build on every supported version.
- D2 Follow the upstream support policy.
- D3 Do not raise the floor without a reason.

## Considered options

1. **Floor 1.25; test 1.25, `oldstable` and `stable`**
   - Pro: synctest is available everywhere; the matrix tracks upstream
     automatically.
   - Con: drops 1.22–1.24, which upstream no longer supports.
2. **Keep 1.22 and gate synctest tests behind build tags**
   - Pro: wider compatibility.
   - Con: two test variants; older versions get weaker tests.
3. **Track only `stable`**
   - Pro: simplest.
   - Con: breaks users one release behind.

## Decision

We will declare `go 1.25` in `go.mod` and test Go `1.25.x`, `oldstable` and
`stable` in CI. The floor is raised only when a newer language or library
feature is needed, through a new ADR.

## Consequences

### Positive
- One test suite for all supported versions.

### Negative
- Users on Go 1.24 or older must upgrade.

### Follow-ups
- Revisit the floor when 1.25 leaves upstream support.

## Links

- [Go release policy](https://go.dev/doc/devel/release#policy)
- [testing/synctest](https://pkg.go.dev/testing/synctest)
