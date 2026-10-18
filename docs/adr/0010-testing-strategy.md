# ADR 0010: Testing strategy

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 (NFR-LIFE-01, NFR-LIFE-02), ADR-0009 |

## Context

Recorded retrospectively for the first public release. The library's risks
are ordering bugs between concurrent goroutines, recovery from every
possible crash boundary, and invalid persisted state. Some earlier tests
asserted that an operation was still blocked with a non-blocking `select`,
which passed trivially when the goroutine had not been scheduled yet, and
some waited up to one real second for a phase to start.

## Decision drivers

- D1 Tests are deterministic and fast.
- D2 Negative assertions ("has not happened yet") must be meaningful.
- D3 Every requirement is traceable to tests.
- D4 Data races are caught.

## Considered options

1. **`testing/synctest` for time and goroutine coordination, race detector
   always on, adversarial state tables, requirement IDs in tests**
   - Pro: `synctest.Wait` lets every goroutine in the bubble block durably
     before a negative check; virtual time removes real waits.
   - Con: requires Go 1.25 (ADR-0009); code under test must not block on
     real I/O inside a bubble.
2. **Real time with generous timeouts**
   - Pro: no new tooling.
   - Con: slow and flaky; negative checks are probabilistic.
3. **Injected fake clocks**
   - Pro: deterministic time.
   - Con: widens the public API (`Options.Clock`) only for tests and does
     not help with goroutine scheduling.

## Decision

We will:

- Run tests that coordinate goroutines or depend on time inside
  `synctest.Test` and call `synctest.Wait` before every "still blocked" or
  "not yet started" assertion. Snapshot timestamps are checked against the
  bubble's virtual clock.
- Keep pure tests (graph validation, snapshot validation tables, artifact
  hashing, store copies) outside synctest; they involve no time or
  concurrency.
- Exercise every persisted crash boundary and every invalid snapshot shape
  with table-driven and adversarial tests, including package-internal tests
  against unexported validation.
- Use failing and recording test stores to inject persistence failures at
  each save.
- Keep runnable examples (`example_test.go`) in sync with the README.
- Run `go test -race`; releases are verified with `-count=3`.
- Cite requirement IDs (`// FR-LIFE-NN`) on the tests listed in the PRD
  acceptance table.

## Consequences

### Positive
- Coordination tests catch real ordering bugs deterministically and run in
  milliseconds.

### Negative
- Examples cannot use synctest and rely on channel hand-offs for ordering.

### Follow-ups
- Add fuzz tests for `NewTopology` and snapshot validation if bugs appear
  in those areas.

## Links

- [testing/synctest](https://pkg.go.dev/testing/synctest)
