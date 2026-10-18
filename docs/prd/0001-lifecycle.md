# PRD 0001: lifecycle

| Field | Value |
|---|---|
| Status | Accepted |
| Implementation | Complete |
| Owners | Justus Perillieux |
| Related ADRs | ADR-0001, ADR-0002, ADR-0003, ADR-0004, ADR-0005, ADR-0006, ADR-0007, ADR-0008, ADR-0009, ADR-0010 |
| Last updated | 2026-10-18 |

Recorded retrospectively for the first public release; describes the library
as implemented.

## 1. Problem and context

Services and tools often bring several dependent resources up and down in a
fixed order: storage before job queues, job queues before workers, workers
before an HTTP listener. Writing that ordering by hand is easy for a straight
line but error-prone for a graph with forks and joins. It becomes hard once
the process can crash halfway through, a step fails and everything already
started has to be undone, or two processes might try to drive the same run.

lifecycle gives Go programs one engine for this. A caller describes phases
with dependencies and three idempotent actions (enter, exit, rollback). The
library validates the graph, runs independent phases concurrently up to a
bound, persists every action boundary through a fenced store session, and
resumes an interrupted run without repeating completed actions. A separate
`handoff` package moves state between owners through integrity-checked
artifacts.

## 2. Users and use cases

- As a service author, I want to declare start-up phases with dependencies so
  that they start in dependency order and stop in reverse order.
- As a service author, I want a failed start-up to roll back only the phases
  that actually entered, so that no half-started resource is leaked.
- As an operator of a long-running workflow, I want a crashed run to resume
  from its last durable state, so that completed actions are not repeated.
- As a scheduler author, I want to validate a dependency graph and get stable
  topological levels and a digest without registering callbacks, so that I
  can reuse the graph rules in my own scheduler.
- As a library author driving my own actions, I want a coordinator that tells
  me when a phase may run and records the outcome, so that I keep control of
  execution while reusing the durable state machine.
- As a backend implementer, I want a precise Store/Session contract and a
  reference implementation, so that I can provide a durable, fenced store.
- As a system that hands work between owners, I want to export, transport and
  adopt state with integrity checks, so that a corrupted or mismatched
  artifact is never adopted.

## 3. Goals and non-goals

### Goals

- G1 Validate arbitrary DAGs of phases up front and reject invalid graphs.
- G2 Execute entry in dependency order, exit in reverse dependency order and
  roll back on entry failure, with bounded concurrency and no level barriers.
- G3 Persist every action boundary and recover interrupted runs exactly.
- G4 Guarantee one exclusive, fenced owner per run through the Store contract.
- G5 Report errors deterministically and preserve their identity while live.
- G6 Offer a callback-free topology and a caller-driven coordinator in
  addition to the automatic engine.
- G7 Provide a generic, integrity-checked handoff protocol.

### Non-goals

- NG1 A workflow DSL, timers, schedules or long-running orchestration: the
  library runs one lifecycle per call and stays a building block.
- NG2 A durable or distributed Store implementation: backends are
  application-specific; only the in-memory reference store ships.
- NG3 Retries beyond restart recovery of interrupted idempotent actions:
  retry policy belongs to the caller.
- NG4 Synthesised compensation or inferred rollback actions.
- NG5 Domain-specific lifecycle states or resource-ownership semantics.
- NG6 Artifact encryption, authentication, storage or transport
  implementations.
- NG7 Panic recovery for user callbacks.

## 4. Functional requirements

| ID | Requirement | Priority (MUST/SHOULD/MAY) |
|---|---|---|
| FR-LIFE-01 | `NewPlan` and `NewTopology` reject empty names, nil actions (plans only), duplicate phases, unknown, duplicate or self dependencies, and cycles with the matching sentinel error. | MUST |
| FR-LIFE-02 | Plans and topologies compute stable topological levels sorted lexically within each level, independent of input order, and own copies of all dependency slices; `Levels` returns fresh copies. | MUST |
| FR-LIFE-03 | `Digest` is a deterministic SHA-256 digest of canonical phase names and sorted dependencies, identical for a `Plan` and a `Topology` of the same graph; actions are excluded. | MUST |
| FR-LIFE-04 | `NewEngine` and `OpenCoordinator` reject nil or uninitialized plans and topologies, nil stores (including typed nils), negative parallelism and empty run IDs with `ErrInvalidPhase`; a nil session from the store is rejected, and a session for a different run is rejected and released. The canonical empty plan is valid. | MUST |
| FR-LIFE-05 | A phase enters only after all its dependencies have durably entered; a newly ready phase starts without waiting for unrelated phases in the same level. | MUST |
| FR-LIFE-06 | At most `MaxParallel` actions run at once (zero means 1). Among phases ready at a scheduling decision, the engine admits them in lexical order. | MUST |
| FR-LIFE-07 | After all entries succeed, a phase exits only after all its dependents have durably resolved exit; exit is unavailable until every entry has resolved, and exit scheduling has no level barriers. | MUST |
| FR-LIFE-08 | When an entry fails, the run durably moves to `rolling_back`, peers' contexts are cancelled with the failure as cause, waiting dependents fail with `ErrDependencyFailed`, no new entry is admitted, and only successfully entered phases roll back in reverse dependency order. | MUST |
| FR-LIFE-09 | A failed exit or rollback is persisted with a stage-specific status, counts as resolved for its dependencies, does not suppress other cleanup, and makes the run `failed` once all cleanup has resolved. | MUST |
| FR-LIFE-10 | The run state is saved before (in-progress status, incremented attempts) and after every action. If the pre-action save fails the action is not called; if a later save fails the durable state stays restartable and the last durably saved snapshot is returned with the error. | MUST |
| FR-LIFE-11 | On resume, completed actions are skipped and in-progress actions are retried; a run that is `rolling_back` retries durable `entering` phases before rollback begins; recorded failures are preserved and returned. | MUST |
| FR-LIFE-12 | Loaded snapshots are validated before any action runs: run ID, plan digest (`ErrPlanChanged` joined with `ErrInvalidSnapshot`), exact phase set, known statuses, attempt and error-text invariants, reachable run state, dependency ordering. A terminal run returns `ErrRunTerminal`. | MUST |
| FR-LIFE-13 | Each run has at most one owner: `Acquire` returns `ErrRunOwned` while another session is active, released or superseded sessions fail with `ErrOwnershipLost`, and the engine and coordinator release ownership with a context independent of cancellation. | MUST |
| FR-LIFE-14 | Errors from concurrent or recovered failures are joined in topological-level, then lexical, order; live action errors keep their identity for `errors.Is`; recovered errors keep their exact persisted text, including empty text. | MUST |
| FR-LIFE-15 | `Coordinator` lets callers drive actions: `EnterPhase`, `ExitPhase` and `RollbackPhase` block until ready, never admit the same phase twice concurrently, return a non-runnable `Execution` for completed work, reject stage operations invalid for the run state, and a cancelled admission does not affect later admissions. | MUST |
| FR-LIFE-16 | Phase and run `UpdatedAt` timestamps record the time of each transition from `Options.Clock`, or `time.Now` when it is nil. | SHOULD |
| FR-LIFE-17 | `memory.Store` implements the Store/Session contract in process: it returns `ErrSnapshotNotFound` for absent runs, clones snapshots at every boundary, fences stale sessions, and its zero value is ready for use. | MUST |
| FR-LIFE-18 | `handoff.NewArtifact` copies its inputs and derives `Digest` and `ID` from a canonical, length-prefixed envelope of media type, version, data and sorted metadata; `Validate` rejects missing fields (`ErrInvalidArtifact`) and tampering (`ErrIntegrity`). | MUST |
| FR-LIFE-19 | `handoff.Protocol.Transfer` runs Export, Send, Receive, Adopt in order, validates artifacts and receipt identity at each boundary, never adopts after an integrity failure, wraps errors with the failing stage, and `NewProtocol` rejects nil collaborators. | MUST |

## 5. Non-functional requirements

| ID | Requirement | Measure |
|---|---|---|
| NFR-LIFE-01 | All exported types document their concurrency safety; shared types are safe for concurrent use. | `go test -race -count=3 ./...` passes. |
| NFR-LIFE-02 | Tests are deterministic: time and goroutine coordination use `testing/synctest`, with no real sleeps or wall-clock deadlines. | No `time.Sleep`/`time.After` waits outside synctest bubbles; repeated runs pass. |
| NFR-LIFE-03 | Minimal dependencies: the standard library plus `github.com/justusschock/concurrency`. | `go list -m all` lists only these modules. |
| NFR-LIFE-04 | Supported toolchains: Go 1.25 and the two latest upstream releases. | CI matrix `1.25.x`, `oldstable`, `stable`. |
| NFR-LIFE-05 | The library never logs, writes to stdout/stderr or keeps global mutable state. | Code review; no `log`/`fmt.Print` in non-test code. |
| NFR-LIFE-06 | Caller-owned data is never shared: maps and slices are copied at every API and store boundary. | `TestPlanOwnsItsGraphData`, `TestStoreReturnsNotFoundAndCopiesSnapshots`, `TestNewArtifactCopiesInputsAndComputesStableIdentity`. |
| NFR-LIFE-07 | Static analysis is clean. | `go vet ./...` and `golangci-lint run ./...` report nothing. |

## 6. API sketch

```go
package lifecycle

type Action func(context.Context) error
type Phase struct { Name string; DependsOn []string; Enter, Exit, Rollback Action }
type PhaseSpec struct { Name string; DependsOn []string }

func NewPlan(phases []Phase) (*Plan, error)
func NewTopology(phases []PhaseSpec) (*Topology, error)
func (*Plan) Digest() string
func (*Plan) Levels() [][]Phase

type Options struct {
	MaxParallel int
	Clock       func() time.Time // nil means time.Now
}
func NewEngine(plan *Plan, store Store, options Options) (*Engine, error)
func (*Engine) Run(ctx context.Context, runID string) (Snapshot, error)

func OpenCoordinator(ctx context.Context, topology *Topology, store Store, runID string, options Options) (*Coordinator, error)
func (*Coordinator) EnterPhase(ctx context.Context, name string) (*Execution, error)
func (*Coordinator) ExitPhase(ctx context.Context, name string) (*Execution, error)
func (*Coordinator) RollbackPhase(ctx context.Context, name string) (*Execution, error)
func (*Execution) ShouldRun() bool
func (*Execution) Context() context.Context
func (*Execution) Complete(ctx context.Context, actionErr error) error

type Store interface { Acquire(context.Context, string) (Session, error) }
type Session interface {
	RunID() string
	Load(context.Context) (Snapshot, error)
	Save(context.Context, Snapshot) error
	Release(context.Context) error
}

package handoff

func NewArtifact(mediaType string, version uint64, data []byte, metadata map[string]string) (Artifact, error)
func NewProtocol[S, D any](Exporter[S], Transport, Adopter[D]) (*Protocol[S, D], error)
func (*Protocol[S, D]) Transfer(ctx context.Context, source S) (D, Receipt, error)

package memory

func NewStore() *Store
```

## 7. Acceptance criteria

| Criterion | Requirement(s) | Test(s) |
|---|---|---|
| Invalid graphs are rejected with the right sentinel | FR-LIFE-01 | `TestPlanRejectsInvalidGraphs`, `TestTopologyRejectsInvalidGraphs` |
| Levels are stable and graph data is owned | FR-LIFE-02 | `TestPlanBuildsStableBranchAndJoinLevels`, `TestTopologyBuildsStableImmutableDAG`, `TestPlanOwnsItsGraphData` |
| Plan and topology digests agree | FR-LIFE-03 | `TestTopologyAndPlanShareDigest` |
| Construction rejects invalid inputs and sessions | FR-LIFE-04 | `TestEngineRejectsInvalidConstructionAndRunID`, `TestNewEngineRejectsUninitializedPlanAndAcceptsCanonicalEmptyPlan`, `TestEngineRejectsTypedNilSession`, `TestEngineRejectsMismatchedSessionAndStillReleasesIt` |
| Entry follows dependencies without level barriers | FR-LIFE-05 | `TestEngineRunsArbitraryDAGInDependencyOrder`, `TestEngineSchedulesNewlyReadyPhasesWithoutLevelBarrier`, `TestCoordinatorBlocksUntilDependenciesDurablyComplete`, `TestCoordinatorAdmitsSiblingsAndBlocksJoin` |
| Parallelism is bounded and admission lexical | FR-LIFE-06 | `TestCoordinatorEnforcesMaxParallel`, `TestEngineAdmitsReadyPhasesLexically` |
| Exit runs in reverse dependency order | FR-LIFE-07 | `TestEngineRunsBranchesTogetherAndJoinsBeforeReverseExit`, `TestCoordinatorExitsInReverseDependencyOrder`, `TestCoordinatorRejectsExitUntilAllEntriesResolve`, `TestEngineSchedulesReverseCleanupWithoutLevelBarriers` |
| Entry failure cancels peers and rolls back entered phases only | FR-LIFE-08 | `TestEngineCancelsFailingLevelAndRollsBackOnlyEnteredPhases`, `TestCoordinatorEntryFailureCancelsPeersAndWakesDependents`, `TestCoordinatorRollsBackEnteredPhasesInReverseDependencyOrder`, `TestAdversarialEntryReadinessAfterDurableRollback`, `TestAdversarialAtomicEntryAdmissionAfterDurableRollback` |
| Cleanup is best-effort with stage-specific failures | FR-LIFE-09 | `TestEnginePersistsStageSpecificFailuresAndCleansBestEffort`, `TestEngineAttemptsEveryEnteredRollbackAfterRollbackFailure`, `TestCoordinatorCleanupFailureDoesNotSuppressAncestors` |
| Every action boundary is persisted; save failures stay restartable | FR-LIFE-10 | `TestEnginePersistsEnteringBeforeCallingAction`, `TestEngineReturnsPreActionStoreFailureWithoutRunningAction`, `TestEngineLeavesInterruptedActionsRestartableAfterTerminalSaveFailure`, `TestEngineDoesNotExposeFailedRunStatusSaveAsCurrent`, `TestEngineLeavesExitFailureNonterminalUntilFinalStatusIsDurable`, `TestCoordinatorPersistenceFailureStopsAdmission`, `TestCoordinatorCompletionRetryRestoresAdmission`, `TestCoordinatorRetriesTerminalizationAfterSaveFailure` |
| Interrupted runs resume exactly | FR-LIFE-11 | `TestEngineDoesNotRepeatCompletedEntryAfterRestart`, `TestEngineRetriesInterruptedExitAndRollback`, `TestCoordinatorSkipsCompletedEntryAfterRestart`, `TestCoordinatorRetriesInterruptedEntryAfterRestart`, `TestCoordinatorRetriesInterruptedExitAndRollback`, `TestCoordinatorRollbackRetriesEnteringBeforeCleanup`, `TestRecoveredRollbackRetriesEnteringThenReturnsOriginalFailure`, `TestRecoveredRollbackPreservesFailureAndFinishesOutstandingAncestors`, `TestRecoveredExitPreservesFailureAndFinishesOutstandingCleanup`, `TestEngineAcceptsEveryPersistedCrashBoundary` |
| Loaded snapshots are validated | FR-LIFE-12 | `TestEngineValidatesLoadedSnapshots`, `TestEngineRejectsChangedPlanAndTerminalRunBeforeActions`, `TestSnapshotRejectsImpossiblePhaseStateCountersAndErrors`, `TestSnapshotRejectsUnreachableRunStates`, `TestSnapshotRejectsImpossibleDependencyAndCleanupOrdering`, `TestSnapshotRejectsRollbackCleanupWhileEntryRemainsInProgress`, `TestAdversarialSnapshotDependencyConsistency`, `TestAdversarialSnapshotPhaseStateConsistency`, `TestSnapshotAcceptsNaturalPhaseStateCrashBoundaries`, `TestSnapshotAcceptsReachableRollbackProvenance`, `TestSnapshotAcceptsCanonicalEmptyPendingState`, `TestSnapshotAcceptsDependencyOrderedCrashBoundaries` |
| Runs are owned exclusively and released on cancellation | FR-LIFE-13 | `TestEngineOwnsEachRunExclusively`, `TestEngineCancellationDoesNotStrandRunOwnership`, `TestStoreSessionsFenceRunOwnership`, `TestSessionReleaseIgnoresCanceledAcquisitionContext`, `TestCoordinatorCloseReleasesOwnershipAfterCancellation`, `TestCoordinatorCloseRetriesFailedRelease` |
| Errors are ordered deterministically | FR-LIFE-14 | `TestEngineOrdersConcurrentEntryErrorsByPlanOrder`, `TestEngineOrdersAllRecoveredEntryFailuresByPlanOrder`, `TestEngineAcceptsFailureWhoseErrorTextIsEmpty`, `TestEngineRecoversEmptyPersistedErrorTextExactly` |
| Coordinator admission rules | FR-LIFE-15 | `TestCoordinatorPreventsDuplicateConcurrentExecution`, `TestCoordinatorCanceledAdmissionDoesNotStopLaterAdmission`, `TestCoordinatorRejectsStageOperationsInInvalidRunState` |
| Transitions are timestamped | FR-LIFE-16 | `TestEngineStampsTransitionsWithCurrentTime`, `TestEngineStampsTransitionsWithSuppliedClock` |
| In-memory store honours the contract | FR-LIFE-17 | `TestStoreReturnsNotFoundAndCopiesSnapshots`, `TestStoreZeroValueIsReadyForUse`, `TestStoreSessionsFenceRunOwnership` |
| Artifact identity is canonical and tamper-evident | FR-LIFE-18 | `TestNewArtifactCopiesInputsAndComputesStableIdentity`, `TestArtifactIdentityCommitsToCanonicalSemanticEnvelope`, `TestArtifactRejectsInvalidFieldsAndTampering` |
| Handoff protocol order and integrity | FR-LIFE-19 | `TestProtocolTransfersInOrderAcrossCopyBoundaries`, `TestProtocolRejectsReceiptAndReceivedArtifactMismatchBeforeAdoption`, `TestProtocolPreservesStageErrors`, `TestNewProtocolRejectsNilCollaborators` |

## 8. Source references

None.

## 9. Open questions

- Q1 Should the Store/Session contract ship as a public conformance suite
  (for example `storetest`) so that durable backends can verify fencing and
  isolation? Today the contract is described in documentation and exercised
  only against `memory.Store`.
- Q2 Should `ErrInvalidArtifact` and `ErrIntegrity` move from the root
  package to `handoff`? They are used by consumers from the root package, so
  a move would be a breaking change and needs an ADR.
