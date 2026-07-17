package lifecycle_test

import (
	"context"
	"errors"
	"testing"

	lifecycle "github.com/justusschock/go-lifecycle"
	"github.com/justusschock/go-lifecycle/memory"
)

type coordinatorResult struct {
	execution *lifecycle.Execution
	err       error
}

func TestCoordinatorBlocksUntilDependenciesDurablyComplete(t *testing.T) {
	topology := coordinatorTopology(t,
		lifecycle.PhaseSpec{Name: "root"},
		lifecycle.PhaseSpec{Name: "child", DependsOn: []string{"root"}},
	)
	coordinator := openCoordinator(t, topology, memory.NewStore(), "dependency-run", lifecycle.Options{MaxParallel: 2})
	defer closeCoordinator(t, coordinator)

	child := enterAsync(coordinator, "child")
	assertStillWaiting(t, child)

	root, err := coordinator.EnterPhase(context.Background(), "root")
	if err != nil {
		t.Fatal(err)
	}
	if err := root.Complete(context.Background(), nil); err != nil {
		t.Fatal(err)
	}

	childResult := <-child
	if childResult.err != nil {
		t.Fatal(childResult.err)
	}
	if !childResult.execution.ShouldRun() {
		t.Fatal("child did not require execution")
	}
	if err := childResult.execution.Complete(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
}

func TestCoordinatorAdmitsSiblingsAndBlocksJoin(t *testing.T) {
	topology := coordinatorTopology(t,
		lifecycle.PhaseSpec{Name: "root"},
		lifecycle.PhaseSpec{Name: "left", DependsOn: []string{"root"}},
		lifecycle.PhaseSpec{Name: "right", DependsOn: []string{"root"}},
		lifecycle.PhaseSpec{Name: "join", DependsOn: []string{"left", "right"}},
	)
	coordinator := openCoordinator(t, topology, memory.NewStore(), "join-run", lifecycle.Options{MaxParallel: 2})
	defer closeCoordinator(t, coordinator)

	root := enterPhase(t, coordinator, "root")
	completePhase(t, root, nil)

	leftResult := enterAsync(coordinator, "left")
	rightResult := enterAsync(coordinator, "right")
	left := requireExecution(t, <-leftResult)
	right := requireExecution(t, <-rightResult)

	joinResult := enterAsync(coordinator, "join")
	assertStillWaiting(t, joinResult)
	completePhase(t, left, nil)
	assertStillWaiting(t, joinResult)
	completePhase(t, right, nil)
	completePhase(t, requireExecution(t, <-joinResult), nil)
}

func TestCoordinatorEnforcesMaxParallel(t *testing.T) {
	topology := coordinatorTopology(t,
		lifecycle.PhaseSpec{Name: "a"},
		lifecycle.PhaseSpec{Name: "b"},
		lifecycle.PhaseSpec{Name: "c"},
	)
	coordinator := openCoordinator(t, topology, memory.NewStore(), "parallel-run", lifecycle.Options{MaxParallel: 2})
	defer closeCoordinator(t, coordinator)

	results := make(chan coordinatorResult, 3)
	for _, name := range []string{"a", "b", "c"} {
		name := name
		go func() {
			execution, err := coordinator.EnterPhase(context.Background(), name)
			results <- coordinatorResult{execution: execution, err: err}
		}()
	}

	first := requireExecution(t, <-results)
	second := requireExecution(t, <-results)
	assertStillWaiting(t, results)
	completePhase(t, first, nil)
	third := requireExecution(t, <-results)
	completePhase(t, second, nil)
	completePhase(t, third, nil)
}

func TestCoordinatorPreventsDuplicateConcurrentExecution(t *testing.T) {
	topology := coordinatorTopology(t, lifecycle.PhaseSpec{Name: "phase"})
	coordinator := openCoordinator(t, topology, memory.NewStore(), "duplicate-run", lifecycle.Options{MaxParallel: 2})
	defer closeCoordinator(t, coordinator)

	first := enterPhase(t, coordinator, "phase")
	duplicate := enterAsync(coordinator, "phase")
	assertStillWaiting(t, duplicate)
	completePhase(t, first, nil)

	result := <-duplicate
	if result.err != nil {
		t.Fatal(result.err)
	}
	if result.execution.ShouldRun() {
		t.Fatal("duplicate caller was admitted after durable completion")
	}
}

func TestCoordinatorSkipsCompletedEntryAfterRestart(t *testing.T) {
	store := memory.NewStore()
	topology := coordinatorTopology(t, lifecycle.PhaseSpec{Name: "phase"})
	coordinator := openCoordinator(t, topology, store, "restart-skip", lifecycle.Options{})
	completePhase(t, enterPhase(t, coordinator, "phase"), nil)
	closeCoordinator(t, coordinator)

	restarted := openCoordinator(t, topology, store, "restart-skip", lifecycle.Options{})
	defer closeCoordinator(t, restarted)
	execution, err := restarted.EnterPhase(context.Background(), "phase")
	if err != nil {
		t.Fatal(err)
	}
	if execution.ShouldRun() {
		t.Fatal("completed entry was admitted after restart")
	}
}

func TestCoordinatorRetriesInterruptedEntryAfterRestart(t *testing.T) {
	store := memory.NewStore()
	topology := coordinatorTopology(t, lifecycle.PhaseSpec{Name: "phase"})
	coordinator := openCoordinator(t, topology, store, "restart-retry", lifecycle.Options{})
	if execution := enterPhase(t, coordinator, "phase"); !execution.ShouldRun() {
		t.Fatal("initial entry did not require execution")
	}
	closeCoordinator(t, coordinator)

	restarted := openCoordinator(t, topology, store, "restart-retry", lifecycle.Options{})
	defer closeCoordinator(t, restarted)
	execution := enterPhase(t, restarted, "phase")
	if !execution.ShouldRun() {
		t.Fatal("interrupted entry was not retried")
	}
	if attempts := restarted.Snapshot().Phases["phase"].Attempts; attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
	completePhase(t, execution, nil)
}

func TestCoordinatorEntryFailureCancelsPeersAndWakesDependents(t *testing.T) {
	failure := errors.New("entry failed")
	topology := coordinatorTopology(t,
		lifecycle.PhaseSpec{Name: "left"},
		lifecycle.PhaseSpec{Name: "right"},
		lifecycle.PhaseSpec{Name: "child", DependsOn: []string{"left"}},
	)
	coordinator := openCoordinator(t, topology, memory.NewStore(), "failure-run", lifecycle.Options{MaxParallel: 2})
	defer closeCoordinator(t, coordinator)

	left := enterPhase(t, coordinator, "left")
	right := enterPhase(t, coordinator, "right")
	child := enterAsync(coordinator, "child")
	assertStillWaiting(t, child)

	if err := left.Complete(context.Background(), failure); !errors.Is(err, failure) {
		t.Fatalf("Complete error = %v, want %v", err, failure)
	}
	result := <-child
	if !errors.Is(result.err, lifecycle.ErrDependencyFailed) || !errors.Is(result.err, failure) {
		t.Fatalf("child error = %v, want dependency and action failure", result.err)
	}
	select {
	case <-right.Context().Done():
		if !errors.Is(context.Cause(right.Context()), failure) {
			t.Fatalf("peer cancellation cause = %v, want %v", context.Cause(right.Context()), failure)
		}
	default:
		t.Fatal("peer execution context was not canceled")
	}
	if err := right.Complete(context.Background(), context.Cause(right.Context())); err == nil {
		t.Fatal("canceled peer completion returned nil")
	}
}

func TestCoordinatorPersistenceFailureStopsAdmission(t *testing.T) {
	failure := errors.New("save failed")
	store := newRecordingStore()
	store.failAt = 1
	store.failure = failure
	topology := coordinatorTopology(t, lifecycle.PhaseSpec{Name: "a"}, lifecycle.PhaseSpec{Name: "b"})
	coordinator := openCoordinator(t, topology, store, "save-run", lifecycle.Options{MaxParallel: 2})
	defer closeCoordinator(t, coordinator)

	if _, err := coordinator.EnterPhase(context.Background(), "a"); !errors.Is(err, failure) {
		t.Fatalf("first admission error = %v, want %v", err, failure)
	}
	if _, err := coordinator.EnterPhase(context.Background(), "b"); !errors.Is(err, failure) {
		t.Fatalf("second admission error = %v, want %v", err, failure)
	}
	if got := store.latest().Phases; got != nil {
		t.Fatalf("store persisted failed candidate: %v", got)
	}
}

func TestCoordinatorCanceledAdmissionDoesNotStopLaterAdmission(t *testing.T) {
	topology := coordinatorTopology(t, lifecycle.PhaseSpec{Name: "phase"})
	coordinator := openCoordinator(t, topology, memory.NewStore(), "canceled-admission", lifecycle.Options{})
	defer closeCoordinator(t, coordinator)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := coordinator.EnterPhase(ctx, "phase"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled admission error = %v, want %v", err, context.Canceled)
	}
	completePhase(t, enterPhase(t, coordinator, "phase"), nil)
}

func TestCoordinatorCompletionRetryRestoresAdmission(t *testing.T) {
	failure := errors.New("save failed")
	store := newRecordingStore()
	store.failAt = 2
	store.failure = failure
	topology := coordinatorTopology(t,
		lifecycle.PhaseSpec{Name: "root"},
		lifecycle.PhaseSpec{Name: "child", DependsOn: []string{"root"}},
	)
	coordinator := openCoordinator(t, topology, store, "completion-retry", lifecycle.Options{})
	defer closeCoordinator(t, coordinator)

	root := enterPhase(t, coordinator, "root")
	if err := root.Complete(context.Background(), nil); !errors.Is(err, failure) {
		t.Fatalf("first completion error = %v, want %v", err, failure)
	}
	if err := root.Complete(context.Background(), nil); err != nil {
		t.Fatalf("completion retry error = %v", err)
	}
	completePhase(t, enterPhase(t, coordinator, "child"), nil)
}

func TestCoordinatorCloseReleasesOwnershipAfterCancellation(t *testing.T) {
	store := memory.NewStore()
	topology := coordinatorTopology(t, lifecycle.PhaseSpec{Name: "phase"})
	ctx, cancel := context.WithCancel(context.Background())
	coordinator := openCoordinator(t, topology, store, "close-run", lifecycle.Options{})
	cancel()
	if err := coordinator.Close(context.WithoutCancel(ctx)); err != nil {
		t.Fatal(err)
	}

	reopened := openCoordinator(t, topology, store, "close-run", lifecycle.Options{})
	closeCoordinator(t, reopened)
}

func TestCoordinatorCloseRetriesFailedRelease(t *testing.T) {
	store := memory.NewStore()
	topology := coordinatorTopology(t, lifecycle.PhaseSpec{Name: "phase"})
	coordinator := openCoordinator(t, topology, store, "close-retry", lifecycle.Options{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := coordinator.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("first close error = %v, want %v", err, context.Canceled)
	}
	if err := coordinator.Close(context.Background()); err != nil {
		t.Fatalf("close retry error = %v", err)
	}

	reopened := openCoordinator(t, topology, store, "close-retry", lifecycle.Options{})
	closeCoordinator(t, reopened)
}

func TestCoordinatorExitsInReverseDependencyOrder(t *testing.T) {
	topology := coordinatorTopology(t,
		lifecycle.PhaseSpec{Name: "root"},
		lifecycle.PhaseSpec{Name: "child", DependsOn: []string{"root"}},
	)
	coordinator := openCoordinator(t, topology, memory.NewStore(), "exit-order", lifecycle.Options{})
	defer closeCoordinator(t, coordinator)
	completePhase(t, enterPhase(t, coordinator, "root"), nil)
	completePhase(t, enterPhase(t, coordinator, "child"), nil)

	rootExit := exitAsync(coordinator, "root")
	assertStillWaiting(t, rootExit)
	completePhase(t, exitPhase(t, coordinator, "child"), nil)
	completePhase(t, requireExecution(t, <-rootExit), nil)
	if status := coordinator.Snapshot().Status; status != lifecycle.RunSucceeded {
		t.Fatalf("run status = %q, want %q", status, lifecycle.RunSucceeded)
	}
}

func TestCoordinatorRollsBackEnteredPhasesInReverseDependencyOrder(t *testing.T) {
	failure := errors.New("entry failed")
	topology := coordinatorTopology(t,
		lifecycle.PhaseSpec{Name: "root"},
		lifecycle.PhaseSpec{Name: "child", DependsOn: []string{"root"}},
		lifecycle.PhaseSpec{Name: "failing"},
	)
	coordinator := openCoordinator(t, topology, memory.NewStore(), "rollback-order", lifecycle.Options{})
	defer closeCoordinator(t, coordinator)
	completePhase(t, enterPhase(t, coordinator, "root"), nil)
	completePhase(t, enterPhase(t, coordinator, "child"), nil)
	if err := enterPhase(t, coordinator, "failing").Complete(context.Background(), failure); !errors.Is(err, failure) {
		t.Fatalf("failure completion = %v, want %v", err, failure)
	}

	rootRollback := rollbackAsync(coordinator, "root")
	assertStillWaiting(t, rootRollback)
	completePhase(t, rollbackPhase(t, coordinator, "child"), nil)
	completePhase(t, requireExecution(t, <-rootRollback), nil)
	if status := coordinator.Snapshot().Status; status != lifecycle.RunRolledBack {
		t.Fatalf("run status = %q, want %q", status, lifecycle.RunRolledBack)
	}
}

func TestCoordinatorCleanupFailureDoesNotSuppressAncestors(t *testing.T) {
	failure := errors.New("exit failed")
	topology := coordinatorTopology(t,
		lifecycle.PhaseSpec{Name: "root"},
		lifecycle.PhaseSpec{Name: "left", DependsOn: []string{"root"}},
		lifecycle.PhaseSpec{Name: "right", DependsOn: []string{"root"}},
	)
	coordinator := openCoordinator(t, topology, memory.NewStore(), "cleanup-failure", lifecycle.Options{MaxParallel: 2})
	defer closeCoordinator(t, coordinator)
	completePhase(t, enterPhase(t, coordinator, "root"), nil)
	completePhase(t, enterPhase(t, coordinator, "left"), nil)
	completePhase(t, enterPhase(t, coordinator, "right"), nil)

	left := exitPhase(t, coordinator, "left")
	right := exitPhase(t, coordinator, "right")
	completePhase(t, left, nil)
	if err := right.Complete(context.Background(), failure); !errors.Is(err, failure) {
		t.Fatalf("failed exit completion = %v, want %v", err, failure)
	}
	completePhase(t, exitPhase(t, coordinator, "root"), nil)
	if status := coordinator.Snapshot().Status; status != lifecycle.RunFailed {
		t.Fatalf("run status = %q, want %q", status, lifecycle.RunFailed)
	}
}

func TestCoordinatorRetriesInterruptedExitAndRollback(t *testing.T) {
	exitTopology := coordinatorTopology(t, lifecycle.PhaseSpec{Name: "phase"})

	exitStore := memory.NewStore()
	exiting := openCoordinator(t, exitTopology, exitStore, "retry-exit", lifecycle.Options{})
	completePhase(t, enterPhase(t, exiting, "phase"), nil)
	if execution := exitPhase(t, exiting, "phase"); !execution.ShouldRun() {
		t.Fatal("initial exit did not require execution")
	}
	closeCoordinator(t, exiting)
	exitRestart := openCoordinator(t, exitTopology, exitStore, "retry-exit", lifecycle.Options{})
	completePhase(t, exitPhase(t, exitRestart, "phase"), nil)
	closeCoordinator(t, exitRestart)

	rollbackStore := memory.NewStore()
	rollbackTopology := coordinatorTopology(t,
		lifecycle.PhaseSpec{Name: "phase"},
		lifecycle.PhaseSpec{Name: "failing"},
	)
	rollingBack := openCoordinator(t, rollbackTopology, rollbackStore, "retry-rollback", lifecycle.Options{})
	completePhase(t, enterPhase(t, rollingBack, "phase"), nil)
	if err := enterPhase(t, rollingBack, "failing").Complete(context.Background(), errors.New("failed")); err == nil {
		t.Fatal("entry failure completion returned nil")
	}
	if execution := rollbackPhase(t, rollingBack, "phase"); !execution.ShouldRun() {
		t.Fatal("initial rollback did not require execution")
	}
	closeCoordinator(t, rollingBack)
	rollbackRestart := openCoordinator(t, rollbackTopology, rollbackStore, "retry-rollback", lifecycle.Options{})
	completePhase(t, rollbackPhase(t, rollbackRestart, "phase"), nil)
	if status := rollbackRestart.Snapshot().Status; status != lifecycle.RunRolledBack {
		t.Fatalf("run status = %q, want %q", status, lifecycle.RunRolledBack)
	}
	closeCoordinator(t, rollbackRestart)
}

func TestCoordinatorRollbackRetriesEnteringBeforeCleanup(t *testing.T) {
	failure := errors.New("entry failed")
	topology := coordinatorTopology(t,
		lifecycle.PhaseSpec{Name: "root"},
		lifecycle.PhaseSpec{Name: "child", DependsOn: []string{"root"}},
		lifecycle.PhaseSpec{Name: "failing"},
	)
	store := memory.NewStore()
	coordinator := openCoordinator(t, topology, store, "rollback-entering", lifecycle.Options{MaxParallel: 2})
	completePhase(t, enterPhase(t, coordinator, "root"), nil)
	if execution := enterPhase(t, coordinator, "child"); !execution.ShouldRun() {
		t.Fatal("child did not enter")
	}
	if err := enterPhase(t, coordinator, "failing").Complete(context.Background(), failure); !errors.Is(err, failure) {
		t.Fatalf("failure completion = %v, want %v", err, failure)
	}
	closeCoordinator(t, coordinator)

	restarted := openCoordinator(t, topology, store, "rollback-entering", lifecycle.Options{})
	defer closeCoordinator(t, restarted)
	child := enterPhase(t, restarted, "child")
	rootRollback := rollbackAsync(restarted, "root")
	assertStillWaiting(t, rootRollback)
	completePhase(t, child, nil)
	assertStillWaiting(t, rootRollback)
	completePhase(t, rollbackPhase(t, restarted, "child"), nil)
	completePhase(t, requireExecution(t, <-rootRollback), nil)
}

func TestCoordinatorRejectsStageOperationsInInvalidRunState(t *testing.T) {
	topology := coordinatorTopology(t, lifecycle.PhaseSpec{Name: "phase"})
	coordinator := openCoordinator(t, topology, memory.NewStore(), "invalid-stage", lifecycle.Options{})
	defer closeCoordinator(t, coordinator)
	if _, err := coordinator.ExitPhase(context.Background(), "phase"); err == nil {
		t.Fatal("exit was accepted before entry")
	}
	if _, err := coordinator.RollbackPhase(context.Background(), "phase"); err == nil {
		t.Fatal("rollback was accepted before entry failure")
	}
}

func TestCoordinatorRejectsExitUntilAllEntriesResolve(t *testing.T) {
	topology := coordinatorTopology(t,
		lifecycle.PhaseSpec{Name: "entered"},
		lifecycle.PhaseSpec{Name: "pending"},
	)
	coordinator := openCoordinator(t, topology, memory.NewStore(), "exit-eligibility", lifecycle.Options{})
	defer closeCoordinator(t, coordinator)
	completePhase(t, enterPhase(t, coordinator, "entered"), nil)

	if _, err := coordinator.ExitPhase(context.Background(), "entered"); err == nil {
		t.Fatal("exit was admitted while an unrelated entry remained pending")
	}
}

func coordinatorTopology(t *testing.T, phases ...lifecycle.PhaseSpec) *lifecycle.Topology {
	t.Helper()
	topology, err := lifecycle.NewTopology(phases)
	if err != nil {
		t.Fatal(err)
	}
	return topology
}

func openCoordinator(t *testing.T, topology *lifecycle.Topology, store lifecycle.Store, runID string, options lifecycle.Options) *lifecycle.Coordinator {
	t.Helper()
	coordinator, err := lifecycle.OpenCoordinator(context.Background(), topology, store, runID, options)
	if err != nil {
		t.Fatal(err)
	}
	return coordinator
}

func closeCoordinator(t *testing.T, coordinator *lifecycle.Coordinator) {
	t.Helper()
	if err := coordinator.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func enterPhase(t *testing.T, coordinator *lifecycle.Coordinator, name string) *lifecycle.Execution {
	t.Helper()
	execution, err := coordinator.EnterPhase(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	if !execution.ShouldRun() {
		t.Fatalf("phase %q did not require execution", name)
	}
	return execution
}

func enterAsync(coordinator *lifecycle.Coordinator, name string) <-chan coordinatorResult {
	result := make(chan coordinatorResult, 1)
	go func() {
		execution, err := coordinator.EnterPhase(context.Background(), name)
		result <- coordinatorResult{execution: execution, err: err}
	}()
	return result
}

func exitPhase(t *testing.T, coordinator *lifecycle.Coordinator, name string) *lifecycle.Execution {
	t.Helper()
	execution, err := coordinator.ExitPhase(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	if !execution.ShouldRun() {
		t.Fatalf("phase %q did not require exit", name)
	}
	return execution
}

func exitAsync(coordinator *lifecycle.Coordinator, name string) <-chan coordinatorResult {
	result := make(chan coordinatorResult, 1)
	go func() {
		execution, err := coordinator.ExitPhase(context.Background(), name)
		result <- coordinatorResult{execution: execution, err: err}
	}()
	return result
}

func rollbackPhase(t *testing.T, coordinator *lifecycle.Coordinator, name string) *lifecycle.Execution {
	t.Helper()
	execution, err := coordinator.RollbackPhase(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	if !execution.ShouldRun() {
		t.Fatalf("phase %q did not require rollback", name)
	}
	return execution
}

func rollbackAsync(coordinator *lifecycle.Coordinator, name string) <-chan coordinatorResult {
	result := make(chan coordinatorResult, 1)
	go func() {
		execution, err := coordinator.RollbackPhase(context.Background(), name)
		result <- coordinatorResult{execution: execution, err: err}
	}()
	return result
}

func requireExecution(t *testing.T, result coordinatorResult) *lifecycle.Execution {
	t.Helper()
	if result.err != nil {
		t.Fatal(result.err)
	}
	if result.execution == nil || !result.execution.ShouldRun() {
		t.Fatal("expected runnable execution")
	}
	return result.execution
}

func completePhase(t *testing.T, execution *lifecycle.Execution, actionErr error) {
	t.Helper()
	if err := execution.Complete(context.Background(), actionErr); !errors.Is(err, actionErr) {
		t.Fatalf("Complete error = %v, want %v", err, actionErr)
	}
}

func assertStillWaiting(t *testing.T, results <-chan coordinatorResult) {
	t.Helper()
	select {
	case result := <-results:
		t.Fatalf("operation returned before it was ready: execution=%v error=%v", result.execution, result.err)
	default:
	}
}
