package lifecycle_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	lifecycle "github.com/justusschock/lifecycle"
	"github.com/justusschock/lifecycle/memory"
)

// FR-LIFE-07
func TestEngineRunsBranchesTogetherAndJoinsBeforeReverseExit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		branchEntered := make(chan string, 2)
		releaseEntry := make(chan struct{})
		joinEntered := make(chan struct{})
		joinExited := make(chan struct{})
		branchExitStarted := make(chan string, 2)
		releaseExit := make(chan struct{})
		var branchExitCompleted atomic.Int32

		noop := func(context.Context) error { return nil }
		branch := func(name string) lifecycle.Phase {
			return lifecycle.Phase{
				Name:      name,
				DependsOn: []string{"root"},
				Enter: func(context.Context) error {
					branchEntered <- name
					<-releaseEntry
					return nil
				},
				Exit: func(context.Context) error {
					select {
					case <-joinExited:
					default:
						return errors.New("branch exited before join")
					}
					branchExitStarted <- name
					<-releaseExit
					branchExitCompleted.Add(1)
					return nil
				},
				Rollback: noop,
			}
		}
		plan, err := lifecycle.NewPlan([]lifecycle.Phase{
			{Name: "join", DependsOn: []string{"left", "right"}, Enter: func(context.Context) error { close(joinEntered); return nil }, Exit: func(context.Context) error { close(joinExited); return nil }, Rollback: noop},
			branch("right"),
			{Name: "root", Enter: noop, Exit: func(context.Context) error {
				if got := branchExitCompleted.Load(); got != 2 {
					return fmt.Errorf("root exited after %d branches", got)
				}
				return nil
			}, Rollback: noop},
			branch("left"),
		})
		if err != nil {
			t.Fatal(err)
		}
		engine, err := lifecycle.NewEngine(plan, memory.NewStore(), lifecycle.Options{MaxParallel: 2})
		if err != nil {
			t.Fatal(err)
		}

		type outcome struct {
			snapshot lifecycle.Snapshot
			err      error
		}
		result := make(chan outcome, 1)
		go func() {
			snapshot, err := engine.Run(context.Background(), "branch-run")
			result <- outcome{snapshot: snapshot, err: err}
		}()

		seenEntries := map[string]bool{<-branchEntered: true, <-branchEntered: true}
		if !seenEntries["left"] || !seenEntries["right"] {
			t.Fatalf("entered branches = %v", seenEntries)
		}
		synctest.Wait()
		select {
		case <-joinEntered:
			t.Fatal("join entered before both branches completed")
		default:
		}
		close(releaseEntry)
		<-joinEntered

		seenExits := map[string]bool{<-branchExitStarted: true, <-branchExitStarted: true}
		if !seenExits["left"] || !seenExits["right"] {
			t.Fatalf("exiting branches = %v", seenExits)
		}
		synctest.Wait()
		select {
		case got := <-result:
			t.Fatalf("run returned before branches exited: %v", got.err)
		default:
		}
		close(releaseExit)

		got := <-result
		if got.err != nil {
			t.Fatal(got.err)
		}
		if got.snapshot.Status != lifecycle.RunSucceeded {
			t.Fatalf("status = %q, want %q", got.snapshot.Status, lifecycle.RunSucceeded)
		}
		for name, state := range got.snapshot.Phases {
			if state.Status != lifecycle.PhaseExited {
				t.Fatalf("phase %q status = %q, want %q", name, state.Status, lifecycle.PhaseExited)
			}
		}
	})
}

// FR-LIFE-05
func TestEngineRunsArbitraryDAGInDependencyOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dependencies := map[string][]string{
			"config":    nil,
			"network":   nil,
			"left":      {"config"},
			"right":     {"config"},
			"probe":     {"network"},
			"assembled": {"left", "right"},
			"observed":  {"probe", "right"},
			"publish-a": {"assembled"},
			"publish-b": {"assembled"},
			"audit":     {"observed"},
			"done":      {"audit", "publish-a", "publish-b"},
		}
		dependents := make(map[string][]string)
		for name, required := range dependencies {
			for _, dependency := range required {
				dependents[dependency] = append(dependents[dependency], name)
			}
		}

		var mu sync.Mutex
		entered := make(map[string]bool)
		exited := make(map[string]bool)
		makePhase := func(name string) lifecycle.Phase {
			return lifecycle.Phase{
				Name:      name,
				DependsOn: dependencies[name],
				Enter: func(context.Context) error {
					mu.Lock()
					defer mu.Unlock()
					for _, dependency := range dependencies[name] {
						if !entered[dependency] {
							return fmt.Errorf("%s entered before %s", name, dependency)
						}
					}
					entered[name] = true
					return nil
				},
				Exit: func(context.Context) error {
					mu.Lock()
					defer mu.Unlock()
					for _, dependent := range dependents[name] {
						if !exited[dependent] {
							return fmt.Errorf("%s exited before %s", name, dependent)
						}
					}
					exited[name] = true
					return nil
				},
				Rollback: func(context.Context) error { return nil },
			}
		}

		order := []string{"done", "right", "network", "publish-b", "config", "observed", "left", "audit", "probe", "assembled", "publish-a"}
		phases := make([]lifecycle.Phase, 0, len(order))
		for _, name := range order {
			phases = append(phases, makePhase(name))
		}
		plan, err := lifecycle.NewPlan(phases)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := levelNames(plan.Levels()), [][]string{{"config", "network"}, {"left", "probe", "right"}, {"assembled", "observed"}, {"audit", "publish-a", "publish-b"}, {"done"}}; !reflect.DeepEqual(got, want) {
			t.Fatalf("levels = %v, want %v", got, want)
		}
		engine, err := lifecycle.NewEngine(plan, memory.NewStore(), lifecycle.Options{MaxParallel: 4})
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := engine.Run(context.Background(), "arbitrary-dag")
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.Status != lifecycle.RunSucceeded {
			t.Fatalf("status = %q, want %q", snapshot.Status, lifecycle.RunSucceeded)
		}
		mu.Lock()
		defer mu.Unlock()
		if len(entered) != len(dependencies) || len(exited) != len(dependencies) {
			t.Fatalf("entered=%d exited=%d, want %d each", len(entered), len(exited), len(dependencies))
		}
	})
}

// FR-LIFE-08
func TestEngineCancelsFailingLevelAndRollsBackOnlyEnteredPhases(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		want := errors.New("branch failed")
		started := make(chan string, 3)
		allowEntered := make(chan struct{})
		enteredFinished := make(chan struct{})
		fail := make(chan struct{})
		peerCause := make(chan error, 1)
		enteredRolledBack := make(chan struct{})
		var unexpectedRollbacks atomic.Int32

		noop := func(context.Context) error { return nil }
		plan, err := lifecycle.NewPlan([]lifecycle.Phase{
			{Name: "root", Enter: noop, Exit: noop, Rollback: func(context.Context) error {
				select {
				case <-enteredRolledBack:
					return nil
				default:
					return errors.New("root rolled back before entered child")
				}
			}},
			{Name: "entered", DependsOn: []string{"root"}, Enter: func(context.Context) error {
				started <- "entered"
				<-allowEntered
				close(enteredFinished)
				return nil
			}, Exit: noop, Rollback: func(context.Context) error { close(enteredRolledBack); return nil }},
			{Name: "bad", DependsOn: []string{"root"}, Enter: func(context.Context) error {
				started <- "bad"
				<-fail
				return want
			}, Exit: noop, Rollback: func(context.Context) error { unexpectedRollbacks.Add(1); return nil }},
			{Name: "peer", DependsOn: []string{"root"}, Enter: func(ctx context.Context) error {
				started <- "peer"
				<-ctx.Done()
				peerCause <- context.Cause(ctx)
				return context.Cause(ctx)
			}, Exit: noop, Rollback: func(context.Context) error { unexpectedRollbacks.Add(1); return nil }},
			{Name: "join", DependsOn: []string{"entered", "bad", "peer"}, Enter: func(context.Context) error { return errors.New("join must not enter") }, Exit: noop, Rollback: noop},
		})
		if err != nil {
			t.Fatal(err)
		}
		engine, err := lifecycle.NewEngine(plan, memory.NewStore(), lifecycle.Options{MaxParallel: 3})
		if err != nil {
			t.Fatal(err)
		}

		type outcome struct {
			snapshot lifecycle.Snapshot
			err      error
		}
		result := make(chan outcome, 1)
		go func() {
			snapshot, err := engine.Run(context.Background(), "rollback-run")
			result <- outcome{snapshot: snapshot, err: err}
		}()
		seen := map[string]bool{<-started: true, <-started: true, <-started: true}
		if !seen["entered"] || !seen["bad"] || !seen["peer"] {
			t.Fatalf("started = %v", seen)
		}
		close(allowEntered)
		<-enteredFinished
		close(fail)
		if cause := <-peerCause; !errors.Is(cause, want) {
			t.Fatalf("peer cause = %v, want %v", cause, want)
		}

		got := <-result
		if !errors.Is(got.err, want) {
			t.Fatalf("Run error = %v, want %v", got.err, want)
		}
		if got.snapshot.Status != lifecycle.RunRolledBack {
			t.Fatalf("status = %q, want %q", got.snapshot.Status, lifecycle.RunRolledBack)
		}
		if got.snapshot.Phases["root"].Status != lifecycle.PhaseRolledBack || got.snapshot.Phases["entered"].Status != lifecycle.PhaseRolledBack {
			t.Fatalf("rollback states = root:%q entered:%q", got.snapshot.Phases["root"].Status, got.snapshot.Phases["entered"].Status)
		}
		if got := unexpectedRollbacks.Load(); got != 0 {
			t.Fatalf("failed phase rollback calls = %d, want 0", got)
		}
	})
}

// FR-LIFE-09
func TestEngineAttemptsEveryEnteredRollbackAfterRollbackFailure(t *testing.T) {
	entryFailure := errors.New("entry failed")
	rollbackFailure := errors.New("rollback failed")
	var mu sync.Mutex
	rollbackCalls := make(map[string]int)
	noop := func(context.Context) error { return nil }
	child := func(name string, rollbackErr error) lifecycle.Phase {
		return lifecycle.Phase{
			Name:      name,
			DependsOn: []string{"root"},
			Enter:     noop,
			Exit:      noop,
			Rollback: func(context.Context) error {
				mu.Lock()
				rollbackCalls[name]++
				mu.Unlock()
				return rollbackErr
			},
		}
	}
	plan, err := lifecycle.NewPlan([]lifecycle.Phase{
		{Name: "root", Enter: noop, Exit: noop, Rollback: func(context.Context) error {
			mu.Lock()
			rollbackCalls["root"]++
			mu.Unlock()
			return nil
		}},
		child("a", rollbackFailure),
		child("b", nil),
		child("c", nil),
		{Name: "fail", DependsOn: []string{"a", "b", "c"}, Enter: func(context.Context) error { return entryFailure }, Exit: noop, Rollback: noop},
	})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := lifecycle.NewEngine(plan, memory.NewStore(), lifecycle.Options{MaxParallel: 1})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := engine.Run(context.Background(), "best-effort-rollback")
	if !errors.Is(err, entryFailure) || !errors.Is(err, rollbackFailure) {
		t.Fatalf("Run error = %v, want entry and rollback failures", err)
	}
	if snapshot.Status != lifecycle.RunFailed {
		t.Fatalf("status = %q, want %q", snapshot.Status, lifecycle.RunFailed)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, name := range []string{"root", "a", "b", "c"} {
		if got := rollbackCalls[name]; got != 1 {
			t.Fatalf("rollback calls for %q = %d, want 1; all calls = %v", name, got, rollbackCalls)
		}
	}
}

// FR-LIFE-10
func TestEnginePersistsEnteringBeforeCallingAction(t *testing.T) {
	store := newRecordingStore()
	var callbackSaw lifecycle.PhaseStatus
	plan, err := lifecycle.NewPlan([]lifecycle.Phase{{
		Name: "start",
		Enter: func(context.Context) error {
			callbackSaw = store.latest().Phases["start"].Status
			return nil
		},
		Exit:     func(context.Context) error { return nil },
		Rollback: func(context.Context) error { return nil },
	}})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := lifecycle.NewEngine(plan, store, lifecycle.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Run(context.Background(), "persisted-run"); err != nil {
		t.Fatal(err)
	}
	if callbackSaw != lifecycle.PhaseEntering {
		t.Fatalf("callback observed %q, want %q", callbackSaw, lifecycle.PhaseEntering)
	}
	if got, want := store.phaseStatuses("start"), []lifecycle.PhaseStatus{lifecycle.PhaseEntering, lifecycle.PhaseEntered, lifecycle.PhaseExiting, lifecycle.PhaseExited}; !reflect.DeepEqual(got, want) {
		t.Fatalf("saved statuses = %v, want %v", got, want)
	}
}

// FR-LIFE-10
func TestEngineReturnsPreActionStoreFailureWithoutRunningAction(t *testing.T) {
	want := errors.New("store unavailable")
	store := newRecordingStore()
	store.failAt = 1
	store.failure = want
	var calls atomic.Int32
	plan := singlePhasePlan(t, "start", func(context.Context) error { calls.Add(1); return nil }, func(context.Context) error { return nil }, func(context.Context) error { return nil })
	engine, err := lifecycle.NewEngine(plan, store, lifecycle.Options{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := engine.Run(context.Background(), "failed-save")
	if !errors.Is(err, want) {
		t.Fatalf("Run error = %v, want %v", err, want)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("action calls = %d, want 0", got)
	}
	if got := snapshot.Phases["start"].Status; got != lifecycle.PhasePending {
		t.Fatalf("returned phase status = %q, want last durably saved status %q", got, lifecycle.PhasePending)
	}
	if _, err := store.Load(context.Background(), "failed-save"); !errors.Is(err, lifecycle.ErrSnapshotNotFound) {
		t.Fatalf("Load after rejected first save error = %v, want %v", err, lifecycle.ErrSnapshotNotFound)
	}
}

// FR-LIFE-10
func TestEngineLeavesInterruptedActionsRestartableAfterTerminalSaveFailure(t *testing.T) {
	t.Run("enter", func(t *testing.T) {
		want := errors.New("entered status save failed")
		store := newRecordingStore()
		store.failAt = 2
		store.failure = want
		var enterCalls atomic.Int32
		var rollbackCalls atomic.Int32
		noop := func(context.Context) error { return nil }
		plan := singlePhasePlan(t, "phase", func(context.Context) error { enterCalls.Add(1); return nil }, noop, func(context.Context) error { rollbackCalls.Add(1); return nil })
		engine, err := lifecycle.NewEngine(plan, store, lifecycle.Options{})
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := engine.Run(context.Background(), "interrupted-enter")
		if !errors.Is(err, want) {
			t.Fatalf("first Run error = %v, want %v", err, want)
		}
		if snapshot.Status != lifecycle.RunRunning || snapshot.Phases["phase"].Status != lifecycle.PhaseEntering {
			t.Fatalf("first snapshot = run:%q phase:%q, want running/entering", snapshot.Status, snapshot.Phases["phase"].Status)
		}
		if rollbackCalls.Load() != 0 {
			t.Fatalf("rollback calls after persistence error = %d, want 0", rollbackCalls.Load())
		}
		if _, err := engine.Run(context.Background(), "interrupted-enter"); err != nil {
			t.Fatal(err)
		}
		if enterCalls.Load() != 2 || rollbackCalls.Load() != 0 {
			t.Fatalf("calls after restart = enter:%d rollback:%d, want 2 and 0", enterCalls.Load(), rollbackCalls.Load())
		}
	})

	t.Run("exit", func(t *testing.T) {
		want := errors.New("exited status save failed")
		store := newRecordingStore()
		store.failAt = 4
		store.failure = want
		var enterCalls atomic.Int32
		var exitCalls atomic.Int32
		plan := singlePhasePlan(t, "phase", func(context.Context) error { enterCalls.Add(1); return nil }, func(context.Context) error { exitCalls.Add(1); return nil }, func(context.Context) error { return nil })
		engine, err := lifecycle.NewEngine(plan, store, lifecycle.Options{})
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := engine.Run(context.Background(), "interrupted-exit")
		if !errors.Is(err, want) {
			t.Fatalf("first Run error = %v, want %v", err, want)
		}
		if snapshot.Status != lifecycle.RunRunning || snapshot.Phases["phase"].Status != lifecycle.PhaseExiting {
			t.Fatalf("first snapshot = run:%q phase:%q, want running/exiting", snapshot.Status, snapshot.Phases["phase"].Status)
		}
		if _, err := engine.Run(context.Background(), "interrupted-exit"); err != nil {
			t.Fatal(err)
		}
		if enterCalls.Load() != 1 || exitCalls.Load() != 2 {
			t.Fatalf("calls after restart = enter:%d exit:%d, want 1 and 2", enterCalls.Load(), exitCalls.Load())
		}
	})

	t.Run("rollback", func(t *testing.T) {
		entryFailure := errors.New("entry failed")
		saveFailure := errors.New("rolled-back status save failed")
		store := newRecordingStore()
		store.failAt = 6
		store.failure = saveFailure
		var rollbackCalls atomic.Int32
		noop := func(context.Context) error { return nil }
		plan, err := lifecycle.NewPlan([]lifecycle.Phase{
			{Name: "root", Enter: noop, Exit: noop, Rollback: func(context.Context) error { rollbackCalls.Add(1); return nil }},
			{Name: "fail", DependsOn: []string{"root"}, Enter: func(context.Context) error { return entryFailure }, Exit: noop, Rollback: noop},
		})
		if err != nil {
			t.Fatal(err)
		}
		engine, err := lifecycle.NewEngine(plan, store, lifecycle.Options{})
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := engine.Run(context.Background(), "interrupted-rollback")
		if !errors.Is(err, entryFailure) || !errors.Is(err, saveFailure) {
			t.Fatalf("first Run error = %v, want entry and save failures", err)
		}
		if snapshot.Status != lifecycle.RunRollingBack || snapshot.Phases["root"].Status != lifecycle.PhaseRollingBack {
			t.Fatalf("first snapshot = run:%q root:%q, want rolling_back/rolling_back", snapshot.Status, snapshot.Phases["root"].Status)
		}
		if _, err := engine.Run(context.Background(), "interrupted-rollback"); err == nil || err.Error() != entryFailure.Error() {
			t.Fatalf("second Run error = %v, want recovered entry failure", err)
		}
		if rollbackCalls.Load() != 2 {
			t.Fatalf("rollback calls after restart = %d, want 2", rollbackCalls.Load())
		}
	})
}

// FR-LIFE-10
func TestEngineDoesNotExposeFailedRunStatusSaveAsCurrent(t *testing.T) {
	want := errors.New("final status save failed")
	store := newRecordingStore()
	store.failAt = 5
	store.failure = want
	noop := func(context.Context) error { return nil }
	plan := singlePhasePlan(t, "phase", noop, noop, noop)
	engine, err := lifecycle.NewEngine(plan, store, lifecycle.Options{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := engine.Run(context.Background(), "final-save")
	if !errors.Is(err, want) {
		t.Fatalf("Run error = %v, want %v", err, want)
	}
	durable := store.latest()
	if snapshot.Status != durable.Status {
		t.Fatalf("returned status = %q, durable status = %q", snapshot.Status, durable.Status)
	}
	if snapshot.Status != lifecycle.RunRunning {
		t.Fatalf("returned status = %q, want last saved status %q", snapshot.Status, lifecycle.RunRunning)
	}
}

// FR-LIFE-10
func TestEngineLeavesExitFailureNonterminalUntilFinalStatusIsDurable(t *testing.T) {
	actionFailure := errors.New("exit failed")
	statusSaveFailure := errors.New("status save failed")
	store := newRecordingStore()
	store.failAt = 5
	store.failure = statusSaveFailure
	var enterCalls atomic.Int32
	var exitCalls atomic.Int32
	plan := singlePhasePlan(t, "phase", func(context.Context) error {
		enterCalls.Add(1)
		return nil
	}, func(context.Context) error {
		exitCalls.Add(1)
		return actionFailure
	}, func(context.Context) error { return nil })
	engine, err := lifecycle.NewEngine(plan, store, lifecycle.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Run(context.Background(), "failed-exit"); !errors.Is(err, actionFailure) || !errors.Is(err, statusSaveFailure) {
		t.Fatalf("first Run error = %v, want action and status-save failures", err)
	}
	if got := store.latest().Status; got != lifecycle.RunRunning {
		t.Fatalf("durable run status = %q, want cleanup status %q", got, lifecycle.RunRunning)
	}

	restarted, err := lifecycle.NewEngine(plan, store, lifecycle.Options{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := restarted.Run(context.Background(), "failed-exit")
	if err == nil || err.Error() != actionFailure.Error() {
		t.Fatalf("restart error = %v, want recovered action failure", err)
	}
	if snapshot.Status != lifecycle.RunFailed {
		t.Fatalf("restart status = %q, want %q", snapshot.Status, lifecycle.RunFailed)
	}
	if enterCalls.Load() != 1 || exitCalls.Load() != 1 {
		t.Fatalf("calls after restart = enter:%d exit:%d, want 1 each", enterCalls.Load(), exitCalls.Load())
	}
}

// FR-LIFE-11
func TestEngineDoesNotRepeatCompletedEntryAfterRestart(t *testing.T) {
	var completedCalls atomic.Int32
	var retriedCalls atomic.Int32
	noop := func(context.Context) error { return nil }
	plan, err := lifecycle.NewPlan([]lifecycle.Phase{
		{Name: "completed", Enter: func(context.Context) error { completedCalls.Add(1); return nil }, Exit: noop, Rollback: noop},
		{Name: "retry", Enter: func(context.Context) error { retriedCalls.Add(1); return nil }, Exit: noop, Rollback: noop},
	})
	if err != nil {
		t.Fatal(err)
	}
	store := memory.NewStore()
	now := time.Unix(100, 0)
	if err := store.Save(context.Background(), lifecycle.Snapshot{
		RunID:      "run",
		PlanDigest: plan.Digest(),
		Status:     lifecycle.RunRunning,
		Phases: map[string]lifecycle.PhaseState{
			"completed": {Status: lifecycle.PhaseEntered, Attempts: 1, UpdatedAt: now},
			"retry":     {Status: lifecycle.PhaseEntering, Attempts: 1, UpdatedAt: now},
		},
		UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	engine, err := lifecycle.NewEngine(plan, store, lifecycle.Options{MaxParallel: 2})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := engine.Run(context.Background(), "run")
	if err != nil {
		t.Fatal(err)
	}
	if completedCalls.Load() != 0 || retriedCalls.Load() != 1 {
		t.Fatalf("entry calls = completed:%d retry:%d, want 0 and 1", completedCalls.Load(), retriedCalls.Load())
	}
	if got := snapshot.Phases["retry"].Attempts; got != 3 {
		t.Fatalf("retry attempts = %d, want 3 after retried entry and exit", got)
	}
}

// FR-LIFE-11
func TestEngineRetriesInterruptedExitAndRollback(t *testing.T) {
	t.Run("exit", func(t *testing.T) {
		var enterCalls atomic.Int32
		var exitCalls atomic.Int32
		plan := singlePhasePlan(t, "phase", func(context.Context) error { enterCalls.Add(1); return nil }, func(context.Context) error { exitCalls.Add(1); return nil }, func(context.Context) error { return nil })
		store := memory.NewStore()
		saveSnapshot(t, store, lifecycle.Snapshot{RunID: "exit", PlanDigest: plan.Digest(), Status: lifecycle.RunRunning, Phases: map[string]lifecycle.PhaseState{"phase": {Status: lifecycle.PhaseExiting, Attempts: 2}}})
		engine, err := lifecycle.NewEngine(plan, store, lifecycle.Options{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := engine.Run(context.Background(), "exit"); err != nil {
			t.Fatal(err)
		}
		if enterCalls.Load() != 0 || exitCalls.Load() != 1 {
			t.Fatalf("calls = enter:%d exit:%d, want 0 and 1", enterCalls.Load(), exitCalls.Load())
		}
	})

	t.Run("rollback", func(t *testing.T) {
		var enterCalls atomic.Int32
		var exitCalls atomic.Int32
		var rollbackCalls atomic.Int32
		noop := func(context.Context) error { return nil }
		plan, err := lifecycle.NewPlan([]lifecycle.Phase{
			{Name: "phase", Enter: func(context.Context) error { enterCalls.Add(1); return nil }, Exit: func(context.Context) error { exitCalls.Add(1); return nil }, Rollback: func(context.Context) error { rollbackCalls.Add(1); return nil }},
			{Name: "failed", DependsOn: []string{"phase"}, Enter: noop, Exit: noop, Rollback: noop},
		})
		if err != nil {
			t.Fatal(err)
		}
		store := memory.NewStore()
		saveSnapshot(t, store, lifecycle.Snapshot{
			RunID: "rollback", PlanDigest: plan.Digest(), Status: lifecycle.RunRollingBack,
			Phases: map[string]lifecycle.PhaseState{
				"phase":  {Status: lifecycle.PhaseRollingBack, Attempts: 2},
				"failed": {Status: lifecycle.PhaseEnterFailed, Attempts: 1, LastError: "entry failed"},
			},
		})
		engine, err := lifecycle.NewEngine(plan, store, lifecycle.Options{})
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := engine.Run(context.Background(), "rollback")
		if err == nil || err.Error() != "entry failed" {
			t.Fatalf("Run error = %v, want persisted entry failure", err)
		}
		if enterCalls.Load() != 0 || exitCalls.Load() != 0 || rollbackCalls.Load() != 1 {
			t.Fatalf("calls = enter:%d exit:%d rollback:%d, want 0, 0, 1", enterCalls.Load(), exitCalls.Load(), rollbackCalls.Load())
		}
		if snapshot.Status != lifecycle.RunRolledBack {
			t.Fatalf("status = %q, want %q", snapshot.Status, lifecycle.RunRolledBack)
		}
	})
}

// FR-LIFE-12
func TestEngineRejectsChangedPlanAndTerminalRunBeforeActions(t *testing.T) {
	var calls atomic.Int32
	plan := singlePhasePlan(t, "phase", func(context.Context) error { calls.Add(1); return nil }, func(context.Context) error { calls.Add(1); return nil }, func(context.Context) error { calls.Add(1); return nil })

	t.Run("changed plan", func(t *testing.T) {
		store := memory.NewStore()
		saveSnapshot(t, store, lifecycle.Snapshot{RunID: "changed", PlanDigest: "different", Status: lifecycle.RunRunning, Phases: map[string]lifecycle.PhaseState{"phase": {Status: lifecycle.PhasePending}}})
		engine, err := lifecycle.NewEngine(plan, store, lifecycle.Options{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := engine.Run(context.Background(), "changed"); !errors.Is(err, lifecycle.ErrPlanChanged) {
			t.Fatalf("Run error = %v, want %v", err, lifecycle.ErrPlanChanged)
		}
	})

	t.Run("terminal run", func(t *testing.T) {
		store := memory.NewStore()
		saveSnapshot(t, store, lifecycle.Snapshot{RunID: "terminal", PlanDigest: plan.Digest(), Status: lifecycle.RunSucceeded, Phases: map[string]lifecycle.PhaseState{"phase": {Status: lifecycle.PhaseExited, Attempts: 2}}})
		engine, err := lifecycle.NewEngine(plan, store, lifecycle.Options{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := engine.Run(context.Background(), "terminal"); !errors.Is(err, lifecycle.ErrRunTerminal) {
			t.Fatalf("Run error = %v, want %v", err, lifecycle.ErrRunTerminal)
		}
	})

	if got := calls.Load(); got != 0 {
		t.Fatalf("actions called %d times", got)
	}
}

// FR-LIFE-14
func TestEngineOrdersAllRecoveredEntryFailuresByPlanOrder(t *testing.T) {
	var calls atomic.Int32
	noop := func(context.Context) error { calls.Add(1); return nil }
	plan, err := lifecycle.NewPlan([]lifecycle.Phase{
		{Name: "root", Enter: noop, Exit: noop, Rollback: noop},
		{Name: "b", DependsOn: []string{"root"}, Enter: noop, Exit: noop, Rollback: noop},
		{Name: "a", DependsOn: []string{"root"}, Enter: noop, Exit: noop, Rollback: noop},
	})
	if err != nil {
		t.Fatal(err)
	}
	store := memory.NewStore()
	phases := make(map[string]lifecycle.PhaseState)
	phases["b"] = lifecycle.PhaseState{Status: lifecycle.PhaseEnterFailed, Attempts: 1, LastError: "b failed"}
	phases["a"] = lifecycle.PhaseState{Status: lifecycle.PhaseEnterFailed, Attempts: 1, LastError: "a failed"}
	phases["root"] = lifecycle.PhaseState{Status: lifecycle.PhaseEntered, Attempts: 1}
	saveSnapshot(t, store, lifecycle.Snapshot{RunID: "recovered-failures", PlanDigest: plan.Digest(), Status: lifecycle.RunRollingBack, Phases: phases})
	engine, err := lifecycle.NewEngine(plan, store, lifecycle.Options{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = engine.Run(context.Background(), "recovered-failures")
	if err == nil || err.Error() != "a failed\nb failed" {
		t.Fatalf("Run error = %q, want deterministic joined failures", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("action calls = %d, want only root rollback", got)
	}
}

// FR-LIFE-04
func TestEngineRejectsInvalidConstructionAndRunID(t *testing.T) {
	plan := singlePhasePlan(t, "phase", func(context.Context) error { return nil }, func(context.Context) error { return nil }, func(context.Context) error { return nil })
	if _, err := lifecycle.NewEngine(nil, memory.NewStore(), lifecycle.Options{}); err == nil {
		t.Fatal("NewEngine accepted a nil plan")
	}
	if _, err := lifecycle.NewEngine(plan, nil, lifecycle.Options{}); err == nil {
		t.Fatal("NewEngine accepted a nil store")
	}
	var typedNilStore *memory.Store
	if _, err := lifecycle.NewEngine(plan, typedNilStore, lifecycle.Options{}); err == nil {
		t.Fatal("NewEngine accepted a typed-nil store")
	}
	if _, err := lifecycle.NewEngine(plan, memory.NewStore(), lifecycle.Options{MaxParallel: -1}); err == nil {
		t.Fatal("NewEngine accepted negative parallelism")
	}
	engine, err := lifecycle.NewEngine(plan, memory.NewStore(), lifecycle.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Run(context.Background(), ""); !errors.Is(err, lifecycle.ErrInvalidPhase) {
		t.Fatalf("Run empty ID error = %v, want %v", err, lifecycle.ErrInvalidPhase)
	}
}

type recordingStore struct {
	mu       sync.Mutex
	snapshot *lifecycle.Snapshot
	saves    []lifecycle.Snapshot
	saveCall int
	failAt   int
	failure  error
}

type recordingSession struct {
	store *recordingStore
	runID string
}

func newRecordingStore() *recordingStore {
	return &recordingStore{}
}

func (s *recordingStore) Acquire(_ context.Context, runID string) (lifecycle.Session, error) {
	return &recordingSession{store: s, runID: runID}, nil
}

func (s *recordingSession) RunID() string { return s.runID }

func (s *recordingSession) Load(ctx context.Context) (lifecycle.Snapshot, error) {
	return s.store.Load(ctx, s.runID)
}

func (s *recordingSession) Save(ctx context.Context, snapshot lifecycle.Snapshot) error {
	return s.store.Save(ctx, snapshot)
}

func (s *recordingSession) Release(context.Context) error { return nil }

func (s *recordingStore) Load(context.Context, string) (lifecycle.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapshot == nil {
		return lifecycle.Snapshot{}, lifecycle.ErrSnapshotNotFound
	}
	return cloneSnapshot(*s.snapshot), nil
}

func (s *recordingStore) Save(_ context.Context, snapshot lifecycle.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saveCall++
	if s.failAt == s.saveCall {
		return s.failure
	}
	copy := cloneSnapshot(snapshot)
	s.snapshot = &copy
	s.saves = append(s.saves, copy)
	return nil
}

func (s *recordingStore) latest() lifecycle.Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapshot == nil {
		return lifecycle.Snapshot{}
	}
	return cloneSnapshot(*s.snapshot)
}

func (s *recordingStore) phaseStatuses(name string) []lifecycle.PhaseStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	statuses := make([]lifecycle.PhaseStatus, 0, len(s.saves))
	var previous lifecycle.PhaseStatus
	for _, snapshot := range s.saves {
		status := snapshot.Phases[name].Status
		if status != previous {
			statuses = append(statuses, status)
			previous = status
		}
	}
	return statuses
}

func cloneSnapshot(snapshot lifecycle.Snapshot) lifecycle.Snapshot {
	copy := snapshot
	copy.Phases = make(map[string]lifecycle.PhaseState, len(snapshot.Phases))
	for name, state := range snapshot.Phases {
		copy.Phases[name] = state
	}
	return copy
}

func singlePhasePlan(t *testing.T, name string, enter, exit, rollback lifecycle.Action) *lifecycle.Plan {
	t.Helper()
	plan, err := lifecycle.NewPlan([]lifecycle.Phase{{Name: name, Enter: enter, Exit: exit, Rollback: rollback}})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func saveSnapshot(t *testing.T, store lifecycle.Store, snapshot lifecycle.Snapshot) {
	t.Helper()
	session, err := store.Acquire(context.Background(), snapshot.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Save(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	if err := session.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func levelNames(levels [][]lifecycle.Phase) [][]string {
	result := make([][]string, len(levels))
	for levelIndex, level := range levels {
		for _, phase := range level {
			result[levelIndex] = append(result[levelIndex], phase.Name)
		}
	}
	return result
}
