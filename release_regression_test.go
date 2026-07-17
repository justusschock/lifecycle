package lifecycle_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	lifecycle "github.com/justusschock/go-lifecycle"
	"github.com/justusschock/go-lifecycle/memory"
)

func TestEngineOwnsEachRunExclusively(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	noop := func(context.Context) error { return nil }
	plan := singlePhasePlan(t, "phase", func(context.Context) error {
		calls.Add(1)
		close(started)
		<-release
		return nil
	}, noop, noop)
	engine, err := lifecycle.NewEngine(plan, memory.NewStore(), lifecycle.Options{})
	if err != nil {
		t.Fatal(err)
	}

	first := make(chan error, 1)
	go func() {
		_, err := engine.Run(context.Background(), "exclusive")
		first <- err
	}()
	<-started

	if _, err := engine.Run(context.Background(), "exclusive"); !errors.Is(err, lifecycle.ErrRunOwned) {
		t.Fatalf("concurrent Run error = %v, want %v", err, lifecycle.ErrRunOwned)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("entry calls = %d, want 1", got)
	}
}

func TestEngineCancellationDoesNotStrandRunOwnership(t *testing.T) {
	started := make(chan struct{})
	var calls atomic.Int32
	noop := func(context.Context) error { return nil }
	plan := singlePhasePlan(t, "phase", func(ctx context.Context) error {
		if calls.Add(1) == 1 {
			close(started)
			<-ctx.Done()
			return context.Cause(ctx)
		}
		return nil
	}, noop, noop)
	engine, err := lifecycle.NewEngine(plan, memory.NewStore(), lifecycle.Options{})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() {
		_, err := engine.Run(ctx, "canceled")
		first <- err
	}()
	<-started
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Run error = %v, want %v", err, context.Canceled)
	}

	if _, err := engine.Run(context.Background(), "canceled"); err != nil {
		t.Fatalf("Run after cancellation = %v, want successful reacquisition", err)
	}
}

func TestEngineSchedulesNewlyReadyPhasesWithoutLevelBarrier(t *testing.T) {
	leftDone := make(chan struct{})
	rightStarted := make(chan struct{})
	releaseRight := make(chan struct{})
	childStarted := make(chan struct{})
	noop := func(context.Context) error { return nil }
	plan, err := lifecycle.NewPlan([]lifecycle.Phase{
		{Name: "left", Enter: func(context.Context) error { close(leftDone); return nil }, Exit: noop, Rollback: noop},
		{Name: "right", Enter: func(context.Context) error { close(rightStarted); <-releaseRight; return nil }, Exit: noop, Rollback: noop},
		{Name: "child", DependsOn: []string{"left"}, Enter: func(context.Context) error { close(childStarted); return nil }, Exit: noop, Rollback: noop},
	})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := lifecycle.NewEngine(plan, memory.NewStore(), lifecycle.Options{MaxParallel: 3})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := engine.Run(context.Background(), "ready")
		done <- err
	}()
	<-leftDone
	<-rightStarted
	select {
	case <-childStarted:
	case <-time.After(time.Second):
		close(releaseRight)
		<-done
		t.Fatal("child waited for an unrelated root")
	}
	close(releaseRight)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestEngineAdmitsReadyPhasesLexically(t *testing.T) {
	var mu sync.Mutex
	var entered []string
	noop := func(context.Context) error { return nil }
	enter := func(name string) lifecycle.Action {
		return func(context.Context) error {
			mu.Lock()
			entered = append(entered, name)
			mu.Unlock()
			return nil
		}
	}
	plan, err := lifecycle.NewPlan([]lifecycle.Phase{
		{Name: "z", Enter: enter("z"), Exit: noop, Rollback: noop},
		{Name: "b", DependsOn: []string{"a"}, Enter: enter("b"), Exit: noop, Rollback: noop},
		{Name: "a", Enter: enter("a"), Exit: noop, Rollback: noop},
	})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := lifecycle.NewEngine(plan, memory.NewStore(), lifecycle.Options{MaxParallel: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Run(context.Background(), "lexical"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if want := []string{"a", "b", "z"}; !reflect.DeepEqual(entered, want) {
		t.Fatalf("entry admission = %v, want %v", entered, want)
	}
}

func TestEnginePersistsStageSpecificFailuresAndCleansBestEffort(t *testing.T) {
	t.Run("exit", func(t *testing.T) {
		exitFailure := errors.New("a exit failed")
		var callsMu sync.Mutex
		var calls []string
		noop := func(context.Context) error { return nil }
		exit := func(name string, result error) lifecycle.Action {
			return func(context.Context) error {
				callsMu.Lock()
				calls = append(calls, name)
				callsMu.Unlock()
				return result
			}
		}
		plan, err := lifecycle.NewPlan([]lifecycle.Phase{
			{Name: "root", Enter: noop, Exit: exit("root", nil), Rollback: noop},
			{Name: "a", DependsOn: []string{"root"}, Enter: noop, Exit: exit("a", exitFailure), Rollback: noop},
			{Name: "b", DependsOn: []string{"root"}, Enter: noop, Exit: exit("b", nil), Rollback: noop},
		})
		if err != nil {
			t.Fatal(err)
		}
		engine, err := lifecycle.NewEngine(plan, memory.NewStore(), lifecycle.Options{MaxParallel: 1})
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := engine.Run(context.Background(), "exit-failure")
		if !errors.Is(err, exitFailure) {
			t.Fatalf("Run error = %v, want %v", err, exitFailure)
		}
		if snapshot.Status != lifecycle.RunFailed || snapshot.Phases["a"].Status != lifecycle.PhaseExitFailed {
			t.Fatalf("snapshot = run:%q a:%q", snapshot.Status, snapshot.Phases["a"].Status)
		}
		if snapshot.Phases["b"].Status != lifecycle.PhaseExited || snapshot.Phases["root"].Status != lifecycle.PhaseExited {
			t.Fatalf("best-effort states = b:%q root:%q", snapshot.Phases["b"].Status, snapshot.Phases["root"].Status)
		}
		callsMu.Lock()
		defer callsMu.Unlock()
		if want := []string{"a", "b", "root"}; !reflect.DeepEqual(calls, want) {
			t.Fatalf("exit calls = %v, want %v", calls, want)
		}
	})

	t.Run("rollback", func(t *testing.T) {
		entryFailure := errors.New("entry failed")
		rollbackFailure := errors.New("child rollback failed")
		var rootCalls atomic.Int32
		noop := func(context.Context) error { return nil }
		plan, err := lifecycle.NewPlan([]lifecycle.Phase{
			{Name: "root", Enter: noop, Exit: noop, Rollback: func(context.Context) error { rootCalls.Add(1); return nil }},
			{Name: "child", DependsOn: []string{"root"}, Enter: noop, Exit: noop, Rollback: func(context.Context) error { return rollbackFailure }},
			{Name: "fail", DependsOn: []string{"child"}, Enter: func(context.Context) error { return entryFailure }, Exit: noop, Rollback: noop},
		})
		if err != nil {
			t.Fatal(err)
		}
		engine, err := lifecycle.NewEngine(plan, memory.NewStore(), lifecycle.Options{MaxParallel: 1})
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := engine.Run(context.Background(), "rollback-failure")
		if !errors.Is(err, entryFailure) || !errors.Is(err, rollbackFailure) {
			t.Fatalf("Run error = %v, want both failures", err)
		}
		if snapshot.Status != lifecycle.RunFailed || snapshot.Phases["fail"].Status != lifecycle.PhaseEnterFailed || snapshot.Phases["child"].Status != lifecycle.PhaseRollbackFailed {
			t.Fatalf("snapshot = run:%q fail:%q child:%q", snapshot.Status, snapshot.Phases["fail"].Status, snapshot.Phases["child"].Status)
		}
		if snapshot.Phases["root"].Status != lifecycle.PhaseRolledBack || rootCalls.Load() != 1 {
			t.Fatalf("root cleanup = status:%q calls:%d", snapshot.Phases["root"].Status, rootCalls.Load())
		}
	})
}

func TestRecoveredRollbackRetriesEnteringThenReturnsOriginalFailure(t *testing.T) {
	var retried atomic.Int32
	var rolledBack atomic.Int32
	noop := func(context.Context) error { return nil }
	plan, err := lifecycle.NewPlan([]lifecycle.Phase{
		{Name: "uncertain", Enter: func(context.Context) error { retried.Add(1); return nil }, Exit: noop, Rollback: func(context.Context) error { rolledBack.Add(1); return nil }},
		{Name: "failed", Enter: noop, Exit: noop, Rollback: noop},
	})
	if err != nil {
		t.Fatal(err)
	}
	store := memory.NewStore()
	saveSnapshot(t, store, lifecycle.Snapshot{
		RunID: "recover", PlanDigest: plan.Digest(), Status: lifecycle.RunRollingBack,
		Phases: map[string]lifecycle.PhaseState{
			"uncertain": {Status: lifecycle.PhaseEntering, Attempts: 1},
			"failed":    {Status: lifecycle.PhaseEnterFailed, Attempts: 1, LastError: "original entry failure"},
		},
	})
	engine, err := lifecycle.NewEngine(plan, store, lifecycle.Options{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := engine.Run(context.Background(), "recover")
	if err == nil || err.Error() != "original entry failure" {
		t.Fatalf("Run error = %v, want persisted original failure", err)
	}
	if snapshot.Status != lifecycle.RunRolledBack || snapshot.Phases["uncertain"].Status != lifecycle.PhaseRolledBack {
		t.Fatalf("snapshot = run:%q uncertain:%q", snapshot.Status, snapshot.Phases["uncertain"].Status)
	}
	if retried.Load() != 1 || rolledBack.Load() != 1 {
		t.Fatalf("calls = enter:%d rollback:%d, want 1 each", retried.Load(), rolledBack.Load())
	}
}

func TestRecoveredRollbackPreservesFailureAndFinishesOutstandingAncestors(t *testing.T) {
	var rootRollbacks atomic.Int32
	noop := func(context.Context) error { return nil }
	plan, err := lifecycle.NewPlan([]lifecycle.Phase{
		{Name: "root", Enter: noop, Exit: noop, Rollback: func(context.Context) error { rootRollbacks.Add(1); return nil }},
		{Name: "child", DependsOn: []string{"root"}, Enter: noop, Exit: noop, Rollback: noop},
	})
	if err != nil {
		t.Fatal(err)
	}
	store := memory.NewStore()
	saveSnapshot(t, store, lifecycle.Snapshot{
		RunID: "rollback-restart", PlanDigest: plan.Digest(), Status: lifecycle.RunRollingBack,
		Phases: map[string]lifecycle.PhaseState{
			"root":  {Status: lifecycle.PhaseEntered},
			"child": {Status: lifecycle.PhaseRollbackFailed, LastError: "child rollback failed"},
		},
	})
	engine, err := lifecycle.NewEngine(plan, store, lifecycle.Options{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := engine.Run(context.Background(), "rollback-restart")
	if err == nil || err.Error() != "child rollback failed" {
		t.Fatalf("Run error = %v, want persisted rollback failure", err)
	}
	if snapshot.Status != lifecycle.RunFailed || snapshot.Phases["root"].Status != lifecycle.PhaseRolledBack {
		t.Fatalf("snapshot = run:%q root:%q", snapshot.Status, snapshot.Phases["root"].Status)
	}
	if rootRollbacks.Load() != 1 {
		t.Fatalf("root rollback calls = %d, want 1", rootRollbacks.Load())
	}
}

func TestRecoveredExitPreservesFailureAndFinishesOutstandingCleanup(t *testing.T) {
	var bExits atomic.Int32
	var rootExits atomic.Int32
	noop := func(context.Context) error { return nil }
	plan, err := lifecycle.NewPlan([]lifecycle.Phase{
		{Name: "root", Enter: noop, Exit: func(context.Context) error { rootExits.Add(1); return nil }, Rollback: noop},
		{Name: "a", DependsOn: []string{"root"}, Enter: noop, Exit: noop, Rollback: noop},
		{Name: "b", DependsOn: []string{"root"}, Enter: noop, Exit: func(context.Context) error { bExits.Add(1); return nil }, Rollback: noop},
	})
	if err != nil {
		t.Fatal(err)
	}
	store := memory.NewStore()
	saveSnapshot(t, store, lifecycle.Snapshot{
		RunID: "exit-restart", PlanDigest: plan.Digest(), Status: lifecycle.RunRunning,
		Phases: map[string]lifecycle.PhaseState{
			"root": {Status: lifecycle.PhaseEntered},
			"a":    {Status: lifecycle.PhaseExitFailed, LastError: "a exit failed"},
			"b":    {Status: lifecycle.PhaseExiting},
		},
	})
	engine, err := lifecycle.NewEngine(plan, store, lifecycle.Options{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := engine.Run(context.Background(), "exit-restart")
	if err == nil || err.Error() != "a exit failed" {
		t.Fatalf("Run error = %v, want persisted exit failure", err)
	}
	if snapshot.Status != lifecycle.RunFailed || snapshot.Phases["b"].Status != lifecycle.PhaseExited || snapshot.Phases["root"].Status != lifecycle.PhaseExited {
		t.Fatalf("snapshot = run:%q b:%q root:%q", snapshot.Status, snapshot.Phases["b"].Status, snapshot.Phases["root"].Status)
	}
	if bExits.Load() != 1 || rootExits.Load() != 1 {
		t.Fatalf("exit calls = b:%d root:%d, want 1 each", bExits.Load(), rootExits.Load())
	}
}

func TestEngineValidatesLoadedSnapshots(t *testing.T) {
	noop := func(context.Context) error { return nil }
	plan := singlePhasePlan(t, "phase", noop, noop, noop)
	valid := lifecycle.Snapshot{
		RunID: "run", PlanDigest: plan.Digest(), Status: lifecycle.RunRunning,
		Phases: map[string]lifecycle.PhaseState{"phase": {Status: lifecycle.PhaseEntering, Attempts: 1}},
	}
	tests := []struct {
		name     string
		mutate   func(*lifecycle.Snapshot)
		alsoWant error
	}{
		{name: "wrong run ID", mutate: func(snapshot *lifecycle.Snapshot) { snapshot.RunID = "other" }},
		{name: "changed plan", mutate: func(snapshot *lifecycle.Snapshot) { snapshot.PlanDigest = "other" }, alsoWant: lifecycle.ErrPlanChanged},
		{name: "missing phase", mutate: func(snapshot *lifecycle.Snapshot) { delete(snapshot.Phases, "phase") }},
		{name: "extra phase", mutate: func(snapshot *lifecycle.Snapshot) {
			snapshot.Phases["extra"] = lifecycle.PhaseState{Status: lifecycle.PhasePending}
		}},
		{name: "unknown phase status", mutate: func(snapshot *lifecycle.Snapshot) { snapshot.Phases["phase"] = lifecycle.PhaseState{Status: "mystery"} }},
		{name: "unknown run status", mutate: func(snapshot *lifecycle.Snapshot) { snapshot.Status = "mystery" }},
		{name: "successful run with unexited phase", mutate: func(snapshot *lifecycle.Snapshot) { snapshot.Status = lifecycle.RunSucceeded }},
		{name: "entry failure without rollback status", mutate: func(snapshot *lifecycle.Snapshot) {
			snapshot.Phases["phase"] = lifecycle.PhaseState{Status: lifecycle.PhaseEnterFailed, LastError: "failed"}
		}},
		{name: "exit failure marked succeeded", mutate: func(snapshot *lifecycle.Snapshot) {
			snapshot.Status = lifecycle.RunSucceeded
			snapshot.Phases["phase"] = lifecycle.PhaseState{Status: lifecycle.PhaseExitFailed, LastError: "failed"}
		}},
		{name: "unresolved terminal rollback", mutate: func(snapshot *lifecycle.Snapshot) {
			snapshot.Status = lifecycle.RunRolledBack
			snapshot.Phases["phase"] = lifecycle.PhaseState{Status: lifecycle.PhaseRollingBack}
		}},
		{name: "failed run with outstanding exit", mutate: func(snapshot *lifecycle.Snapshot) {
			snapshot.Status = lifecycle.RunFailed
			snapshot.Phases["phase"] = lifecycle.PhaseState{Status: lifecycle.PhaseExiting}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := cloneSnapshot(valid)
			test.mutate(&snapshot)
			engine, err := lifecycle.NewEngine(plan, staticStore{snapshot: snapshot}, lifecycle.Options{})
			if err != nil {
				t.Fatal(err)
			}
			_, err = engine.Run(context.Background(), "run")
			if !errors.Is(err, lifecycle.ErrInvalidSnapshot) {
				t.Fatalf("Run error = %v, want %v", err, lifecycle.ErrInvalidSnapshot)
			}
			if test.alsoWant != nil && !errors.Is(err, test.alsoWant) {
				t.Fatalf("Run error = %v, also want %v", err, test.alsoWant)
			}
		})
	}
}

func TestEngineAcceptsEveryPersistedCrashBoundary(t *testing.T) {
	noop := func(context.Context) error { return nil }
	plan := singlePhasePlan(t, "phase", noop, noop, noop)
	tests := []struct {
		name     string
		run      lifecycle.RunStatus
		phase    lifecycle.PhaseStatus
		lastErr  string
		terminal bool
	}{
		{name: "pending", run: lifecycle.RunPending, phase: lifecycle.PhasePending},
		{name: "entering", run: lifecycle.RunRunning, phase: lifecycle.PhaseEntering},
		{name: "entered", run: lifecycle.RunRunning, phase: lifecycle.PhaseEntered},
		{name: "exiting", run: lifecycle.RunRunning, phase: lifecycle.PhaseExiting},
		{name: "exited before final save", run: lifecycle.RunRunning, phase: lifecycle.PhaseExited},
		{name: "exit failed before final save", run: lifecycle.RunRunning, phase: lifecycle.PhaseExitFailed, lastErr: "exit failed"},
		{name: "entering during rollback", run: lifecycle.RunRollingBack, phase: lifecycle.PhaseEntering},
		{name: "entered during rollback", run: lifecycle.RunRollingBack, phase: lifecycle.PhaseEntered},
		{name: "enter failed", run: lifecycle.RunRollingBack, phase: lifecycle.PhaseEnterFailed, lastErr: "enter failed"},
		{name: "rolling back", run: lifecycle.RunRollingBack, phase: lifecycle.PhaseRollingBack},
		{name: "rolled back before final save", run: lifecycle.RunRollingBack, phase: lifecycle.PhaseRolledBack},
		{name: "rollback failed before final save", run: lifecycle.RunRollingBack, phase: lifecycle.PhaseRollbackFailed, lastErr: "rollback failed"},
		{name: "succeeded", run: lifecycle.RunSucceeded, phase: lifecycle.PhaseExited, terminal: true},
		{name: "rolled back", run: lifecycle.RunRolledBack, phase: lifecycle.PhaseEnterFailed, lastErr: "enter failed", terminal: true},
		{name: "exit path failed", run: lifecycle.RunFailed, phase: lifecycle.PhaseExitFailed, lastErr: "exit failed", terminal: true},
		{name: "rollback path failed", run: lifecycle.RunFailed, phase: lifecycle.PhaseRollbackFailed, lastErr: "rollback failed", terminal: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := memory.NewStore()
			saveSnapshot(t, store, lifecycle.Snapshot{
				RunID: "boundary", PlanDigest: plan.Digest(), Status: test.run,
				Phases: map[string]lifecycle.PhaseState{"phase": {Status: test.phase, LastError: test.lastErr}},
			})
			engine, err := lifecycle.NewEngine(plan, store, lifecycle.Options{})
			if err != nil {
				t.Fatal(err)
			}
			_, err = engine.Run(context.Background(), "boundary")
			if errors.Is(err, lifecycle.ErrInvalidSnapshot) {
				t.Fatalf("valid crash boundary rejected: %v", err)
			}
			if test.terminal && !errors.Is(err, lifecycle.ErrRunTerminal) {
				t.Fatalf("terminal Run error = %v, want %v", err, lifecycle.ErrRunTerminal)
			}
		})
	}
}

func TestEngineRejectsTypedNilSession(t *testing.T) {
	noop := func(context.Context) error { return nil }
	plan := singlePhasePlan(t, "phase", noop, noop, noop)
	engine, err := lifecycle.NewEngine(plan, typedNilSessionStore{}, lifecycle.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Run(context.Background(), "run"); !errors.Is(err, lifecycle.ErrInvalidSnapshot) {
		t.Fatalf("Run error = %v, want %v", err, lifecycle.ErrInvalidSnapshot)
	}
}

type staticStore struct {
	snapshot lifecycle.Snapshot
}

func (store staticStore) Acquire(context.Context, string) (lifecycle.Session, error) {
	return &staticSession{snapshot: cloneSnapshot(store.snapshot)}, nil
}

type staticSession struct {
	snapshot lifecycle.Snapshot
}

func (session *staticSession) RunID() string { return "run" }

func (session *staticSession) Load(context.Context) (lifecycle.Snapshot, error) {
	return cloneSnapshot(session.snapshot), nil
}

func (session *staticSession) Save(_ context.Context, snapshot lifecycle.Snapshot) error {
	session.snapshot = cloneSnapshot(snapshot)
	return nil
}

func (session *staticSession) Release(context.Context) error { return nil }

type typedNilSessionStore struct{}

func (typedNilSessionStore) Acquire(context.Context, string) (lifecycle.Session, error) {
	var session *staticSession
	return session, nil
}
