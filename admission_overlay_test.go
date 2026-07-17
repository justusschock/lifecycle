package lifecycle

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestAdversarialEntryReadinessAfterDurableRollback(t *testing.T) {
	noop := func(context.Context) error { return nil }
	plan, err := NewPlan([]Phase{
		{Name: "a", Enter: noop, Exit: noop, Rollback: noop},
		{Name: "b", Enter: noop, Exit: noop, Rollback: noop},
		{Name: "child", DependsOn: []string{"a"}, Enter: noop, Exit: noop, Rollback: noop},
	})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(plan, &adversarialStore{}, Options{MaxParallel: 2})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := Snapshot{
		RunID: "run", PlanDigest: plan.Digest(), Status: RunRollingBack,
		Phases: map[string]PhaseState{
			"a":     {Status: PhaseEntered, Attempts: 1},
			"b":     {Status: PhaseEnterFailed, Attempts: 1, LastError: "b failed"},
			"child": {Status: PhasePending},
		},
	}
	ready := engine.readyPhases(snapshot, map[string]struct{}{"b": {}}, engine.dependents(), scheduleEnter)
	for _, phase := range ready {
		if phase.Name == "child" {
			t.Fatalf("child admitted after run durably entered %q", snapshot.Status)
		}
	}
}

func TestAdversarialAtomicEntryAdmissionAfterDurableRollback(t *testing.T) {
	called := false
	noop := func(context.Context) error { return nil }
	plan, err := NewPlan([]Phase{{
		Name: "child",
		Enter: func(context.Context) error {
			called = true
			return nil
		},
		Exit: noop, Rollback: noop,
	}})
	if err != nil {
		t.Fatal(err)
	}
	session := &adversarialSession{}
	run := newRunner(session, time.Now, Snapshot{
		RunID: "run", PlanDigest: plan.Digest(), Status: RunRollingBack,
		Phases: map[string]PhaseState{"child": {Status: PhasePending}},
	})
	phase := plan.phases["child"]
	execution := run.runAction(
		context.Background(), context.Background(), phase, phase.Enter,
		PhaseEntering, PhaseEntered, PhaseEnterFailed, RunRollingBack,
	)
	if execution.actionErr != nil || execution.persistenceErr != nil {
		t.Fatalf("runAction() = %+v, want clean rejected admission", execution)
	}
	if called {
		t.Fatal("entry callback launched after run durably entered rollback")
	}
	if session.saves != 0 {
		t.Fatalf("session saves = %d, want 0", session.saves)
	}
	if got := run.current().Phases["child"].Status; got != PhasePending {
		t.Fatalf("child status = %q, want %q", got, PhasePending)
	}
}

func TestAdversarialSnapshotDependencyConsistency(t *testing.T) {
	noop := func(context.Context) error { return nil }
	plan, err := NewPlan([]Phase{
		{Name: "parent", Enter: noop, Exit: noop, Rollback: noop},
		{Name: "child", DependsOn: []string{"parent"}, Enter: noop, Exit: noop, Rollback: noop},
	})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		status RunStatus
		parent PhaseStatus
		child  PhaseStatus
	}{
		{name: "entered child with pending dependency", status: RunRunning, parent: PhasePending, child: PhaseEntered},
		{name: "exited dependency with unexited child", status: RunRunning, parent: PhaseExited, child: PhaseEntered},
		{name: "rolled back dependency with entered child", status: RunRollingBack, parent: PhaseRolledBack, child: PhaseEntered},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := Snapshot{
				RunID: "run", PlanDigest: plan.Digest(), Status: test.status,
				Phases: map[string]PhaseState{
					"parent": adversarialPhaseState(test.parent),
					"child":  adversarialPhaseState(test.child),
				},
			}
			if err := validateSnapshot(plan, "run", snapshot); !errors.Is(err, ErrInvalidSnapshot) || !strings.Contains(err.Error(), "dependency ordering") {
				t.Fatalf("validateSnapshot() = %v, want %v caused by dependency ordering", err, ErrInvalidSnapshot)
			}
		})
	}
}

func TestAdversarialSnapshotPhaseStateConsistency(t *testing.T) {
	noop := func(context.Context) error { return nil }
	plan, err := NewPlan([]Phase{{Name: "phase", Enter: noop, Exit: noop, Rollback: noop}})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name  string
		run   RunStatus
		state PhaseState
	}{
		{name: "entered without an attempt", run: RunRunning, state: PhaseState{Status: PhaseEntered}},
		{name: "exited after only one attempt", run: RunSucceeded, state: PhaseState{Status: PhaseExited, Attempts: 1}},
		{name: "successful state retaining error", run: RunSucceeded, state: PhaseState{Status: PhaseExited, Attempts: 2, LastError: "stale"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := Snapshot{
				RunID: "run", PlanDigest: plan.Digest(), Status: test.run,
				Phases: map[string]PhaseState{"phase": test.state},
			}
			if err := validateSnapshot(plan, "run", snapshot); !errors.Is(err, ErrInvalidSnapshot) {
				t.Fatalf("validateSnapshot() = %v, want %v", err, ErrInvalidSnapshot)
			}
		})
	}
}

type adversarialStore struct{}

func (*adversarialStore) Acquire(context.Context, string) (Session, error) {
	panic("not used")
}

type adversarialSession struct {
	saves    int
	snapshot Snapshot
}

func (*adversarialSession) RunID() string { return "run" }

func (s *adversarialSession) Load(context.Context) (Snapshot, error) {
	return cloneSnapshot(s.snapshot), nil
}

func (s *adversarialSession) Save(_ context.Context, snapshot Snapshot) error {
	s.saves++
	s.snapshot = cloneSnapshot(snapshot)
	return nil
}

func (*adversarialSession) Release(context.Context) error { return nil }

func adversarialPhaseState(status PhaseStatus) PhaseState {
	state := PhaseState{Status: status}
	switch status {
	case PhaseEntering, PhaseEntered, PhaseEnterFailed:
		state.Attempts = 1
	case PhaseExiting, PhaseExited, PhaseExitFailed, PhaseRollingBack, PhaseRolledBack, PhaseRollbackFailed:
		state.Attempts = 2
	}
	if status == PhaseEnterFailed || status == PhaseExitFailed || status == PhaseRollbackFailed {
		state.LastError = "failed"
	}
	return state
}
