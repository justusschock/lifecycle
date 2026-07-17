package lifecycle

import (
	"context"
	"errors"
	"testing"
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
			"a":     {Status: PhaseEntered},
			"b":     {Status: PhaseEnterFailed, LastError: "b failed"},
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
					"parent": {Status: test.parent},
					"child":  {Status: test.child},
				},
			}
			if err := validateSnapshot(plan, "run", snapshot); !errors.Is(err, ErrInvalidSnapshot) {
				t.Fatalf("validateSnapshot() = %v, want %v", err, ErrInvalidSnapshot)
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
