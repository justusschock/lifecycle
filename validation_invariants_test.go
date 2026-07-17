package lifecycle

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestSnapshotRejectsImpossiblePhaseStateCountersAndErrors(t *testing.T) {
	plan := validationSinglePhasePlan(t)
	tests := []struct {
		name  string
		run   RunStatus
		state PhaseState
	}{
		{name: "pending attempt", run: RunPending, state: PhaseState{Status: PhasePending, Attempts: 1}},
		{name: "pending error", run: RunPending, state: PhaseState{Status: PhasePending, LastError: "stale"}},
		{name: "entering without attempt", run: RunRunning, state: PhaseState{Status: PhaseEntering}},
		{name: "entered without attempt", run: RunRunning, state: PhaseState{Status: PhaseEntered}},
		{name: "entry failure without attempt", run: RunRollingBack, state: PhaseState{Status: PhaseEnterFailed, LastError: "failed"}},
		{name: "entering with stale error", run: RunRunning, state: PhaseState{Status: PhaseEntering, Attempts: 1, LastError: "stale"}},
		{name: "entered with stale error", run: RunRunning, state: PhaseState{Status: PhaseEntered, Attempts: 1, LastError: "stale"}},
		{name: "exiting after one attempt", run: RunRunning, state: PhaseState{Status: PhaseExiting, Attempts: 1}},
		{name: "exited after one attempt", run: RunRunning, state: PhaseState{Status: PhaseExited, Attempts: 1}},
		{name: "exit failure after one attempt", run: RunRunning, state: PhaseState{Status: PhaseExitFailed, Attempts: 1, LastError: "failed"}},
		{name: "exiting with stale error", run: RunRunning, state: PhaseState{Status: PhaseExiting, Attempts: 2, LastError: "stale"}},
		{name: "exited with stale error", run: RunRunning, state: PhaseState{Status: PhaseExited, Attempts: 2, LastError: "stale"}},
		{name: "rolling back after one attempt", run: RunRollingBack, state: PhaseState{Status: PhaseRollingBack, Attempts: 1}},
		{name: "rolled back after one attempt", run: RunRollingBack, state: PhaseState{Status: PhaseRolledBack, Attempts: 1}},
		{name: "rollback failure after one attempt", run: RunRollingBack, state: PhaseState{Status: PhaseRollbackFailed, Attempts: 1, LastError: "failed"}},
		{name: "rolling back with stale error", run: RunRollingBack, state: PhaseState{Status: PhaseRollingBack, Attempts: 2, LastError: "stale"}},
		{name: "rolled back with stale error", run: RunRollingBack, state: PhaseState{Status: PhaseRolledBack, Attempts: 2, LastError: "stale"}},
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

func TestSnapshotAcceptsNaturalPhaseStateCrashBoundaries(t *testing.T) {
	plan := validationSinglePhasePlan(t)
	tests := []struct {
		name  string
		run   RunStatus
		state PhaseState
	}{
		{name: "pending", run: RunPending, state: PhaseState{Status: PhasePending}},
		{name: "entering", run: RunRunning, state: PhaseState{Status: PhaseEntering, Attempts: 1}},
		{name: "entered", run: RunRunning, state: PhaseState{Status: PhaseEntered, Attempts: 1}},
		{name: "exiting", run: RunRunning, state: PhaseState{Status: PhaseExiting, Attempts: 2}},
		{name: "exited before terminal save", run: RunRunning, state: PhaseState{Status: PhaseExited, Attempts: 2}},
		{name: "exit failure before terminal save", run: RunRunning, state: PhaseState{Status: PhaseExitFailed, Attempts: 2}},
		{name: "entering during rollback", run: RunRollingBack, state: PhaseState{Status: PhaseEntering, Attempts: 1}},
		{name: "entered during rollback", run: RunRollingBack, state: PhaseState{Status: PhaseEntered, Attempts: 1}},
		{name: "entry failure", run: RunRollingBack, state: PhaseState{Status: PhaseEnterFailed, Attempts: 1}},
		{name: "rolling back", run: RunRollingBack, state: PhaseState{Status: PhaseRollingBack, Attempts: 2}},
		{name: "rolled back before terminal save", run: RunRollingBack, state: PhaseState{Status: PhaseRolledBack, Attempts: 2}},
		{name: "rollback failure before terminal save", run: RunRollingBack, state: PhaseState{Status: PhaseRollbackFailed, Attempts: 2}},
		{name: "succeeded", run: RunSucceeded, state: PhaseState{Status: PhaseExited, Attempts: 2}},
		{name: "rolled back", run: RunRolledBack, state: PhaseState{Status: PhaseRolledBack, Attempts: 2}},
		{name: "exit path failed", run: RunFailed, state: PhaseState{Status: PhaseExitFailed, Attempts: 2}},
		{name: "rollback path failed", run: RunFailed, state: PhaseState{Status: PhaseRollbackFailed, Attempts: 2}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := Snapshot{
				RunID: "run", PlanDigest: plan.Digest(), Status: test.run,
				Phases: map[string]PhaseState{"phase": test.state},
			}
			if err := validateSnapshot(plan, "run", snapshot); err != nil {
				t.Fatalf("validateSnapshot() = %v, want nil", err)
			}
		})
	}
}

func TestSnapshotRejectsImpossibleDependencyAndCleanupOrdering(t *testing.T) {
	plan := validationParentChildPlan(t)
	tests := []struct {
		name   string
		run    RunStatus
		parent PhaseState
		child  PhaseState
	}{
		{
			name: "entering child without entered dependency", run: RunRunning,
			parent: PhaseState{Status: PhaseEntering, Attempts: 1},
			child:  PhaseState{Status: PhaseEntering, Attempts: 1},
		},
		{
			name: "entered child with pending dependency", run: RunRunning,
			parent: PhaseState{Status: PhasePending},
			child:  PhaseState{Status: PhaseEntered, Attempts: 1},
		},
		{
			name: "failed child with failed dependency", run: RunRollingBack,
			parent: PhaseState{Status: PhaseEnterFailed, Attempts: 1},
			child:  PhaseState{Status: PhaseEnterFailed, Attempts: 1},
		},
		{
			name: "exiting dependency with entered child", run: RunRunning,
			parent: PhaseState{Status: PhaseExiting, Attempts: 2},
			child:  PhaseState{Status: PhaseEntered, Attempts: 1},
		},
		{
			name: "exited dependency with exiting child", run: RunRunning,
			parent: PhaseState{Status: PhaseExited, Attempts: 2},
			child:  PhaseState{Status: PhaseExiting, Attempts: 2},
		},
		{
			name: "exit-failed dependency with entered child", run: RunRunning,
			parent: PhaseState{Status: PhaseExitFailed, Attempts: 2},
			child:  PhaseState{Status: PhaseEntered, Attempts: 1},
		},
		{
			name: "rolling-back dependency with entered child", run: RunRollingBack,
			parent: PhaseState{Status: PhaseRollingBack, Attempts: 2},
			child:  PhaseState{Status: PhaseEntered, Attempts: 1},
		},
		{
			name: "rolled-back dependency with rolling-back child", run: RunRollingBack,
			parent: PhaseState{Status: PhaseRolledBack, Attempts: 2},
			child:  PhaseState{Status: PhaseRollingBack, Attempts: 2},
		},
		{
			name: "rollback-failed dependency with entered child", run: RunRollingBack,
			parent: PhaseState{Status: PhaseRollbackFailed, Attempts: 2},
			child:  PhaseState{Status: PhaseEntered, Attempts: 1},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := Snapshot{
				RunID: "run", PlanDigest: plan.Digest(), Status: test.run,
				Phases: map[string]PhaseState{"parent": test.parent, "child": test.child},
			}
			if err := validateSnapshot(plan, "run", snapshot); !errors.Is(err, ErrInvalidSnapshot) || !strings.Contains(err.Error(), "dependency ordering") {
				t.Fatalf("validateSnapshot() = %v, want %v caused by dependency ordering", err, ErrInvalidSnapshot)
			}
		})
	}
}

func TestSnapshotAcceptsDependencyOrderedCrashBoundaries(t *testing.T) {
	plan := validationParentChildPlan(t)
	tests := []struct {
		name   string
		run    RunStatus
		parent PhaseState
		child  PhaseState
	}{
		{
			name: "child entering", run: RunRunning,
			parent: PhaseState{Status: PhaseEntered, Attempts: 1},
			child:  PhaseState{Status: PhaseEntering, Attempts: 1},
		},
		{
			name: "child exiting", run: RunRunning,
			parent: PhaseState{Status: PhaseEntered, Attempts: 1},
			child:  PhaseState{Status: PhaseExiting, Attempts: 2},
		},
		{
			name: "parent exiting after child", run: RunRunning,
			parent: PhaseState{Status: PhaseExiting, Attempts: 2},
			child:  PhaseState{Status: PhaseExited, Attempts: 2},
		},
		{
			name: "child rolling back", run: RunRollingBack,
			parent: PhaseState{Status: PhaseEntered, Attempts: 1},
			child:  PhaseState{Status: PhaseRollingBack, Attempts: 2},
		},
		{
			name: "parent rolling back after child", run: RunRollingBack,
			parent: PhaseState{Status: PhaseRollingBack, Attempts: 2},
			child:  PhaseState{Status: PhaseRolledBack, Attempts: 2},
		},
		{
			name: "parent rolled back after child entry failure", run: RunRollingBack,
			parent: PhaseState{Status: PhaseRolledBack, Attempts: 2},
			child:  PhaseState{Status: PhaseEnterFailed, Attempts: 1},
		},
		{
			name: "parent rolls back while child was never admitted", run: RunRollingBack,
			parent: PhaseState{Status: PhaseRollingBack, Attempts: 2},
			child:  PhaseState{Status: PhasePending},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := Snapshot{
				RunID: "run", PlanDigest: plan.Digest(), Status: test.run,
				Phases: map[string]PhaseState{"parent": test.parent, "child": test.child},
			}
			if err := validateSnapshot(plan, "run", snapshot); err != nil {
				t.Fatalf("validateSnapshot() = %v, want nil", err)
			}
		})
	}
}

func TestSnapshotRejectsRollbackCleanupWhileEntryRemainsInProgress(t *testing.T) {
	noop := func(context.Context) error { return nil }
	plan, err := NewPlan([]Phase{
		{Name: "entering", Enter: noop, Exit: noop, Rollback: noop},
		{Name: "cleaned", Enter: noop, Exit: noop, Rollback: noop},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := Snapshot{
		RunID: "run", PlanDigest: plan.Digest(), Status: RunRollingBack,
		Phases: map[string]PhaseState{
			"entering": {Status: PhaseEntering, Attempts: 1},
			"cleaned":  {Status: PhaseRolledBack, Attempts: 2},
		},
	}
	if err := validateSnapshot(plan, "run", snapshot); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("validateSnapshot() = %v, want %v", err, ErrInvalidSnapshot)
	}
}

func validationSinglePhasePlan(t *testing.T) *Plan {
	t.Helper()
	noop := func(context.Context) error { return nil }
	plan, err := NewPlan([]Phase{{Name: "phase", Enter: noop, Exit: noop, Rollback: noop}})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func validationParentChildPlan(t *testing.T) *Plan {
	t.Helper()
	noop := func(context.Context) error { return nil }
	plan, err := NewPlan([]Phase{
		{Name: "parent", Enter: noop, Exit: noop, Rollback: noop},
		{Name: "child", DependsOn: []string{"parent"}, Enter: noop, Exit: noop, Rollback: noop},
	})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}
