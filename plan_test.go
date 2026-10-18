package lifecycle

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

// FR-LIFE-02
func TestPlanBuildsStableBranchAndJoinLevels(t *testing.T) {
	noop := func(context.Context) error { return nil }
	plan, err := NewPlan([]Phase{
		{Name: "join", DependsOn: []string{"right", "left"}, Enter: noop, Exit: noop, Rollback: noop},
		{Name: "root", Enter: noop, Exit: noop, Rollback: noop},
		{Name: "right", DependsOn: []string{"root"}, Enter: noop, Exit: noop, Rollback: noop},
		{Name: "left", DependsOn: []string{"root"}, Enter: noop, Exit: noop, Rollback: noop},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := phaseNames(plan.Levels()), [][]string{{"root"}, {"left", "right"}, {"join"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("levels = %v, want %v", got, want)
	}

	second, err := NewPlan([]Phase{
		{Name: "left", DependsOn: []string{"root"}, Enter: noop, Exit: noop, Rollback: noop},
		{Name: "join", DependsOn: []string{"left", "right"}, Enter: noop, Exit: noop, Rollback: noop},
		{Name: "root", Enter: noop, Exit: noop, Rollback: noop},
		{Name: "right", DependsOn: []string{"root"}, Enter: noop, Exit: noop, Rollback: noop},
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Digest() != second.Digest() {
		t.Fatalf("digests differ: %s %s", plan.Digest(), second.Digest())
	}
}

// FR-LIFE-01
func TestPlanRejectsInvalidGraphs(t *testing.T) {
	noop := func(context.Context) error { return nil }
	tests := []struct {
		name   string
		phases []Phase
		target error
	}{
		{
			name: "cycle",
			phases: []Phase{
				{Name: "a", DependsOn: []string{"b"}, Enter: noop, Exit: noop, Rollback: noop},
				{Name: "b", DependsOn: []string{"a"}, Enter: noop, Exit: noop, Rollback: noop},
			},
			target: ErrCycle,
		},
		{
			name:   "self dependency",
			phases: []Phase{{Name: "a", DependsOn: []string{"a"}, Enter: noop, Exit: noop, Rollback: noop}},
			target: ErrCycle,
		},
		{
			name:   "unknown dependency",
			phases: []Phase{{Name: "a", DependsOn: []string{"missing"}, Enter: noop, Exit: noop, Rollback: noop}},
			target: ErrUnknownDependency,
		},
		{
			name:   "empty name",
			phases: []Phase{{Enter: noop, Exit: noop, Rollback: noop}},
			target: ErrInvalidPhase,
		},
		{
			name: "duplicate name",
			phases: []Phase{
				{Name: "a", Enter: noop, Exit: noop, Rollback: noop},
				{Name: "a", Enter: noop, Exit: noop, Rollback: noop},
			},
			target: ErrDuplicatePhase,
		},
		{
			name:   "duplicate dependency",
			phases: []Phase{{Name: "root", Enter: noop, Exit: noop, Rollback: noop}, {Name: "a", DependsOn: []string{"root", "root"}, Enter: noop, Exit: noop, Rollback: noop}},
			target: ErrInvalidPhase,
		},
		{
			name:   "nil enter",
			phases: []Phase{{Name: "a", Exit: noop, Rollback: noop}},
			target: ErrInvalidPhase,
		},
		{
			name:   "nil exit",
			phases: []Phase{{Name: "a", Enter: noop, Rollback: noop}},
			target: ErrInvalidPhase,
		},
		{
			name:   "nil rollback",
			phases: []Phase{{Name: "a", Enter: noop, Exit: noop}},
			target: ErrInvalidPhase,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewPlan(test.phases); !errors.Is(err, test.target) {
				t.Fatalf("NewPlan error = %v, want %v", err, test.target)
			}
		})
	}
}

// FR-LIFE-02
func TestPlanOwnsItsGraphData(t *testing.T) {
	noop := func(context.Context) error { return nil }
	dependencies := []string{"root"}
	plan, err := NewPlan([]Phase{
		{Name: "root", Enter: noop, Exit: noop, Rollback: noop},
		{Name: "child", DependsOn: dependencies, Enter: noop, Exit: noop, Rollback: noop},
	})
	if err != nil {
		t.Fatal(err)
	}
	digest := plan.Digest()

	dependencies[0] = "changed"
	levels := plan.Levels()
	levels[1][0].Name = "mutated"
	levels[1][0].DependsOn[0] = "mutated"

	if got, want := phaseNames(plan.Levels()), [][]string{{"root"}, {"child"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("levels after external mutation = %v, want %v", got, want)
	}
	if plan.Digest() != digest {
		t.Fatalf("digest changed from %s to %s", digest, plan.Digest())
	}
}

func phaseNames(levels [][]Phase) [][]string {
	result := make([][]string, len(levels))
	for index, level := range levels {
		for _, phase := range level {
			result[index] = append(result[index], phase.Name)
		}
	}
	return result
}
