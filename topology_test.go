package lifecycle

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestTopologyBuildsStableImmutableDAG(t *testing.T) {
	dependencies := []string{"right", "left"}
	topology, err := NewTopology([]PhaseSpec{
		{Name: "join", DependsOn: dependencies},
		{Name: "root"},
		{Name: "right", DependsOn: []string{"root"}},
		{Name: "left", DependsOn: []string{"root"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	dependencies[0] = "mutated"
	if got := specNames(topology.Levels()); !reflect.DeepEqual(got, [][]string{{"root"}, {"left", "right"}, {"join"}}) {
		t.Fatalf("levels = %v", got)
	}

	levels := topology.Levels()
	levels[2][0].DependsOn[0] = "mutated"
	if got := topology.Levels()[2][0].DependsOn; !reflect.DeepEqual(got, []string{"left", "right"}) {
		t.Fatalf("dependencies = %v", got)
	}

	reordered, err := NewTopology([]PhaseSpec{
		{Name: "left", DependsOn: []string{"root"}},
		{Name: "join", DependsOn: []string{"left", "right"}},
		{Name: "root"},
		{Name: "right", DependsOn: []string{"root"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if topology.Digest() != reordered.Digest() {
		t.Fatalf("digests differ: %s %s", topology.Digest(), reordered.Digest())
	}
}

func TestTopologyAndPlanShareDigest(t *testing.T) {
	noop := func(context.Context) error { return nil }
	topology, err := NewTopology([]PhaseSpec{
		{Name: "root"},
		{Name: "child", DependsOn: []string{"root"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := NewPlan([]Phase{
		{Name: "root", Enter: noop, Exit: noop, Rollback: noop},
		{Name: "child", DependsOn: []string{"root"}, Enter: noop, Exit: noop, Rollback: noop},
	})
	if err != nil {
		t.Fatal(err)
	}
	if topology.Digest() != plan.Digest() {
		t.Fatalf("digests differ: %s %s", topology.Digest(), plan.Digest())
	}
}

func TestTopologyRejectsInvalidGraphs(t *testing.T) {
	tests := []struct {
		name   string
		specs  []PhaseSpec
		target error
	}{
		{name: "empty name", specs: []PhaseSpec{{Name: ""}}, target: ErrInvalidPhase},
		{name: "duplicate phase", specs: []PhaseSpec{{Name: "phase"}, {Name: "phase"}}, target: ErrDuplicatePhase},
		{name: "unknown dependency", specs: []PhaseSpec{{Name: "phase", DependsOn: []string{"missing"}}}, target: ErrUnknownDependency},
		{name: "duplicate dependency", specs: []PhaseSpec{{Name: "root"}, {Name: "phase", DependsOn: []string{"root", "root"}}}, target: ErrInvalidPhase},
		{name: "self dependency", specs: []PhaseSpec{{Name: "phase", DependsOn: []string{"phase"}}}, target: ErrCycle},
		{name: "cycle", specs: []PhaseSpec{{Name: "left", DependsOn: []string{"right"}}, {Name: "right", DependsOn: []string{"left"}}}, target: ErrCycle},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewTopology(test.specs); !errors.Is(err, test.target) {
				t.Fatalf("NewTopology error = %v, want %v", err, test.target)
			}
		})
	}
}

func specNames(levels [][]PhaseSpec) [][]string {
	result := make([][]string, len(levels))
	for levelIndex, level := range levels {
		for _, phase := range level {
			result[levelIndex] = append(result[levelIndex], phase.Name)
		}
	}
	return result
}
