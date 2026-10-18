package lifecycle

import (
	"context"
	"fmt"
	"slices"
)

// Action is one idempotent phase callback. It may be retried after a crash
// and must honour cancellation of ctx.
type Action func(context.Context) error

// Phase is a named node of a Plan with its dependencies and the three
// actions that enter, exit and roll it back.
type Phase struct {
	Name      string
	DependsOn []string
	Enter     Action
	Exit      Action
	Rollback  Action
}

// Plan is a validated, immutable lifecycle graph with callbacks. It is safe
// for concurrent use.
type Plan struct {
	phases   map[string]Phase
	levels   [][]string
	digest   string
	topology *Topology
}

// NewPlan validates phases and returns a Plan. It rejects empty names, nil
// actions, duplicate phases, unknown, duplicate or self dependencies, and
// cycles. The plan keeps its own copies of the dependency slices.
func NewPlan(phases []Phase) (*Plan, error) {
	canonical := make(map[string]Phase, len(phases))
	specs := make([]PhaseSpec, 0, len(phases))
	for _, phase := range phases {
		if phase.Name == "" || phase.Enter == nil || phase.Exit == nil || phase.Rollback == nil {
			return nil, fmt.Errorf("%w: %q", ErrInvalidPhase, phase.Name)
		}
		if _, exists := canonical[phase.Name]; exists {
			return nil, fmt.Errorf("%w: %q", ErrDuplicatePhase, phase.Name)
		}

		phase.DependsOn = slices.Clone(phase.DependsOn)
		canonical[phase.Name] = phase
		specs = append(specs, PhaseSpec{Name: phase.Name, DependsOn: phase.DependsOn})
	}

	topology, err := NewTopology(specs)
	if err != nil {
		return nil, err
	}
	for name, phase := range canonical {
		phase.DependsOn = slices.Clone(topology.phases[name].DependsOn)
		canonical[name] = phase
	}
	return &Plan{
		phases:   canonical,
		levels:   topology.levels,
		digest:   topology.digest,
		topology: topology,
	}, nil
}

// Digest returns the SHA-256 topology digest of the plan's phase names and
// dependencies. Actions are not part of the digest.
func (p *Plan) Digest() string {
	return p.digest
}

// Levels returns fresh copies of the phases grouped by topological level,
// sorted lexically within each level.
func (p *Plan) Levels() [][]Phase {
	levels := make([][]Phase, len(p.levels))
	for levelIndex, names := range p.levels {
		levels[levelIndex] = make([]Phase, len(names))
		for phaseIndex, name := range names {
			phase := p.phases[name]
			phase.DependsOn = slices.Clone(phase.DependsOn)
			levels[levelIndex][phaseIndex] = phase
		}
	}
	return levels
}
