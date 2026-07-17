package lifecycle

import (
	"context"
	"fmt"
	"slices"
)

type Action func(context.Context) error

type Phase struct {
	Name      string
	DependsOn []string
	Enter     Action
	Exit      Action
	Rollback  Action
}

type Plan struct {
	phases   map[string]Phase
	levels   [][]string
	digest   string
	topology *Topology
}

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

func (p *Plan) Digest() string {
	return p.digest
}

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
