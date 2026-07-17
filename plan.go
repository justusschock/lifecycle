package lifecycle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	phases map[string]Phase
	levels [][]string
	digest string
}

func NewPlan(phases []Phase) (*Plan, error) {
	canonical := make(map[string]Phase, len(phases))
	for _, phase := range phases {
		if phase.Name == "" || phase.Enter == nil || phase.Exit == nil || phase.Rollback == nil {
			return nil, fmt.Errorf("%w: %q", ErrInvalidPhase, phase.Name)
		}
		if _, exists := canonical[phase.Name]; exists {
			return nil, fmt.Errorf("%w: %q", ErrDuplicatePhase, phase.Name)
		}

		dependencies := slices.Clone(phase.DependsOn)
		slices.Sort(dependencies)
		for index, dependency := range dependencies {
			if dependency == phase.Name {
				return nil, fmt.Errorf("%w: %q depends on itself", ErrCycle, phase.Name)
			}
			if index > 0 && dependencies[index-1] == dependency {
				return nil, fmt.Errorf("%w: %q repeats dependency %q", ErrInvalidPhase, phase.Name, dependency)
			}
		}
		phase.DependsOn = dependencies
		canonical[phase.Name] = phase
	}

	for name, phase := range canonical {
		for _, dependency := range phase.DependsOn {
			if _, exists := canonical[dependency]; !exists {
				return nil, fmt.Errorf("%w: phase %q depends on %q", ErrUnknownDependency, name, dependency)
			}
		}
	}

	levels, err := buildLevels(canonical)
	if err != nil {
		return nil, err
	}
	return &Plan{
		phases: canonical,
		levels: levels,
		digest: digestPlan(canonical),
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

func buildLevels(phases map[string]Phase) ([][]string, error) {
	indegree := make(map[string]int, len(phases))
	dependents := make(map[string][]string, len(phases))
	ready := make([]string, 0, len(phases))
	for name, phase := range phases {
		indegree[name] = len(phase.DependsOn)
		if len(phase.DependsOn) == 0 {
			ready = append(ready, name)
		}
		for _, dependency := range phase.DependsOn {
			dependents[dependency] = append(dependents[dependency], name)
		}
	}
	slices.Sort(ready)

	levels := make([][]string, 0)
	visited := 0
	for len(ready) > 0 {
		level := slices.Clone(ready)
		levels = append(levels, level)
		visited += len(level)
		next := make([]string, 0)
		for _, name := range level {
			for _, dependent := range dependents[name] {
				indegree[dependent]--
				if indegree[dependent] == 0 {
					next = append(next, dependent)
				}
			}
		}
		slices.Sort(next)
		ready = next
	}
	if visited != len(phases) {
		return nil, ErrCycle
	}
	return levels, nil
}

func digestPlan(phases map[string]Phase) string {
	names := make([]string, 0, len(phases))
	for name := range phases {
		names = append(names, name)
	}
	slices.Sort(names)

	hash := sha256.New()
	fmt.Fprintf(hash, "%d:", len(names))
	for _, name := range names {
		writeDigestField(hash, name)
		dependencies := phases[name].DependsOn
		fmt.Fprintf(hash, "%d:", len(dependencies))
		for _, dependency := range dependencies {
			writeDigestField(hash, dependency)
		}
	}
	return hex.EncodeToString(hash.Sum(nil))
}

type digestWriter interface {
	Write([]byte) (int, error)
}

func writeDigestField(writer digestWriter, value string) {
	fmt.Fprintf(writer, "%d:%s", len(value), value)
}
