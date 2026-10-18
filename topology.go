package lifecycle

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
)

type PhaseSpec struct {
	Name      string
	DependsOn []string
}

type Topology struct {
	phases map[string]PhaseSpec
	levels [][]string
	digest string
}

func NewTopology(phases []PhaseSpec) (*Topology, error) {
	canonical := make(map[string]PhaseSpec, len(phases))
	for _, phase := range phases {
		if phase.Name == "" {
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

	levels, err := buildTopologyLevels(canonical)
	if err != nil {
		return nil, err
	}
	return &Topology{
		phases: canonical,
		levels: levels,
		digest: digestTopology(canonical),
	}, nil
}

func (t *Topology) Digest() string {
	return t.digest
}

func (t *Topology) Levels() [][]PhaseSpec {
	levels := make([][]PhaseSpec, len(t.levels))
	for levelIndex, names := range t.levels {
		levels[levelIndex] = make([]PhaseSpec, len(names))
		for phaseIndex, name := range names {
			phase := t.phases[name]
			phase.DependsOn = slices.Clone(phase.DependsOn)
			levels[levelIndex][phaseIndex] = phase
		}
	}
	return levels
}

func buildTopologyLevels(phases map[string]PhaseSpec) ([][]string, error) {
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

func digestTopology(phases map[string]PhaseSpec) string {
	names := make([]string, 0, len(phases))
	for name := range phases {
		names = append(names, name)
	}
	slices.Sort(names)

	hash := sha256.New()
	_, _ = fmt.Fprintf(hash, "%d:", len(names))
	for _, name := range names {
		writeDigestField(hash, name)
		dependencies := phases[name].DependsOn
		_, _ = fmt.Fprintf(hash, "%d:", len(dependencies))
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
	_, _ = fmt.Fprintf(writer, "%d:%s", len(value), value)
}
