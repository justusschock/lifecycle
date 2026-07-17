package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Coordinator owns one fenced, durable lifecycle run.
type Coordinator struct {
	topology *Topology
	session  Session
	options  Options

	mu         sync.Mutex
	snapshot   Snapshot
	changed    chan struct{}
	active     map[string]*Execution
	liveErrors map[string]error
	fatalErr   error
	closed     bool
}

// OpenCoordinator acquires exclusive ownership of runID and restores its
// durable lifecycle state.
func OpenCoordinator(ctx context.Context, topology *Topology, store Store, runID string, options Options) (*Coordinator, error) {
	if topology == nil || topology.phases == nil || topology.levels == nil || topology.digest == "" {
		return nil, fmt.Errorf("%w: uninitialized topology", ErrInvalidPhase)
	}
	if isNilStore(store) {
		return nil, fmt.Errorf("%w: nil store", ErrInvalidPhase)
	}
	if runID == "" {
		return nil, fmt.Errorf("%w: empty run ID", ErrInvalidPhase)
	}
	if options.MaxParallel < 0 {
		return nil, fmt.Errorf("%w: negative parallelism", ErrInvalidPhase)
	}
	if options.MaxParallel == 0 {
		options.MaxParallel = 1
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}

	session, err := store.Acquire(ctx, runID)
	if err != nil {
		return nil, err
	}
	if isNilSession(session) {
		return nil, fmt.Errorf("%w: store returned a nil session", ErrInvalidSnapshot)
	}
	if session.RunID() != runID {
		return nil, errors.Join(
			fmt.Errorf("%w: store returned session for %q, want %q", ErrInvalidSnapshot, session.RunID(), runID),
			session.Release(context.WithoutCancel(ctx)),
		)
	}

	snapshot, err := session.Load(ctx)
	if err != nil {
		if !errors.Is(err, ErrSnapshotNotFound) {
			return nil, errors.Join(err, session.Release(context.WithoutCancel(ctx)))
		}
		snapshot = newTopologySnapshot(topology, runID, options.Clock())
	} else if validationErr := validateTopologySnapshot(topology, runID, snapshot); validationErr != nil {
		return nil, errors.Join(validationErr, session.Release(context.WithoutCancel(ctx)))
	}

	return &Coordinator{
		topology:   topology,
		session:    session,
		options:    options,
		snapshot:   snapshot,
		changed:    make(chan struct{}),
		active:     make(map[string]*Execution),
		liveErrors: make(map[string]error),
	}, nil
}

// EnterPhase waits until phase dependencies are durably entered and execution
// capacity is available.
func (c *Coordinator) EnterPhase(ctx context.Context, name string) (*Execution, error) {
	if _, exists := c.topology.phases[name]; !exists {
		return nil, fmt.Errorf("%w: %q", ErrInvalidPhase, name)
	}

	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return nil, ErrOwnershipLost
		}
		if c.fatalErr != nil {
			err := c.fatalErr
			c.mu.Unlock()
			return nil, err
		}
		if isTerminal(c.snapshot.Status) {
			c.mu.Unlock()
			return nil, ErrRunTerminal
		}

		state := c.snapshot.Phases[name]
		if hasSuccessfulEntryEvidence(state.Status) || state.Status == PhaseEnterFailed {
			if state.Status == PhaseEnterFailed {
				err := c.phaseErrorLocked(name)
				c.mu.Unlock()
				return nil, err
			}
			c.mu.Unlock()
			return skippedExecution(), nil
		}
		if failure := c.dependencyFailureLocked(name); failure != nil {
			c.mu.Unlock()
			return nil, errors.Join(ErrDependencyFailed, failure)
		}
		if c.snapshot.Status == RunRollingBack && state.Status != PhaseEntering {
			err := c.entryFailureLocked()
			c.mu.Unlock()
			return nil, err
		}

		_, duplicate := c.active[name]
		ready := state.Status == PhaseEntering || (state.Status == PhasePending && c.dependenciesEnteredLocked(name))
		if !duplicate && ready && len(c.active) < c.options.MaxParallel {
			candidate := cloneSnapshot(c.snapshot)
			phaseState := candidate.Phases[name]
			phaseState.Status = PhaseEntering
			phaseState.Attempts++
			phaseState.LastError = ""
			phaseState.UpdatedAt = c.options.Clock()
			candidate.Phases[name] = phaseState
			if candidate.Status == RunPending {
				candidate.Status = RunRunning
			}
			candidate.UpdatedAt = phaseState.UpdatedAt
			if err := c.session.Save(ctx, cloneSnapshot(candidate)); err != nil {
				c.failLocked(err)
				c.mu.Unlock()
				return nil, err
			}
			c.snapshot = candidate
			executionCtx, cancel := context.WithCancelCause(ctx)
			execution := &Execution{
				coordinator: c,
				name:        name,
				shouldRun:   true,
				ctx:         executionCtx,
				cancel:      cancel,
			}
			c.active[name] = execution
			c.signalLocked()
			c.mu.Unlock()
			return execution, nil
		}

		changed := c.changed
		c.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		}
	}
}

// Snapshot returns an isolated copy of the current durable state.
func (c *Coordinator) Snapshot() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return cloneSnapshot(c.snapshot)
}

// Close cancels active executions and releases fenced ownership.
func (c *Coordinator) Close(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	for _, execution := range c.active {
		execution.cancel(ErrOwnershipLost)
	}
	c.signalLocked()
	c.mu.Unlock()
	return c.session.Release(ctx)
}

func (c *Coordinator) completeEntry(ctx context.Context, execution *Execution, actionErr error) (error, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errors.Join(actionErr, ErrOwnershipLost), false
	}
	if c.active[execution.name] != execution {
		return errors.Join(actionErr, ErrInvalidSnapshot), false
	}

	candidate := cloneSnapshot(c.snapshot)
	state := candidate.Phases[execution.name]
	state.UpdatedAt = c.options.Clock()
	state.LastError = ""
	state.Status = PhaseEntered
	if actionErr != nil {
		state.Status = PhaseEnterFailed
		state.LastError = actionErr.Error()
		candidate.Status = RunRollingBack
	}
	candidate.Phases[execution.name] = state
	candidate.UpdatedAt = state.UpdatedAt
	if err := c.session.Save(ctx, cloneSnapshot(candidate)); err != nil {
		c.failLocked(err)
		return errors.Join(actionErr, err), false
	}

	c.snapshot = candidate
	delete(c.active, execution.name)
	execution.cancel(nil)
	if actionErr != nil {
		c.liveErrors[execution.name] = actionErr
		for _, active := range c.active {
			active.cancel(actionErr)
		}
	}
	c.signalLocked()
	return actionErr, true
}

func (c *Coordinator) dependenciesEnteredLocked(name string) bool {
	for _, dependency := range c.topology.phases[name].DependsOn {
		if c.snapshot.Phases[dependency].Status != PhaseEntered {
			return false
		}
	}
	return true
}

func (c *Coordinator) dependencyFailureLocked(name string) error {
	visited := make(map[string]struct{})
	var visit func(string) error
	visit = func(phaseName string) error {
		for _, dependency := range c.topology.phases[phaseName].DependsOn {
			if _, seen := visited[dependency]; seen {
				continue
			}
			visited[dependency] = struct{}{}
			if c.snapshot.Phases[dependency].Status == PhaseEnterFailed {
				return c.phaseErrorLocked(dependency)
			}
			if err := visit(dependency); err != nil {
				return err
			}
		}
		return nil
	}
	return visit(name)
}

func (c *Coordinator) phaseErrorLocked(name string) error {
	if err := c.liveErrors[name]; err != nil {
		return err
	}
	if message := c.snapshot.Phases[name].LastError; message != "" {
		return errors.New(message)
	}
	return fmt.Errorf("%w: phase %q failed entry", ErrInvalidSnapshot, name)
}

func (c *Coordinator) entryFailureLocked() error {
	for _, level := range c.topology.levels {
		for _, name := range level {
			if c.snapshot.Phases[name].Status == PhaseEnterFailed {
				return c.phaseErrorLocked(name)
			}
		}
	}
	return ErrRunTerminal
}

func (c *Coordinator) failLocked(err error) {
	c.fatalErr = err
	for _, execution := range c.active {
		execution.cancel(err)
	}
	c.signalLocked()
}

func (c *Coordinator) signalLocked() {
	close(c.changed)
	c.changed = make(chan struct{})
}

func newTopologySnapshot(topology *Topology, runID string, now time.Time) Snapshot {
	phases := make(map[string]PhaseState, len(topology.phases))
	for name := range topology.phases {
		phases[name] = PhaseState{Status: PhasePending}
	}
	return Snapshot{
		RunID:      runID,
		PlanDigest: topology.Digest(),
		Status:     RunPending,
		Phases:     phases,
		UpdatedAt:  now,
	}
}

func skippedExecution() *Execution {
	return &Execution{ctx: context.Background()}
}
