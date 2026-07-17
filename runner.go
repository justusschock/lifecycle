package lifecycle

import (
	"context"
	"errors"
	"sync"
	"time"
)

type runner struct {
	store    Session
	clock    func() time.Time
	snapshot Snapshot
	mu       sync.Mutex
}

type persistenceError struct {
	err error
}

type actionExecution struct {
	actionErr      error
	persistenceErr error
}

func (e *persistenceError) Error() string {
	return e.err.Error()
}

func (e *persistenceError) Unwrap() error {
	return e.err
}

func isPersistenceError(err error) bool {
	var target *persistenceError
	return errors.As(err, &target)
}

func wrapPersistenceError(err error) error {
	if err == nil || isPersistenceError(err) {
		return err
	}
	return &persistenceError{err: err}
}

func newRunner(store Session, clock func() time.Time, snapshot Snapshot) *runner {
	return &runner{store: store, clock: clock, snapshot: cloneSnapshot(snapshot)}
}

func (r *runner) current() Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return cloneSnapshot(r.snapshot)
}

func (r *runner) setRunStatus(ctx context.Context, status RunStatus) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	candidate := cloneSnapshot(r.snapshot)
	candidate.Status = status
	candidate.UpdatedAt = r.clock()
	if err := r.store.Save(ctx, cloneSnapshot(candidate)); err != nil {
		return wrapPersistenceError(err)
	}
	r.snapshot = candidate
	return nil
}

func (r *runner) runAction(
	persistCtx context.Context,
	actionCtx context.Context,
	phase Phase,
	action Action,
	inProgress PhaseStatus,
	completed PhaseStatus,
	failed PhaseStatus,
	failedRunStatus RunStatus,
) actionExecution {
	admitted, err := r.beginAction(persistCtx, phase.Name, inProgress)
	if err != nil {
		return actionExecution{persistenceErr: err}
	}
	if !admitted {
		return actionExecution{}
	}

	actionErr := action(actionCtx)
	terminal := completed
	lastError := ""
	if actionErr != nil {
		terminal = failed
		lastError = actionErr.Error()
	}
	runStatus := RunStatus("")
	if actionErr != nil {
		runStatus = failedRunStatus
	}
	persistenceErr := r.transition(persistCtx, phase.Name, terminal, lastError, runStatus)
	return actionExecution{actionErr: actionErr, persistenceErr: persistenceErr}
}

func (r *runner) beginAction(ctx context.Context, name string, status PhaseStatus) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.snapshot.Phases[name]
	if !actionAdmissionAllowed(r.snapshot.Status, current.Status, status) {
		return false, nil
	}
	candidate := cloneSnapshot(r.snapshot)
	state := candidate.Phases[name]
	state.Status = status
	state.Attempts++
	state.LastError = ""
	state.UpdatedAt = r.clock()
	candidate.Phases[name] = state
	if candidate.Status == RunPending {
		candidate.Status = RunRunning
	}
	candidate.UpdatedAt = state.UpdatedAt
	if err := r.store.Save(ctx, cloneSnapshot(candidate)); err != nil {
		return false, wrapPersistenceError(err)
	}
	r.snapshot = candidate
	return true, nil
}

func actionAdmissionAllowed(runStatus RunStatus, current, inProgress PhaseStatus) bool {
	switch inProgress {
	case PhaseEntering:
		if current == PhasePending {
			return runStatus == RunPending || runStatus == RunRunning
		}
		return current == PhaseEntering && (runStatus == RunRunning || runStatus == RunRollingBack)
	case PhaseExiting:
		return runStatus == RunRunning && (current == PhaseEntered || current == PhaseExiting)
	case PhaseRollingBack:
		return runStatus == RunRollingBack && (current == PhaseEntered || current == PhaseRollingBack)
	default:
		return false
	}
}

func (r *runner) transition(ctx context.Context, name string, status PhaseStatus, lastError string, runStatus RunStatus) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	candidate := cloneSnapshot(r.snapshot)
	state := candidate.Phases[name]
	state.Status = status
	state.LastError = lastError
	state.UpdatedAt = r.clock()
	candidate.Phases[name] = state
	if candidate.Status == RunPending {
		candidate.Status = RunRunning
	}
	if runStatus != "" {
		candidate.Status = runStatus
	}
	candidate.UpdatedAt = state.UpdatedAt
	if err := r.store.Save(ctx, cloneSnapshot(candidate)); err != nil {
		return wrapPersistenceError(err)
	}
	r.snapshot = candidate
	return nil
}

func cloneSnapshot(snapshot Snapshot) Snapshot {
	copy := snapshot
	copy.Phases = make(map[string]PhaseState, len(snapshot.Phases))
	for name, state := range snapshot.Phases {
		copy.Phases[name] = state
	}
	return copy
}
