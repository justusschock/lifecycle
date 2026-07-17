package lifecycle

import (
	"context"
	"errors"
	"sync"
	"time"
)

type runner struct {
	store    Store
	clock    func() time.Time
	snapshot Snapshot
	mu       sync.Mutex
}

func newRunner(store Store, clock func() time.Time, snapshot Snapshot) *runner {
	return &runner{store: store, clock: clock, snapshot: cloneSnapshot(snapshot)}
}

func (r *runner) current() Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return cloneSnapshot(r.snapshot)
}

func (r *runner) phaseStatus(name string) PhaseStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.snapshot.Phases[name].Status
}

func (r *runner) setRunStatus(ctx context.Context, status RunStatus) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.snapshot.Status = status
	r.snapshot.UpdatedAt = r.clock()
	return r.store.Save(ctx, cloneSnapshot(r.snapshot))
}

func (r *runner) runAction(persistCtx, actionCtx context.Context, phase Phase, action Action, inProgress, completed PhaseStatus, failedRunStatus RunStatus) error {
	if err := r.transition(persistCtx, phase.Name, inProgress, "", true, ""); err != nil {
		return err
	}

	actionErr := action(actionCtx)
	terminal := completed
	lastError := ""
	if actionErr != nil {
		terminal = PhaseFailed
		lastError = actionErr.Error()
	}
	runStatus := RunStatus("")
	if actionErr != nil {
		runStatus = failedRunStatus
	}
	saveErr := r.transition(persistCtx, phase.Name, terminal, lastError, false, runStatus)
	return errors.Join(actionErr, saveErr)
}

func (r *runner) transition(ctx context.Context, name string, status PhaseStatus, lastError string, increment bool, runStatus RunStatus) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.snapshot.Phases[name]
	state.Status = status
	state.LastError = lastError
	state.UpdatedAt = r.clock()
	if increment {
		state.Attempts++
	}
	r.snapshot.Phases[name] = state
	if runStatus != "" {
		r.snapshot.Status = runStatus
	}
	r.snapshot.UpdatedAt = state.UpdatedAt
	return r.store.Save(ctx, cloneSnapshot(r.snapshot))
}

func cloneSnapshot(snapshot Snapshot) Snapshot {
	copy := snapshot
	copy.Phases = make(map[string]PhaseState, len(snapshot.Phases))
	for name, state := range snapshot.Phases {
		copy.Phases[name] = state
	}
	return copy
}
