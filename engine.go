package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"time"

	concurrency "github.com/justusschock/go-concurrency"
)

type Engine struct {
	plan    *Plan
	store   Store
	options Options
}

func NewEngine(plan *Plan, store Store, options Options) (*Engine, error) {
	if plan == nil {
		return nil, fmt.Errorf("%w: nil plan", ErrInvalidPhase)
	}
	if store == nil {
		return nil, fmt.Errorf("%w: nil store", ErrInvalidPhase)
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
	return &Engine{plan: plan, store: store, options: options}, nil
}

func (e *Engine) Run(ctx context.Context, runID string) (Snapshot, error) {
	if runID == "" {
		return Snapshot{}, fmt.Errorf("%w: empty run ID", ErrInvalidPhase)
	}

	snapshot, err := e.store.Load(ctx, runID)
	if err != nil {
		if !errors.Is(err, ErrSnapshotNotFound) {
			return Snapshot{}, err
		}
		snapshot = e.newSnapshot(runID)
	} else {
		if snapshot.PlanDigest != e.plan.Digest() {
			return snapshot, ErrPlanChanged
		}
		if isTerminal(snapshot.Status) {
			return snapshot, ErrRunTerminal
		}
		e.ensurePhases(&snapshot)
	}

	run := newRunner(e.store, e.options.Clock, snapshot)
	if snapshot.Status == RunRollingBack {
		rollbackErr := e.rollback(ctx, run)
		return e.finishRollback(ctx, run, rollbackErr)
	}
	run.snapshot.Status = RunRunning

	if failure := existingFailure(snapshot); failure != nil {
		if statusErr := run.setRunStatus(ctx, RunRollingBack); statusErr != nil {
			return run.current(), errors.Join(failure, statusErr)
		}
		rollbackErr := e.rollback(ctx, run)
		result, finishErr := e.finishRollback(ctx, run, rollbackErr)
		return result, errors.Join(failure, finishErr)
	}

	if err := e.enter(ctx, run); err != nil {
		if statusErr := run.setRunStatus(ctx, RunRollingBack); statusErr != nil {
			return run.current(), errors.Join(err, statusErr)
		}
		rollbackErr := e.rollback(ctx, run)
		result, finishErr := e.finishRollback(ctx, run, rollbackErr)
		return result, errors.Join(err, finishErr)
	}
	if err := e.exit(ctx, run); err != nil {
		statusErr := run.setRunStatus(ctx, RunFailed)
		return run.current(), errors.Join(err, statusErr)
	}
	if err := run.setRunStatus(ctx, RunSucceeded); err != nil {
		return run.current(), err
	}
	return run.current(), nil
}

func (e *Engine) newSnapshot(runID string) Snapshot {
	phases := make(map[string]PhaseState)
	for _, level := range e.plan.Levels() {
		for _, phase := range level {
			phases[phase.Name] = PhaseState{Status: PhasePending}
		}
	}
	return Snapshot{
		RunID:      runID,
		PlanDigest: e.plan.Digest(),
		Status:     RunPending,
		Phases:     phases,
		UpdatedAt:  e.options.Clock(),
	}
}

func (e *Engine) ensurePhases(snapshot *Snapshot) {
	if snapshot.Phases == nil {
		snapshot.Phases = make(map[string]PhaseState)
	}
	for _, level := range e.plan.Levels() {
		for _, phase := range level {
			if _, exists := snapshot.Phases[phase.Name]; !exists {
				snapshot.Phases[phase.Name] = PhaseState{Status: PhasePending}
			}
		}
	}
}

func (e *Engine) enter(ctx context.Context, run *runner) error {
	for _, level := range e.plan.Levels() {
		phases := selectPhases(level, run, PhasePending, PhaseEntering)
		if err := e.runLevel(ctx, run, phases, func(actionCtx context.Context, phase Phase) error {
			return run.runAction(ctx, actionCtx, phase, phase.Enter, PhaseEntering, PhaseEntered, RunRollingBack)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) exit(ctx context.Context, run *runner) error {
	levels := e.plan.Levels()
	for index := len(levels) - 1; index >= 0; index-- {
		phases := selectPhases(levels[index], run, PhaseEntered, PhaseExiting)
		if err := e.runLevel(ctx, run, phases, func(actionCtx context.Context, phase Phase) error {
			return run.runAction(ctx, actionCtx, phase, phase.Exit, PhaseExiting, PhaseExited, RunFailed)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) rollback(ctx context.Context, run *runner) error {
	levels := e.plan.Levels()
	var rollbackErrors []error
	for index := len(levels) - 1; index >= 0; index-- {
		phases := selectPhases(levels[index], run, PhaseEntered, PhaseRollingBack)
		if err := e.runRollbackLevel(ctx, run, phases); err != nil {
			rollbackErrors = append(rollbackErrors, err)
		}
	}
	return errors.Join(rollbackErrors...)
}

func (e *Engine) runRollbackLevel(ctx context.Context, run *runner, phases []Phase) error {
	if len(phases) == 0 {
		return nil
	}
	results, err := concurrency.ParallelMap(ctx, phases, e.options.MaxParallel, func(actionCtx context.Context, phase Phase) (error, error) {
		return run.runAction(ctx, actionCtx, phase, phase.Rollback, PhaseRollingBack, PhaseRolledBack, RunFailed), nil
	})
	if err != nil {
		return err
	}
	return errors.Join(results...)
}

func (e *Engine) runLevel(ctx context.Context, run *runner, phases []Phase, action func(context.Context, Phase) error) error {
	if len(phases) == 0 {
		return nil
	}
	_, err := concurrency.ParallelMap(ctx, phases, e.options.MaxParallel, func(actionCtx context.Context, phase Phase) (struct{}, error) {
		return struct{}{}, action(actionCtx, phase)
	})
	return err
}

func (e *Engine) finishRollback(ctx context.Context, run *runner, rollbackErr error) (Snapshot, error) {
	status := RunRolledBack
	if rollbackErr != nil {
		status = RunFailed
	}
	statusErr := run.setRunStatus(ctx, status)
	return run.current(), errors.Join(rollbackErr, statusErr)
}

func selectPhases(level []Phase, run *runner, statuses ...PhaseStatus) []Phase {
	selected := make([]Phase, 0, len(level))
	for _, phase := range level {
		status := run.phaseStatus(phase.Name)
		for _, candidate := range statuses {
			if status == candidate {
				selected = append(selected, phase)
				break
			}
		}
	}
	return selected
}

func existingFailure(snapshot Snapshot) error {
	for name, state := range snapshot.Phases {
		if state.Status == PhaseFailed {
			if state.LastError == "" {
				return fmt.Errorf("phase %q failed", name)
			}
			return errors.New(state.LastError)
		}
	}
	return nil
}

func isTerminal(status RunStatus) bool {
	return status == RunSucceeded || status == RunRolledBack || status == RunFailed
}
