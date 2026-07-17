package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"reflect"
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
	if isNilStore(store) {
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

func (e *Engine) Run(ctx context.Context, runID string) (result Snapshot, resultErr error) {
	if runID == "" {
		return Snapshot{}, fmt.Errorf("%w: empty run ID", ErrInvalidPhase)
	}

	session, err := e.store.Acquire(ctx, runID)
	if err != nil {
		return Snapshot{}, err
	}
	if isNilSession(session) {
		return Snapshot{}, fmt.Errorf("%w: store returned a nil session", ErrInvalidSnapshot)
	}
	defer func() {
		releaseErr := session.Release(context.WithoutCancel(ctx))
		resultErr = errors.Join(resultErr, releaseErr)
	}()
	if session.RunID() != runID {
		return Snapshot{}, fmt.Errorf("%w: store returned session for %q, want %q", ErrInvalidSnapshot, session.RunID(), runID)
	}

	snapshot, err := session.Load(ctx)
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

	run := newRunner(session, e.options.Clock, snapshot)
	if snapshot.Status == RunRollingBack {
		rollbackErr := e.rollback(ctx, run)
		if isPersistenceError(rollbackErr) {
			return run.current(), rollbackErr
		}
		return e.finishRollback(ctx, run, rollbackErr)
	}

	if failure := existingFailure(e.plan.Levels(), snapshot); failure != nil {
		if statusErr := run.setRunStatus(ctx, RunRollingBack); statusErr != nil {
			return run.current(), errors.Join(failure, statusErr)
		}
		rollbackErr := e.rollback(ctx, run)
		if isPersistenceError(rollbackErr) {
			return run.current(), errors.Join(failure, rollbackErr)
		}
		result, finishErr := e.finishRollback(ctx, run, rollbackErr)
		return result, errors.Join(failure, finishErr)
	}

	if err := e.enter(ctx, run); err != nil {
		if isPersistenceError(err) {
			return run.current(), err
		}
		if statusErr := run.setRunStatus(ctx, RunRollingBack); statusErr != nil {
			return run.current(), errors.Join(err, statusErr)
		}
		rollbackErr := e.rollback(ctx, run)
		if isPersistenceError(rollbackErr) {
			return run.current(), errors.Join(err, rollbackErr)
		}
		result, finishErr := e.finishRollback(ctx, run, rollbackErr)
		return result, errors.Join(err, finishErr)
	}
	if err := e.exit(ctx, run); err != nil {
		if isPersistenceError(err) {
			return run.current(), err
		}
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
			if isPersistenceError(err) {
				return errors.Join(rollbackErrors...)
			}
		}
	}
	return errors.Join(rollbackErrors...)
}

func (e *Engine) runRollbackLevel(ctx context.Context, run *runner, phases []Phase) error {
	if len(phases) == 0 {
		return nil
	}
	results, err := concurrency.ParallelMap(ctx, phases, e.options.MaxParallel, func(actionCtx context.Context, phase Phase) (error, error) {
		actionErr := run.runAction(ctx, actionCtx, phase, phase.Rollback, PhaseRollingBack, PhaseRolledBack, RunFailed)
		if isPersistenceError(actionErr) {
			return nil, actionErr
		}
		return actionErr, nil
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

func existingFailure(levels [][]Phase, snapshot Snapshot) error {
	var failures []error
	for _, level := range levels {
		for _, phase := range level {
			state := snapshot.Phases[phase.Name]
			if state.Status != PhaseFailed {
				continue
			}
			if state.LastError == "" {
				failures = append(failures, fmt.Errorf("phase %q failed", phase.Name))
			} else {
				failures = append(failures, errors.New(state.LastError))
			}
		}
	}
	return errors.Join(failures...)
}

func isTerminal(status RunStatus) bool {
	return status == RunSucceeded || status == RunRolledBack || status == RunFailed
}

func isNilStore(store Store) bool {
	if store == nil {
		return true
	}
	value := reflect.ValueOf(store)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func isNilSession(session Session) bool {
	if session == nil {
		return true
	}
	value := reflect.ValueOf(session)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
