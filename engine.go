package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	concurrency "github.com/justusschock/go-concurrency"

	"github.com/justusschock/go-lifecycle/internal/nilcheck"
)

// Engine automatically drives a callback-bearing Plan through a durable
// Coordinator. Coordinator owns all topology readiness and snapshot mutation.
type Engine struct {
	plan    *Plan
	store   Store
	options Options
}

func NewEngine(plan *Plan, store Store, options Options) (*Engine, error) {
	if plan == nil {
		return nil, fmt.Errorf("%w: nil plan", ErrInvalidPhase)
	}
	if plan.phases == nil || plan.levels == nil || plan.digest == "" || plan.topology == nil {
		return nil, fmt.Errorf("%w: uninitialized plan", ErrInvalidPhase)
	}
	if nilcheck.IsNil(store) {
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

// Run executes the complete entry, exit, or rollback lifecycle for runID.
func (e *Engine) Run(ctx context.Context, runID string) (result Snapshot, resultErr error) {
	coordinator, err := OpenCoordinator(ctx, e.plan.topology, e.store, runID, e.options)
	if err != nil {
		return Snapshot{}, err
	}
	defer func() {
		resultErr = errors.Join(resultErr, coordinator.Close(context.WithoutCancel(ctx)))
	}()

	if isTerminal(coordinator.Snapshot().Status) {
		return coordinator.Snapshot(), ErrRunTerminal
	}

	entry := e.drive(ctx, coordinator, stageEnter)
	if interruption := e.stageInterruption(entry); interruption != nil {
		return coordinator.Snapshot(), interruption
	}
	if coordinator.Snapshot().Status == RunRollingBack {
		return e.finishRollback(ctx, coordinator, entry.liveErrors)
	}
	return e.finishExit(ctx, coordinator)
}

func (e *Engine) finishExit(ctx context.Context, coordinator *Coordinator) (Snapshot, error) {
	exit := e.drive(ctx, coordinator, stageExit)
	if interruption := e.stageInterruption(exit); interruption != nil {
		return coordinator.Snapshot(), errors.Join(e.stageErrors(coordinator.Snapshot(), PhaseExitFailed, exit.liveErrors), interruption)
	}
	if err := coordinator.finalize(ctx, stageExit); err != nil {
		return coordinator.Snapshot(), errors.Join(e.stageErrors(coordinator.Snapshot(), PhaseExitFailed, exit.liveErrors), err)
	}
	exitErr := e.stageErrors(coordinator.Snapshot(), PhaseExitFailed, exit.liveErrors)
	return coordinator.Snapshot(), errors.Join(exitErr, e.stageInterruption(exit))
}

func (e *Engine) finishRollback(ctx context.Context, coordinator *Coordinator, liveEntryErrors map[string]error) (Snapshot, error) {
	retried := e.drive(ctx, coordinator, stageEnter)
	liveEntryErrors = mergeErrorMaps(liveEntryErrors, retried.liveErrors)
	entryErr := e.stageErrors(coordinator.Snapshot(), PhaseEnterFailed, liveEntryErrors)
	if interruption := e.stageInterruption(retried); interruption != nil {
		return coordinator.Snapshot(), errors.Join(entryErr, interruption)
	}

	rollback := e.drive(ctx, coordinator, stageRollback)
	if interruption := e.stageInterruption(rollback); interruption != nil {
		return coordinator.Snapshot(), errors.Join(
			entryErr,
			e.stageErrors(coordinator.Snapshot(), PhaseRollbackFailed, rollback.liveErrors),
			interruption,
		)
	}
	if err := coordinator.finalize(ctx, stageRollback); err != nil {
		return coordinator.Snapshot(), errors.Join(
			entryErr,
			e.stageErrors(coordinator.Snapshot(), PhaseRollbackFailed, rollback.liveErrors),
			err,
		)
	}
	return coordinator.Snapshot(), errors.Join(
		entryErr,
		e.stageErrors(coordinator.Snapshot(), PhaseRollbackFailed, rollback.liveErrors),
		e.stageInterruption(rollback),
	)
}

type stageResult struct {
	liveErrors        map[string]error
	interruptedErrors map[string]error
}

type stageCompletion struct {
	name          string
	actionErr     error
	completionErr error
}

func (e *Engine) drive(ctx context.Context, coordinator *Coordinator, stage executionStage) stageResult {
	result := stageResult{
		liveErrors:        make(map[string]error),
		interruptedErrors: make(map[string]error),
	}
	names := e.lexicalPhaseNames()
	pending := make(map[string]bool, len(names))
	for _, name := range names {
		pending[name] = true
	}
	running := make(map[string]struct{})
	completions := make(chan stageCompletion, e.options.MaxParallel)

	for len(pending) > 0 || len(running) > 0 {
		progressed := false
		for _, name := range names {
			if !pending[name] {
				continue
			}
			execution, err := coordinator.tryAdmit(ctx, name, stage)
			if errors.Is(err, errAdmissionPending) {
				continue
			}
			if err != nil {
				delete(pending, name)
				progressed = true
				if !e.expectedAdmissionError(coordinator.Snapshot(), name, stage, err) {
					result.interruptedErrors[name] = err
				}
				continue
			}
			delete(pending, name)
			progressed = true
			if !execution.ShouldRun() {
				continue
			}
			running[name] = struct{}{}
			name := name
			_ = concurrency.Go(ctx, func(context.Context) (struct{}, error) {
				actionErr := e.action(name, stage)(execution.Context())
				completions <- stageCompletion{name: name, actionErr: actionErr, completionErr: execution.Complete(ctx, actionErr)}
				return struct{}{}, nil
			})
		}

		if len(running) > 0 {
			completion := <-completions
			delete(running, completion.name)
			if completion.actionErr != nil {
				result.liveErrors[completion.name] = completion.actionErr
			}
			if completion.completionErr != nil && (completion.actionErr == nil || !errors.Is(completion.completionErr, completion.actionErr)) {
				result.interruptedErrors[completion.name] = completion.completionErr
			}
			if persistenceErr := coordinator.persistenceError(); persistenceErr != nil {
				result.interruptedErrors[completion.name] = errors.Join(result.interruptedErrors[completion.name], persistenceErr)
			}
			continue
		}
		if len(pending) == 0 {
			break
		}
		if !progressed {
			if err := coordinator.waitForChange(ctx); err != nil {
				for name := range pending {
					result.interruptedErrors[name] = err
				}
				break
			}
		}
	}
	return result
}

func (e *Engine) expectedAdmissionError(snapshot Snapshot, name string, stage executionStage, err error) bool {
	if errors.Is(err, ErrDependencyFailed) || isTerminal(snapshot.Status) {
		return true
	}
	return stage == stageEnter && (snapshot.Status == RunRollingBack || snapshot.Phases[name].Status == PhaseEnterFailed)
}

func (e *Engine) action(name string, stage executionStage) Action {
	phase := e.plan.phases[name]
	switch stage {
	case stageEnter:
		return phase.Enter
	case stageExit:
		return phase.Exit
	case stageRollback:
		return phase.Rollback
	default:
		panic("unknown lifecycle stage")
	}
}

func (e *Engine) stageInterruption(result stageResult) error {
	return e.orderedErrors(result.interruptedErrors)
}

func (e *Engine) stageErrors(snapshot Snapshot, failed PhaseStatus, liveErrors map[string]error) error {
	ordered := make(map[string]error)
	for name, state := range snapshot.Phases {
		if state.Status != failed {
			continue
		}
		if liveErr := liveErrors[name]; liveErr != nil {
			ordered[name] = liveErr
		} else {
			ordered[name] = errors.New(state.LastError)
		}
	}
	return e.orderedErrors(ordered)
}

func (e *Engine) orderedErrors(byPhase map[string]error) error {
	errs := make([]error, 0, len(byPhase))
	for _, name := range e.orderedPhaseNames() {
		if err := byPhase[name]; err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func mergeErrorMaps(first, second map[string]error) map[string]error {
	merged := make(map[string]error, len(first)+len(second))
	for name, err := range first {
		merged[name] = err
	}
	for name, err := range second {
		merged[name] = err
	}
	return merged
}

func (e *Engine) orderedPhaseNames() []string {
	names := make([]string, 0, len(e.plan.phases))
	for _, level := range e.plan.levels {
		names = append(names, level...)
	}
	return names
}

func (e *Engine) lexicalPhaseNames() []string {
	names := make([]string, 0, len(e.plan.phases))
	for name := range e.plan.phases {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func isTerminal(status RunStatus) bool {
	return status == RunSucceeded || status == RunRolledBack || status == RunFailed
}
