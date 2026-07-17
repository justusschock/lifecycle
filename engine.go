package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
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
	if plan.phases == nil || plan.levels == nil || plan.digest == "" {
		return nil, fmt.Errorf("%w: uninitialized plan", ErrInvalidPhase)
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
		resultErr = errors.Join(resultErr, session.Release(context.WithoutCancel(ctx)))
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
		if err := validateSnapshot(e.plan, runID, snapshot); err != nil {
			return snapshot, err
		}
		if isTerminal(snapshot.Status) {
			return snapshot, ErrRunTerminal
		}
	}

	run := newRunner(session, e.options.Clock, snapshot)
	if snapshot.Status == RunRollingBack {
		return e.resumeRollback(ctx, run, nil)
	}

	entry := e.schedule(ctx, run, scheduleEnter, true)
	if interruption := e.scheduleInterruption(entry); interruption != nil {
		return run.current(), interruption
	}
	if run.current().Status == RunRollingBack {
		return e.resumeRollback(ctx, run, entry.liveErrors)
	}
	return e.resumeExit(ctx, run)
}

func (e *Engine) newSnapshot(runID string) Snapshot {
	phases := make(map[string]PhaseState, len(e.plan.phases))
	for name := range e.plan.phases {
		phases[name] = PhaseState{Status: PhasePending}
	}
	return Snapshot{
		RunID:      runID,
		PlanDigest: e.plan.Digest(),
		Status:     RunPending,
		Phases:     phases,
		UpdatedAt:  e.options.Clock(),
	}
}

func (e *Engine) resumeExit(ctx context.Context, run *runner) (Snapshot, error) {
	exit := e.schedule(ctx, run, scheduleExit, false)
	exitErr := e.stageErrors(run.current(), PhaseExitFailed, exit.liveErrors)
	if interruption := e.scheduleInterruption(exit); interruption != nil {
		return run.current(), errors.Join(exitErr, interruption)
	}

	status := RunSucceeded
	if exitErr != nil {
		status = RunFailed
	} else if !e.allPhasesHaveStatus(run.current(), PhaseExited) {
		return run.current(), fmt.Errorf("%w: exit traversal left unresolved phases", ErrInvalidSnapshot)
	}
	statusErr := run.setRunStatus(ctx, status)
	return run.current(), errors.Join(exitErr, statusErr)
}

func (e *Engine) resumeRollback(ctx context.Context, run *runner, liveEntryErrors map[string]error) (Snapshot, error) {
	retried := e.schedule(ctx, run, scheduleRetryEnter, false)
	liveEntryErrors = mergeErrorMaps(liveEntryErrors, retried.liveErrors)
	entryErr := e.stageErrors(run.current(), PhaseEnterFailed, liveEntryErrors)
	if interruption := e.scheduleInterruption(retried); interruption != nil {
		return run.current(), errors.Join(entryErr, interruption)
	}

	rollback := e.schedule(ctx, run, scheduleRollback, false)
	entryErr = e.stageErrors(run.current(), PhaseEnterFailed, liveEntryErrors)
	rollbackErr := e.stageErrors(run.current(), PhaseRollbackFailed, rollback.liveErrors)
	if interruption := e.scheduleInterruption(rollback); interruption != nil {
		return run.current(), errors.Join(entryErr, rollbackErr, interruption)
	}

	status := RunRolledBack
	if rollbackErr != nil {
		status = RunFailed
	}
	statusErr := run.setRunStatus(ctx, status)
	return run.current(), errors.Join(entryErr, rollbackErr, statusErr)
}

type scheduleKind uint8

const (
	scheduleEnter scheduleKind = iota
	scheduleRetryEnter
	scheduleExit
	scheduleRollback
)

type scheduledCompletion struct {
	name      string
	execution actionExecution
}

type scheduleResult struct {
	liveErrors        map[string]error
	interruptedErrors map[string]error
	controlErr        error
}

func (e *Engine) schedule(ctx context.Context, run *runner, kind scheduleKind, stopOnActionFailure bool) scheduleResult {
	result := scheduleResult{
		liveErrors:        make(map[string]error),
		interruptedErrors: make(map[string]error),
	}
	actionCtx, cancelActions := context.WithCancelCause(ctx)
	defer cancelActions(nil)
	completions := make(chan scheduledCompletion, e.options.MaxParallel)
	running := make(map[string]struct{})
	dependents := e.dependents()
	halted := false

	for {
		if !halted {
			for len(running) < e.options.MaxParallel {
				ready := e.readyPhases(run.current(), running, dependents, kind)
				if len(ready) == 0 {
					break
				}
				available := e.options.MaxParallel - len(running)
				if len(ready) > available {
					ready = ready[:available]
				}
				for _, phase := range ready {
					phase := phase
					running[phase.Name] = struct{}{}
					_ = concurrency.Go(actionCtx, func(taskCtx context.Context) (struct{}, error) {
						action, inProgress, completed, failed, failedRunStatus := scheduledAction(kind, phase)
						execution := run.runAction(ctx, taskCtx, phase, action, inProgress, completed, failed, failedRunStatus)
						completions <- scheduledCompletion{name: phase.Name, execution: execution}
						return struct{}{}, nil
					})
				}
			}
		}

		if len(running) == 0 {
			if !halted && e.hasCandidates(run.current(), kind) {
				result.controlErr = fmt.Errorf("%w: %s scheduler has blocked phases", ErrInvalidSnapshot, kind)
			}
			return result
		}

		completion := <-completions
		delete(running, completion.name)
		execution := completion.execution
		if execution.persistenceErr != nil {
			interrupted := errors.Join(execution.actionErr, execution.persistenceErr)
			result.interruptedErrors[completion.name] = interrupted
			halted = true
			cancelActions(interrupted)
			continue
		}
		if execution.actionErr != nil {
			result.liveErrors[completion.name] = execution.actionErr
			if stopOnActionFailure {
				halted = true
				cancelActions(execution.actionErr)
			}
		}
	}
}

func scheduledAction(kind scheduleKind, phase Phase) (Action, PhaseStatus, PhaseStatus, PhaseStatus, RunStatus) {
	switch kind {
	case scheduleEnter, scheduleRetryEnter:
		return phase.Enter, PhaseEntering, PhaseEntered, PhaseEnterFailed, RunRollingBack
	case scheduleExit:
		return phase.Exit, PhaseExiting, PhaseExited, PhaseExitFailed, ""
	case scheduleRollback:
		return phase.Rollback, PhaseRollingBack, PhaseRolledBack, PhaseRollbackFailed, ""
	default:
		panic("unknown lifecycle schedule kind")
	}
}

func (e *Engine) readyPhases(snapshot Snapshot, running map[string]struct{}, dependents map[string][]string, kind scheduleKind) []Phase {
	if !runStatusAllowsSchedule(snapshot.Status, kind) {
		return nil
	}
	ready := make([]Phase, 0)
	for _, name := range e.lexicalPhaseNames() {
		if _, active := running[name]; active {
			continue
		}
		if !isScheduleCandidate(snapshot.Phases[name].Status, kind) || !e.phaseReady(snapshot, name, dependents, kind) {
			continue
		}
		ready = append(ready, e.plan.phases[name])
	}
	return ready
}

func (e *Engine) phaseReady(snapshot Snapshot, name string, dependents map[string][]string, kind scheduleKind) bool {
	switch kind {
	case scheduleRetryEnter:
		return true
	case scheduleEnter:
		for _, dependency := range e.plan.phases[name].DependsOn {
			if snapshot.Phases[dependency].Status != PhaseEntered {
				return false
			}
		}
		return true
	case scheduleExit:
		for _, dependent := range dependents[name] {
			status := snapshot.Phases[dependent].Status
			if status != PhaseExited && status != PhaseExitFailed {
				return false
			}
		}
		return true
	case scheduleRollback:
		for _, dependent := range dependents[name] {
			status := snapshot.Phases[dependent].Status
			if status != PhasePending && status != PhaseEnterFailed && status != PhaseRolledBack && status != PhaseRollbackFailed {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func isScheduleCandidate(status PhaseStatus, kind scheduleKind) bool {
	switch kind {
	case scheduleEnter:
		return status == PhasePending || status == PhaseEntering
	case scheduleRetryEnter:
		return status == PhaseEntering
	case scheduleExit:
		return status == PhaseEntered || status == PhaseExiting
	case scheduleRollback:
		return status == PhaseEntered || status == PhaseRollingBack
	default:
		return false
	}
}

func (e *Engine) hasCandidates(snapshot Snapshot, kind scheduleKind) bool {
	if !runStatusAllowsSchedule(snapshot.Status, kind) {
		return false
	}
	for _, state := range snapshot.Phases {
		if isScheduleCandidate(state.Status, kind) {
			return true
		}
	}
	return false
}

func runStatusAllowsSchedule(status RunStatus, kind scheduleKind) bool {
	switch kind {
	case scheduleEnter:
		return status == RunPending || status == RunRunning
	case scheduleRetryEnter, scheduleRollback:
		return status == RunRollingBack
	case scheduleExit:
		return status == RunRunning
	default:
		return false
	}
}

func (e *Engine) scheduleInterruption(result scheduleResult) error {
	return errors.Join(e.orderedErrors(result.interruptedErrors), result.controlErr)
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

func (e *Engine) allPhasesHaveStatus(snapshot Snapshot, status PhaseStatus) bool {
	for _, state := range snapshot.Phases {
		if state.Status != status {
			return false
		}
	}
	return true
}

func (e *Engine) lexicalPhaseNames() []string {
	names := make([]string, 0, len(e.plan.phases))
	for name := range e.plan.phases {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func (e *Engine) orderedPhaseNames() []string {
	names := make([]string, 0, len(e.plan.phases))
	for _, level := range e.plan.levels {
		names = append(names, level...)
	}
	return names
}

func (e *Engine) dependents() map[string][]string {
	dependents := make(map[string][]string, len(e.plan.phases))
	for name, phase := range e.plan.phases {
		for _, dependency := range phase.DependsOn {
			dependents[dependency] = append(dependents[dependency], name)
		}
	}
	for name := range dependents {
		slices.Sort(dependents[name])
	}
	return dependents
}

func (kind scheduleKind) String() string {
	switch kind {
	case scheduleEnter:
		return "entry"
	case scheduleRetryEnter:
		return "entry recovery"
	case scheduleExit:
		return "exit"
	case scheduleRollback:
		return "rollback"
	default:
		return "unknown"
	}
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
