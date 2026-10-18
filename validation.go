package lifecycle

import (
	"errors"
	"fmt"
)

func validateSnapshot(plan *Plan, runID string, snapshot Snapshot) error {
	return validateTopologySnapshot(plan.topology, runID, snapshot)
}

func validateTopologySnapshot(topology *Topology, runID string, snapshot Snapshot) error {
	if snapshot.RunID != runID {
		return invalidSnapshot("run ID is %q, want %q", snapshot.RunID, runID)
	}
	if snapshot.PlanDigest != topology.Digest() {
		return errors.Join(invalidSnapshot("plan digest does not match"), ErrPlanChanged)
	}
	if len(snapshot.Phases) != len(topology.phases) {
		return invalidSnapshot("phase count is %d, want %d", len(snapshot.Phases), len(topology.phases))
	}
	for name := range topology.phases {
		if _, exists := snapshot.Phases[name]; !exists {
			return invalidSnapshot("phase %q is missing", name)
		}
	}
	for name, state := range snapshot.Phases {
		if _, exists := topology.phases[name]; !exists {
			return invalidSnapshot("phase %q is not in the plan", name)
		}
		if !knownPhaseStatus(state.Status) {
			return invalidSnapshot("phase %q has unknown status %q", name, state.Status)
		}
		if err := validatePhaseState(name, state); err != nil {
			return err
		}
	}
	if !knownRunStatus(snapshot.Status) {
		return invalidSnapshot("run has unknown status %q", snapshot.Status)
	}
	if err := validateRunReachability(snapshot); err != nil {
		return err
	}
	if !consistentRunState(snapshot) {
		return invalidSnapshot("phase statuses are inconsistent with run status %q", snapshot.Status)
	}
	if snapshot.Status == RunRollingBack && hasStatus(snapshot, PhaseEntering) && hasAnyStatus(snapshot, PhaseRollingBack, PhaseRolledBack, PhaseRollbackFailed) {
		return invalidSnapshot("rollback cleanup started while entry remains in progress")
	}
	if err := validateTopologyDependencyOrdering(topology, snapshot); err != nil {
		return err
	}
	return nil
}

func validateRunReachability(snapshot Snapshot) error {
	switch snapshot.Status {
	case RunRunning:
		if allStatuses(snapshot, PhasePending) {
			return invalidSnapshot("run reachability: running run has no admitted phase")
		}
	case RunRollingBack, RunRolledBack:
		if !hasStatus(snapshot, PhaseEnterFailed) {
			return invalidSnapshot("rollback provenance: run has no failed entry")
		}
	case RunFailed:
		if hasStatus(snapshot, PhaseRollbackFailed) && !hasStatus(snapshot, PhaseEnterFailed) {
			return invalidSnapshot("rollback provenance: failed rollback has no failed entry")
		}
	}
	return nil
}

func validatePhaseState(name string, state PhaseState) error {
	minimumAttempts := uint(0)
	allowsError := false
	switch state.Status {
	case PhasePending:
		if state.Attempts != 0 {
			return invalidSnapshot("phase state for %q is pending with %d attempts", name, state.Attempts)
		}
	case PhaseEntering, PhaseEntered:
		minimumAttempts = 1
	case PhaseEnterFailed:
		minimumAttempts = 1
		allowsError = true
	case PhaseExiting, PhaseExited, PhaseRollingBack, PhaseRolledBack:
		minimumAttempts = 2
	case PhaseExitFailed, PhaseRollbackFailed:
		minimumAttempts = 2
		allowsError = true
	}
	if state.Attempts < minimumAttempts {
		return invalidSnapshot("phase state for %q is %q with %d attempts, want at least %d", name, state.Status, state.Attempts, minimumAttempts)
	}
	if !allowsError && state.LastError != "" {
		return invalidSnapshot("phase state for %q is %q with stale error text", name, state.Status)
	}
	return nil
}

func validateTopologyDependencyOrdering(topology *Topology, snapshot Snapshot) error {
	for _, level := range topology.levels {
		for _, name := range level {
			state := snapshot.Phases[name]
			for _, dependency := range topology.phases[name].DependsOn {
				dependencyState := snapshot.Phases[dependency]
				if state.Status != PhasePending && !hasSuccessfulEntryEvidence(dependencyState.Status) {
					return invalidSnapshot("dependency ordering: phase %q is %q before dependency %q has successful entry evidence", name, state.Status, dependency)
				}
				if exitCleanupStarted(dependencyState.Status) && !exitCleanupResolved(state.Status) {
					return invalidSnapshot("dependency ordering: phase %q is %q while dependency %q has started exit cleanup", name, state.Status, dependency)
				}
				if rollbackCleanupStarted(dependencyState.Status) && !rollbackCleanupResolved(state.Status) {
					return invalidSnapshot("dependency ordering: phase %q is %q while dependency %q has started rollback cleanup", name, state.Status, dependency)
				}
			}
		}
	}
	return nil
}

func hasSuccessfulEntryEvidence(status PhaseStatus) bool {
	switch status {
	case PhaseEntered, PhaseExiting, PhaseExited, PhaseExitFailed,
		PhaseRollingBack, PhaseRolledBack, PhaseRollbackFailed:
		return true
	default:
		return false
	}
}

func exitCleanupStarted(status PhaseStatus) bool {
	return status == PhaseExiting || status == PhaseExited || status == PhaseExitFailed
}

func exitCleanupResolved(status PhaseStatus) bool {
	return status == PhaseExited || status == PhaseExitFailed
}

func rollbackCleanupStarted(status PhaseStatus) bool {
	return status == PhaseRollingBack || status == PhaseRolledBack || status == PhaseRollbackFailed
}

func rollbackCleanupResolved(status PhaseStatus) bool {
	return status == PhasePending || status == PhaseEnterFailed || status == PhaseRolledBack || status == PhaseRollbackFailed
}

func knownPhaseStatus(status PhaseStatus) bool {
	switch status {
	case PhasePending, PhaseEntering, PhaseEntered, PhaseEnterFailed,
		PhaseExiting, PhaseExited, PhaseExitFailed,
		PhaseRollingBack, PhaseRolledBack, PhaseRollbackFailed:
		return true
	default:
		return false
	}
}

func knownRunStatus(status RunStatus) bool {
	switch status {
	case RunPending, RunRunning, RunSucceeded, RunRollingBack, RunRolledBack, RunFailed:
		return true
	default:
		return false
	}
}

func consistentRunState(snapshot Snapshot) bool {
	switch snapshot.Status {
	case RunPending:
		return allStatuses(snapshot, PhasePending)
	case RunRunning:
		return !allStatuses(snapshot, PhasePending) &&
			(allStatusesIn(snapshot, PhasePending, PhaseEntering, PhaseEntered) ||
				allStatusesIn(snapshot, PhaseEntered, PhaseExiting, PhaseExited, PhaseExitFailed))
	case RunRollingBack:
		return hasStatus(snapshot, PhaseEnterFailed) && allStatusesIn(snapshot, PhasePending, PhaseEntering, PhaseEntered, PhaseEnterFailed, PhaseRollingBack, PhaseRolledBack, PhaseRollbackFailed)
	case RunSucceeded:
		return allStatuses(snapshot, PhaseExited)
	case RunRolledBack:
		return hasStatus(snapshot, PhaseEnterFailed) && allStatusesIn(snapshot, PhasePending, PhaseEnterFailed, PhaseRolledBack)
	case RunFailed:
		return terminalExitFailure(snapshot) || terminalRollbackFailure(snapshot)
	default:
		return false
	}
}

func terminalExitFailure(snapshot Snapshot) bool {
	return hasStatus(snapshot, PhaseExitFailed) && allStatusesIn(snapshot, PhaseExited, PhaseExitFailed)
}

func terminalRollbackFailure(snapshot Snapshot) bool {
	return hasStatus(snapshot, PhaseEnterFailed) && hasStatus(snapshot, PhaseRollbackFailed) && allStatusesIn(snapshot, PhasePending, PhaseEnterFailed, PhaseRolledBack, PhaseRollbackFailed)
}

func allStatuses(snapshot Snapshot, status PhaseStatus) bool {
	for _, state := range snapshot.Phases {
		if state.Status != status {
			return false
		}
	}
	return true
}

func allStatusesIn(snapshot Snapshot, allowed ...PhaseStatus) bool {
	for _, state := range snapshot.Phases {
		matched := false
		for _, status := range allowed {
			if state.Status == status {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func hasStatus(snapshot Snapshot, status PhaseStatus) bool {
	for _, state := range snapshot.Phases {
		if state.Status == status {
			return true
		}
	}
	return false
}

func hasAnyStatus(snapshot Snapshot, statuses ...PhaseStatus) bool {
	for _, status := range statuses {
		if hasStatus(snapshot, status) {
			return true
		}
	}
	return false
}

func invalidSnapshot(format string, arguments ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidSnapshot, fmt.Sprintf(format, arguments...))
}
