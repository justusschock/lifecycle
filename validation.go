package lifecycle

import (
	"errors"
	"fmt"
)

func validateSnapshot(plan *Plan, runID string, snapshot Snapshot) error {
	if snapshot.RunID != runID {
		return invalidSnapshot("run ID is %q, want %q", snapshot.RunID, runID)
	}
	if snapshot.PlanDigest != plan.Digest() {
		return errors.Join(invalidSnapshot("plan digest does not match"), ErrPlanChanged)
	}
	if len(snapshot.Phases) != len(plan.phases) {
		return invalidSnapshot("phase count is %d, want %d", len(snapshot.Phases), len(plan.phases))
	}
	for name := range plan.phases {
		if _, exists := snapshot.Phases[name]; !exists {
			return invalidSnapshot("phase %q is missing", name)
		}
	}
	for name, state := range snapshot.Phases {
		if _, exists := plan.phases[name]; !exists {
			return invalidSnapshot("phase %q is not in the plan", name)
		}
		if !knownPhaseStatus(state.Status) {
			return invalidSnapshot("phase %q has unknown status %q", name, state.Status)
		}
	}
	if !knownRunStatus(snapshot.Status) {
		return invalidSnapshot("run has unknown status %q", snapshot.Status)
	}
	if !consistentRunState(snapshot) {
		return invalidSnapshot("phase statuses are inconsistent with run status %q", snapshot.Status)
	}
	return nil
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
		return allStatusesIn(snapshot, PhasePending, PhaseEntering, PhaseEntered) ||
			allStatusesIn(snapshot, PhaseEntered, PhaseExiting, PhaseExited, PhaseExitFailed)
	case RunRollingBack:
		return allStatusesIn(snapshot, PhasePending, PhaseEntering, PhaseEntered, PhaseEnterFailed, PhaseRollingBack, PhaseRolledBack, PhaseRollbackFailed)
	case RunSucceeded:
		return allStatuses(snapshot, PhaseExited)
	case RunRolledBack:
		return allStatusesIn(snapshot, PhasePending, PhaseEnterFailed, PhaseRolledBack)
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
	return hasStatus(snapshot, PhaseRollbackFailed) && allStatusesIn(snapshot, PhasePending, PhaseEnterFailed, PhaseRolledBack, PhaseRollbackFailed)
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

func invalidSnapshot(format string, arguments ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidSnapshot, fmt.Sprintf(format, arguments...))
}
