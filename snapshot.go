package lifecycle

import (
	"context"
	"time"
)

// PhaseStatus is the durable state-machine position of one phase.
type PhaseStatus string

const (
	// PhasePending: entry has not started.
	PhasePending PhaseStatus = "pending"
	// PhaseEntering: Enter started and is retried on recovery.
	PhaseEntering PhaseStatus = "entering"
	// PhaseEntered: Enter completed.
	PhaseEntered PhaseStatus = "entered"
	// PhaseExiting: Exit started and is retried on recovery.
	PhaseExiting PhaseStatus = "exiting"
	// PhaseExited: Exit completed.
	PhaseExited PhaseStatus = "exited"
	// PhaseRollingBack: Rollback started and is retried on recovery.
	PhaseRollingBack PhaseStatus = "rolling_back"
	// PhaseRolledBack: Rollback completed.
	PhaseRolledBack PhaseStatus = "rolled_back"
	// PhaseEnterFailed: Enter returned an error; the run rolls back.
	PhaseEnterFailed PhaseStatus = "enter_failed"
	// PhaseExitFailed: Exit returned an error; cleanup continues.
	PhaseExitFailed PhaseStatus = "exit_failed"
	// PhaseRollbackFailed: Rollback returned an error; cleanup continues.
	PhaseRollbackFailed PhaseStatus = "rollback_failed"

	// PhaseFailed is retained as an alias for entry failures.
	//
	// Deprecated: use PhaseEnterFailed.
	PhaseFailed = PhaseEnterFailed
)

// RunStatus is the durable state of a whole run.
type RunStatus string

const (
	// RunPending: no phase has been admitted.
	RunPending RunStatus = "pending"
	// RunRunning: entry or exit is in progress.
	RunRunning RunStatus = "running"
	// RunSucceeded: every phase exited. Terminal.
	RunSucceeded RunStatus = "succeeded"
	// RunRollingBack: an entry failed and entered phases are rolled back.
	RunRollingBack RunStatus = "rolling_back"
	// RunRolledBack: rollback completed without cleanup failures. Terminal.
	RunRolledBack RunStatus = "rolled_back"
	// RunFailed: an exit or rollback action failed. Terminal.
	RunFailed RunStatus = "failed"
)

// PhaseState is the persisted state of one phase.
type PhaseState struct {
	// Status is the durable state-machine position.
	Status PhaseStatus
	// Attempts counts actions started for the phase across entry, exit and
	// rollback.
	Attempts uint
	// LastError is the text of the most recently persisted action error.
	LastError string
	// UpdatedAt is the time of the last transition.
	UpdatedAt time.Time
}

// Snapshot is the complete durable state of one run. Stores persist and
// return it as a value; implementations must not retain or mutate the
// caller's Phases map.
type Snapshot struct {
	// RunID identifies the run.
	RunID string
	// PlanDigest is the Topology.Digest the run was started with.
	PlanDigest string
	// Status is the run state.
	Status RunStatus
	// Phases maps every phase name to its state.
	Phases map[string]PhaseState
	// UpdatedAt is the time of the last transition.
	UpdatedAt time.Time
}

// Store hands out exclusive, fenced sessions for runs. Implementations must
// be safe for concurrent use; package memory provides a process-local one.
type Store interface {
	// Acquire establishes exclusive, fenced ownership of runID across every
	// process that can access the store. It returns ErrRunOwned while another
	// owner is active.
	Acquire(context.Context, string) (Session, error)
}

// Session is exclusive, fenced ownership of one run.
type Session interface {
	// RunID identifies the only run this session may load or save.
	RunID() string
	// Load returns the current durable state for RunID.
	Load(context.Context) (Snapshot, error)
	// Save durably replaces the state for RunID. Implementations must reject
	// writes from a released or superseded fence with ErrOwnershipLost.
	Save(context.Context, Snapshot) error
	// Release relinquishes ownership. Callers should use a context independent
	// of cancellation of the work performed while the session was active.
	Release(context.Context) error
}

// Options configures an Engine or Coordinator.
type Options struct {
	// MaxParallel bounds the number of concurrently admitted actions. Zero
	// means 1; negative values are rejected with ErrInvalidPhase.
	MaxParallel int
	// Clock returns the time recorded in snapshot and phase UpdatedAt
	// fields. Nil means time.Now.
	Clock func() time.Time
}

func cloneSnapshot(snapshot Snapshot) Snapshot {
	copy := snapshot
	copy.Phases = make(map[string]PhaseState, len(snapshot.Phases))
	for name, state := range snapshot.Phases {
		copy.Phases[name] = state
	}
	return copy
}
