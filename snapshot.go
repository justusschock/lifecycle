package lifecycle

import (
	"context"
	"time"
)

type PhaseStatus string

const (
	PhasePending        PhaseStatus = "pending"
	PhaseEntering       PhaseStatus = "entering"
	PhaseEntered        PhaseStatus = "entered"
	PhaseExiting        PhaseStatus = "exiting"
	PhaseExited         PhaseStatus = "exited"
	PhaseRollingBack    PhaseStatus = "rolling_back"
	PhaseRolledBack     PhaseStatus = "rolled_back"
	PhaseEnterFailed    PhaseStatus = "enter_failed"
	PhaseExitFailed     PhaseStatus = "exit_failed"
	PhaseRollbackFailed PhaseStatus = "rollback_failed"

	// PhaseFailed is retained as an alias for entry failures.
	// Deprecated: use PhaseEnterFailed.
	PhaseFailed = PhaseEnterFailed
)

type RunStatus string

const (
	RunPending     RunStatus = "pending"
	RunRunning     RunStatus = "running"
	RunSucceeded   RunStatus = "succeeded"
	RunRollingBack RunStatus = "rolling_back"
	RunRolledBack  RunStatus = "rolled_back"
	RunFailed      RunStatus = "failed"
)

type PhaseState struct {
	Status    PhaseStatus
	Attempts  uint
	LastError string
	UpdatedAt time.Time
}

type Snapshot struct {
	RunID      string
	PlanDigest string
	Status     RunStatus
	Phases     map[string]PhaseState
	UpdatedAt  time.Time
}

type Store interface {
	// Acquire establishes exclusive, fenced ownership of runID across every
	// process that can access the store. It returns ErrRunOwned while another
	// owner is active.
	Acquire(context.Context, string) (Session, error)
}

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

type Options struct {
	MaxParallel int
	Clock       func() time.Time
}
