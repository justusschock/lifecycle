package lifecycle

import (
	"context"
	"time"
)

type PhaseStatus string

const (
	PhasePending     PhaseStatus = "pending"
	PhaseEntering    PhaseStatus = "entering"
	PhaseEntered     PhaseStatus = "entered"
	PhaseExiting     PhaseStatus = "exiting"
	PhaseExited      PhaseStatus = "exited"
	PhaseRollingBack PhaseStatus = "rolling_back"
	PhaseRolledBack  PhaseStatus = "rolled_back"
	PhaseFailed      PhaseStatus = "failed"
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
	Load(context.Context, string) (Snapshot, error)
	Save(context.Context, Snapshot) error
}

type Options struct {
	MaxParallel int
	Clock       func() time.Time
}
