package lifecycle

import "errors"

var (
	ErrInvalidPhase      = errors.New("lifecycle: invalid phase")
	ErrDuplicatePhase    = errors.New("lifecycle: duplicate phase")
	ErrUnknownDependency = errors.New("lifecycle: unknown dependency")
	ErrDependencyFailed  = errors.New("lifecycle: dependency failed")
	ErrCycle             = errors.New("lifecycle: cycle")
	ErrSnapshotNotFound  = errors.New("lifecycle: snapshot not found")
	ErrInvalidSnapshot   = errors.New("lifecycle: invalid snapshot")
	ErrPlanChanged       = errors.New("lifecycle: plan changed")
	ErrRunTerminal       = errors.New("lifecycle: run is terminal")
	ErrRunOwned          = errors.New("lifecycle: run is already owned")
	ErrOwnershipLost     = errors.New("lifecycle: run ownership lost")
	ErrInvalidArtifact   = errors.New("lifecycle: invalid artifact")
	ErrIntegrity         = errors.New("lifecycle: integrity check failed")
)
