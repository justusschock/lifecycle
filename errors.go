package lifecycle

import "errors"

var (
	ErrInvalidPhase      = errors.New("lifecycle: invalid phase")
	ErrDuplicatePhase    = errors.New("lifecycle: duplicate phase")
	ErrUnknownDependency = errors.New("lifecycle: unknown dependency")
	ErrCycle             = errors.New("lifecycle: cycle")
	ErrSnapshotNotFound  = errors.New("lifecycle: snapshot not found")
	ErrPlanChanged       = errors.New("lifecycle: plan changed")
	ErrRunTerminal       = errors.New("lifecycle: run is terminal")
	ErrInvalidArtifact   = errors.New("lifecycle: invalid artifact")
	ErrIntegrity         = errors.New("lifecycle: integrity check failed")
)
