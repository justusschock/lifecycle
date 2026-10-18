package lifecycle

import "errors"

// Sentinel errors returned by the lifecycle, memory and handoff packages.
// Errors are frequently wrapped or joined; test for them with errors.Is.
var (
	// ErrInvalidPhase reports an invalid phase, engine option, run ID, plan,
	// topology, store, or a stage operation that is not available in the
	// current run state.
	ErrInvalidPhase = errors.New("lifecycle: invalid phase")
	// ErrDuplicatePhase reports that more than one phase uses the same name.
	ErrDuplicatePhase = errors.New("lifecycle: duplicate phase")
	// ErrUnknownDependency reports a dependency name that is not in the plan.
	ErrUnknownDependency = errors.New("lifecycle: unknown dependency")
	// ErrDependencyFailed is joined with the original entry error when a
	// phase cannot enter because a transitive dependency failed entry.
	ErrDependencyFailed = errors.New("lifecycle: dependency failed")
	// ErrCycle reports a dependency cycle or a self-dependency.
	ErrCycle = errors.New("lifecycle: cycle")
	// ErrSnapshotNotFound is returned by Session.Load for a run without a
	// stored snapshot.
	ErrSnapshotNotFound = errors.New("lifecycle: snapshot not found")
	// ErrInvalidSnapshot reports a loaded or saved snapshot that violates the
	// lifecycle state contract.
	ErrInvalidSnapshot = errors.New("lifecycle: invalid snapshot")
	// ErrPlanChanged reports a snapshot whose plan digest differs from the
	// supplied plan or topology. It is always joined with ErrInvalidSnapshot.
	ErrPlanChanged = errors.New("lifecycle: plan changed")
	// ErrRunTerminal reports that the run already reached succeeded,
	// rolled_back or failed.
	ErrRunTerminal = errors.New("lifecycle: run is terminal")
	// ErrRunOwned is returned by Store.Acquire while another session owns
	// the run.
	ErrRunOwned = errors.New("lifecycle: run is already owned")
	// ErrOwnershipLost reports an operation through a released or superseded
	// session, or through a closed Coordinator.
	ErrOwnershipLost = errors.New("lifecycle: run ownership lost")
	// ErrInvalidArtifact reports a structurally invalid handoff artifact,
	// receipt or protocol collaborator.
	ErrInvalidArtifact = errors.New("lifecycle: invalid artifact")
	// ErrIntegrity reports a handoff artifact whose data, digest, ID or
	// receipt identity does not agree.
	ErrIntegrity = errors.New("lifecycle: integrity check failed")
)
