package memory

import (
	"context"
	"errors"
	"fmt"
	"sync"

	lifecycle "github.com/justusschock/go-lifecycle"
)

// Store is a process-local, mutex-protected lifecycle.Store. It clones
// snapshots at every boundary and fences stale sessions with
// lifecycle.ErrOwnershipLost. It is not durable and cannot coordinate
// separate processes. The zero value is ready for use, and a Store is safe
// for concurrent use.
type Store struct {
	mu   sync.Mutex
	runs map[string]*runState
}

type runState struct {
	snapshot    *lifecycle.Snapshot
	nextFence   uint64
	activeFence uint64
}

type session struct {
	store *Store
	runID string
	fence uint64
}

// NewStore returns an empty Store.
func NewStore() *Store {
	return &Store{runs: make(map[string]*runState)}
}

// Acquire returns an exclusive session for runID, or lifecycle.ErrRunOwned
// while another session owns it.
func (s *Store) Acquire(ctx context.Context, runID string) (lifecycle.Session, error) {
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	if runID == "" {
		return nil, fmt.Errorf("%w: empty run ID", lifecycle.ErrInvalidSnapshot)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.runs == nil {
		s.runs = make(map[string]*runState)
	}
	state := s.runs[runID]
	if state == nil {
		state = &runState{}
		s.runs[runID] = state
	}
	if state.activeFence != 0 {
		return nil, fmt.Errorf("%w: %q", lifecycle.ErrRunOwned, runID)
	}
	state.nextFence++
	if state.nextFence == 0 {
		state.nextFence++
	}
	state.activeFence = state.nextFence
	return &session{store: s, runID: runID, fence: state.activeFence}, nil
}

func (s *session) RunID() string {
	return s.runID
}

func (s *session) Load(ctx context.Context) (lifecycle.Snapshot, error) {
	if cause := context.Cause(ctx); cause != nil {
		return lifecycle.Snapshot{}, cause
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	state, err := s.ownedState()
	if err != nil {
		return lifecycle.Snapshot{}, err
	}
	if state.snapshot == nil {
		return lifecycle.Snapshot{}, fmt.Errorf("%w: %q", lifecycle.ErrSnapshotNotFound, s.runID)
	}
	return cloneSnapshot(*state.snapshot), nil
}

func (s *session) Save(ctx context.Context, snapshot lifecycle.Snapshot) error {
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	if snapshot.RunID != s.runID {
		return fmt.Errorf("%w: session for %q cannot save %q", lifecycle.ErrInvalidSnapshot, s.runID, snapshot.RunID)
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	state, err := s.ownedState()
	if err != nil {
		return err
	}
	copy := cloneSnapshot(snapshot)
	state.snapshot = &copy
	return nil
}

func (s *session) Release(ctx context.Context) error {
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	state, err := s.ownedState()
	if err != nil {
		return err
	}
	state.activeFence = 0
	return nil
}

func (s *session) ownedState() (*runState, error) {
	state := s.store.runs[s.runID]
	if state == nil || state.activeFence != s.fence {
		return nil, fmt.Errorf("%w: %q", lifecycle.ErrOwnershipLost, s.runID)
	}
	return state, nil
}

// Load acquires runID, loads its snapshot and releases it again. It is a
// convenience for inspecting a run that no session currently owns.
func (s *Store) Load(ctx context.Context, runID string) (snapshot lifecycle.Snapshot, resultErr error) {
	session, err := s.Acquire(ctx, runID)
	if err != nil {
		return lifecycle.Snapshot{}, err
	}
	defer func() {
		resultErr = errors.Join(resultErr, session.Release(context.WithoutCancel(ctx)))
	}()
	return session.Load(ctx)
}

// Save acquires snapshot.RunID, saves the snapshot and releases it again. It
// is a convenience for seeding a run that no session currently owns.
func (s *Store) Save(ctx context.Context, snapshot lifecycle.Snapshot) (resultErr error) {
	session, err := s.Acquire(ctx, snapshot.RunID)
	if err != nil {
		return err
	}
	defer func() {
		resultErr = errors.Join(resultErr, session.Release(context.WithoutCancel(ctx)))
	}()
	return session.Save(ctx, snapshot)
}

func cloneSnapshot(snapshot lifecycle.Snapshot) lifecycle.Snapshot {
	copy := snapshot
	copy.Phases = make(map[string]lifecycle.PhaseState, len(snapshot.Phases))
	for name, state := range snapshot.Phases {
		copy.Phases[name] = state
	}
	return copy
}
