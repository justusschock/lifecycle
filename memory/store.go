package memory

import (
	"context"
	"fmt"
	"sync"

	lifecycle "github.com/justusschock/go-lifecycle"
)

type Store struct {
	mu        sync.RWMutex
	snapshots map[string]lifecycle.Snapshot
}

func NewStore() *Store {
	return &Store{snapshots: make(map[string]lifecycle.Snapshot)}
}

func (s *Store) Load(ctx context.Context, runID string) (lifecycle.Snapshot, error) {
	if cause := context.Cause(ctx); cause != nil {
		return lifecycle.Snapshot{}, cause
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	snapshot, exists := s.snapshots[runID]
	if !exists {
		return lifecycle.Snapshot{}, fmt.Errorf("%w: %q", lifecycle.ErrSnapshotNotFound, runID)
	}
	return cloneSnapshot(snapshot), nil
}

func (s *Store) Save(ctx context.Context, snapshot lifecycle.Snapshot) error {
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshots[snapshot.RunID] = cloneSnapshot(snapshot)
	return nil
}

func cloneSnapshot(snapshot lifecycle.Snapshot) lifecycle.Snapshot {
	copy := snapshot
	copy.Phases = make(map[string]lifecycle.PhaseState, len(snapshot.Phases))
	for name, state := range snapshot.Phases {
		copy.Phases[name] = state
	}
	return copy
}
