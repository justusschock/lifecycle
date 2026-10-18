package memory_test

import (
	"context"
	"errors"
	"testing"
	"time"

	lifecycle "github.com/justusschock/go-lifecycle"
	"github.com/justusschock/go-lifecycle/memory"
)

// FR-LIFE-17
func TestStoreReturnsNotFoundAndCopiesSnapshots(t *testing.T) {
	store := memory.NewStore()
	if _, err := store.Load(context.Background(), "missing"); !errors.Is(err, lifecycle.ErrSnapshotNotFound) {
		t.Fatalf("Load missing error = %v, want %v", err, lifecycle.ErrSnapshotNotFound)
	}

	now := time.Unix(123, 0)
	original := lifecycle.Snapshot{
		RunID:      "run",
		PlanDigest: "digest",
		Status:     lifecycle.RunRunning,
		Phases:     map[string]lifecycle.PhaseState{"phase": {Status: lifecycle.PhaseEntered, Attempts: 1, UpdatedAt: now}},
		UpdatedAt:  now,
	}
	if err := store.Save(context.Background(), original); err != nil {
		t.Fatal(err)
	}
	original.Phases["phase"] = lifecycle.PhaseState{Status: lifecycle.PhaseFailed}

	loaded, err := store.Load(context.Background(), "run")
	if err != nil {
		t.Fatal(err)
	}
	if got := loaded.Phases["phase"].Status; got != lifecycle.PhaseEntered {
		t.Fatalf("stored status = %q, want %q", got, lifecycle.PhaseEntered)
	}
	loaded.Phases["phase"] = lifecycle.PhaseState{Status: lifecycle.PhaseExited}
	reloaded, err := store.Load(context.Background(), "run")
	if err != nil {
		t.Fatal(err)
	}
	if got := reloaded.Phases["phase"].Status; got != lifecycle.PhaseEntered {
		t.Fatalf("reloaded status = %q, want %q", got, lifecycle.PhaseEntered)
	}
}
