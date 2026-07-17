package memory_test

import (
	"context"
	"errors"
	"testing"

	lifecycle "github.com/justusschock/go-lifecycle"
	"github.com/justusschock/go-lifecycle/memory"
)

func TestStoreSessionsFenceRunOwnership(t *testing.T) {
	store := memory.NewStore()
	first, err := store.Acquire(context.Background(), "run")
	if err != nil {
		t.Fatal(err)
	}
	if first.RunID() != "run" {
		t.Fatalf("RunID = %q, want run", first.RunID())
	}
	if _, err := store.Acquire(context.Background(), "run"); !errors.Is(err, lifecycle.ErrRunOwned) {
		t.Fatalf("second Acquire error = %v, want %v", err, lifecycle.ErrRunOwned)
	}
	if err := first.Release(context.Background()); err != nil {
		t.Fatal(err)
	}

	second, err := store.Acquire(context.Background(), "run")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := second.Release(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()
	if err := first.Save(context.Background(), lifecycle.Snapshot{RunID: "run"}); !errors.Is(err, lifecycle.ErrOwnershipLost) {
		t.Fatalf("stale Save error = %v, want %v", err, lifecycle.ErrOwnershipLost)
	}
	if _, err := first.Load(context.Background()); !errors.Is(err, lifecycle.ErrOwnershipLost) {
		t.Fatalf("stale Load error = %v, want %v", err, lifecycle.ErrOwnershipLost)
	}
	if err := first.Release(context.Background()); !errors.Is(err, lifecycle.ErrOwnershipLost) {
		t.Fatalf("stale Release error = %v, want %v", err, lifecycle.ErrOwnershipLost)
	}
	if err := second.Save(context.Background(), lifecycle.Snapshot{RunID: "other"}); !errors.Is(err, lifecycle.ErrInvalidSnapshot) {
		t.Fatalf("cross-run Save error = %v, want %v", err, lifecycle.ErrInvalidSnapshot)
	}
}

func TestSessionReleaseIgnoresCanceledAcquisitionContext(t *testing.T) {
	store := memory.NewStore()
	ctx, cancel := context.WithCancel(context.Background())
	session, err := store.Acquire(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := session.Release(context.WithoutCancel(ctx)); err != nil {
		t.Fatal(err)
	}
	second, err := store.Acquire(context.Background(), "run")
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}
