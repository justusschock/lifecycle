package lifecycle_test

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	lifecycle "github.com/justusschock/go-lifecycle"
	"github.com/justusschock/go-lifecycle/memory"
)

// FR-LIFE-16
//
// With a nil Options.Clock, snapshot timestamps come from time.Now, so this
// test controls them with testing/synctest's virtual clock.
func TestEngineStampsTransitionsWithCurrentTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		noop := func(context.Context) error { return nil }
		slow := func(context.Context) error {
			time.Sleep(time.Minute)
			return nil
		}
		plan, err := lifecycle.NewPlan([]lifecycle.Phase{
			{Name: "root", Enter: slow, Exit: slow, Rollback: noop},
			{Name: "child", DependsOn: []string{"root"}, Enter: slow, Exit: noop, Rollback: noop},
		})
		if err != nil {
			t.Fatal(err)
		}
		engine, err := lifecycle.NewEngine(plan, memory.NewStore(), lifecycle.Options{})
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := engine.Run(context.Background(), "timestamps")
		if err != nil {
			t.Fatal(err)
		}

		// root enters (1m), child enters (1m), child exits (0), root exits (1m).
		if got, want := snapshot.Phases["child"].UpdatedAt, start.Add(2*time.Minute); !got.Equal(want) {
			t.Fatalf("child UpdatedAt = %v, want %v", got, want)
		}
		if got, want := snapshot.Phases["root"].UpdatedAt, start.Add(3*time.Minute); !got.Equal(want) {
			t.Fatalf("root UpdatedAt = %v, want %v", got, want)
		}
		if got, want := snapshot.UpdatedAt, start.Add(3*time.Minute); !got.Equal(want) {
			t.Fatalf("run UpdatedAt = %v, want %v", got, want)
		}
	})
}

// FR-LIFE-16
func TestEngineStampsTransitionsWithSuppliedClock(t *testing.T) {
	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	var ticks atomic.Int64
	clock := func() time.Time {
		return base.Add(time.Duration(ticks.Add(1)) * time.Second)
	}
	noop := func(context.Context) error { return nil }
	plan, err := lifecycle.NewPlan([]lifecycle.Phase{
		{Name: "only", Enter: noop, Exit: noop, Rollback: noop},
	})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := lifecycle.NewEngine(plan, memory.NewStore(), lifecycle.Options{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := engine.Run(context.Background(), "supplied-clock")
	if err != nil {
		t.Fatal(err)
	}

	// Every timestamp comes from the supplied clock: the run's last
	// transition is the latest reading, and the phase's precedes it.
	last := base.Add(time.Duration(ticks.Load()) * time.Second)
	if ticks.Load() == 0 {
		t.Fatal("supplied clock was never called")
	}
	if got := snapshot.UpdatedAt; !got.Equal(last) {
		t.Fatalf("run UpdatedAt = %v, want %v", got, last)
	}
	if got := snapshot.Phases["only"].UpdatedAt; !got.After(base) || !got.Before(last) {
		t.Fatalf("phase UpdatedAt = %v, want within (%v, %v)", got, base, last)
	}
}
