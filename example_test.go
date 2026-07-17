package lifecycle_test

import (
	"context"
	"fmt"

	lifecycle "github.com/justusschock/go-lifecycle"
	"github.com/justusschock/go-lifecycle/memory"
)

func ExampleEngine() {
	noop := func(context.Context) error { return nil }
	plan, _ := lifecycle.NewPlan([]lifecycle.Phase{
		{Name: "prepare", Enter: noop, Exit: noop, Rollback: noop},
		{Name: "left", DependsOn: []string{"prepare"}, Enter: noop, Exit: noop, Rollback: noop},
		{Name: "right", DependsOn: []string{"prepare"}, Enter: noop, Exit: noop, Rollback: noop},
		{Name: "publish", DependsOn: []string{"left", "right"}, Enter: noop, Exit: noop, Rollback: noop},
	})
	engine, _ := lifecycle.NewEngine(plan, memory.NewStore(), lifecycle.Options{MaxParallel: 2})
	snapshot, _ := engine.Run(context.Background(), "example")
	fmt.Println(snapshot.Status)
	// Output: succeeded
}
