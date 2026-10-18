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

func ExampleCoordinator_EnterPhase() {
	topology, _ := lifecycle.NewTopology([]lifecycle.PhaseSpec{
		{Name: "root"},
		{Name: "child", DependsOn: []string{"root"}},
	})
	coordinator, _ := lifecycle.OpenCoordinator(context.Background(), topology, memory.NewStore(), "example", lifecycle.Options{})
	defer func() { _ = coordinator.Close(context.Background()) }()

	childResult := make(chan *lifecycle.Execution, 1)
	go func() {
		execution, _ := coordinator.EnterPhase(context.Background(), "child")
		childResult <- execution
	}()
	root, _ := coordinator.EnterPhase(context.Background(), "root")
	_ = root.Complete(context.Background(), nil)
	child := <-childResult
	_ = child.Complete(context.Background(), nil)

	rootExit := make(chan *lifecycle.Execution, 1)
	go func() {
		execution, _ := coordinator.ExitPhase(context.Background(), "root")
		rootExit <- execution
	}()
	childExit, _ := coordinator.ExitPhase(context.Background(), "child")
	_ = childExit.Complete(context.Background(), nil)
	_ = (<-rootExit).Complete(context.Background(), nil)

	fmt.Println(coordinator.Snapshot().Status)
	// Output: succeeded
}
