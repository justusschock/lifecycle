package lifecycle_test

import (
	"context"
	"fmt"

	lifecycle "github.com/justusschock/lifecycle"
	"github.com/justusschock/lifecycle/memory"
)

// Example starts three dependent phases in order and stops them in reverse.
func Example() {
	step := func(message string) lifecycle.Action {
		return func(context.Context) error {
			fmt.Println(message)
			return nil
		}
	}
	plan, err := lifecycle.NewPlan([]lifecycle.Phase{
		{Name: "storage", Enter: step("open storage"), Exit: step("close storage"), Rollback: step("discard storage")},
		{Name: "workers", DependsOn: []string{"storage"}, Enter: step("start workers"), Exit: step("stop workers"), Rollback: step("discard workers")},
		{Name: "http", DependsOn: []string{"workers"}, Enter: step("listen"), Exit: step("stop listening"), Rollback: step("discard listener")},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	engine, err := lifecycle.NewEngine(plan, memory.NewStore(), lifecycle.Options{MaxParallel: 2})
	if err != nil {
		fmt.Println(err)
		return
	}
	snapshot, err := engine.Run(context.Background(), "server-1")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(snapshot.Status)
	// Output:
	// open storage
	// start workers
	// listen
	// stop listening
	// stop workers
	// close storage
	// succeeded
}

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
