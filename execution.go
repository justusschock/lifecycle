package lifecycle

import (
	"context"
	"sync"
)

// Execution is a durable admission to execute one lifecycle action. Its
// methods are safe for concurrent use; Complete is idempotent once it has
// persisted the outcome.
type Execution struct {
	coordinator *Coordinator
	name        string
	stage       executionStage
	shouldRun   bool
	ctx         context.Context
	cancel      context.CancelCauseFunc

	mu        sync.Mutex
	completed bool
	resultErr error
}

// ShouldRun reports whether the caller must execute the phase action.
func (e *Execution) ShouldRun() bool {
	return e != nil && e.shouldRun
}

// Context is canceled when another lifecycle action prevents this execution
// from continuing.
func (e *Execution) Context() context.Context {
	if e == nil || e.ctx == nil {
		return context.Background()
	}
	return e.ctx
}

// Complete durably records the outcome of the admitted action.
func (e *Execution) Complete(ctx context.Context, actionErr error) error {
	if e == nil || !e.shouldRun {
		return nil
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.completed {
		return e.resultErr
	}
	resultErr, persisted := e.coordinator.complete(ctx, e, actionErr)
	if persisted {
		e.completed = true
		e.resultErr = resultErr
	}
	return resultErr
}
