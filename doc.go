// Package lifecycle runs restartable lifecycles over directed acyclic graphs
// of phases.
//
// A Phase has Enter, Exit and Rollback actions and named dependencies.
// NewPlan validates the graph; NewTopology validates the same graph without
// callbacks. An Engine drives a Plan to completion: it enters phases once
// their dependencies have entered, exits them in reverse dependency order,
// and rolls back entered phases when an entry fails. A Coordinator exposes
// the same durable state machine to callers that run the actions
// themselves.
//
// Every action boundary is persisted through a fenced Session from a Store
// before and after the action runs, so an interrupted run resumes without
// repeating completed actions. Actions must therefore be idempotent.
// Package memory provides a process-local Store and package handoff a
// generic, integrity-checked state transfer protocol.
package lifecycle
