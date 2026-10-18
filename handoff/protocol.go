package handoff

import (
	"context"
	"fmt"

	lifecycle "github.com/justusschock/lifecycle"
	"github.com/justusschock/lifecycle/internal/nilcheck"
)

// Exporter turns source state into an artifact.
type Exporter[S any] interface {
	Export(context.Context, S) (Artifact, error)
}

// Transport moves an artifact and retrieves it again by receipt.
type Transport interface {
	Send(context.Context, Artifact) (Receipt, error)
	Receive(context.Context, Receipt) (Artifact, error)
}

// Adopter turns a validated artifact into destination state.
type Adopter[D any] interface {
	Adopt(context.Context, Artifact) (D, error)
}

// Protocol composes an Exporter, a Transport and an Adopter. It holds no
// mutable state of its own; it is safe for concurrent use when its
// collaborators are.
type Protocol[S, D any] struct {
	exporter  Exporter[S]
	transport Transport
	adopter   Adopter[D]
}

// NewProtocol returns a Protocol. It returns lifecycle.ErrInvalidArtifact
// for a nil collaborator, including a typed nil.
func NewProtocol[S, D any](exporter Exporter[S], transport Transport, adopter Adopter[D]) (*Protocol[S, D], error) {
	if nilcheck.IsNil(exporter) {
		return nil, fmt.Errorf("%w: nil exporter", lifecycle.ErrInvalidArtifact)
	}
	if nilcheck.IsNil(transport) {
		return nil, fmt.Errorf("%w: nil transport", lifecycle.ErrInvalidArtifact)
	}
	if nilcheck.IsNil(adopter) {
		return nil, fmt.Errorf("%w: nil adopter", lifecycle.ErrInvalidArtifact)
	}
	return &Protocol[S, D]{exporter: exporter, transport: transport, adopter: adopter}, nil
}

// Transfer runs Export, Send, Receive and Adopt in order. It validates the
// artifact after export and after receive, checks that the receipt and the
// received artifact match the exported ID, and never calls Adopt after an
// integrity failure. Errors are wrapped with the failing stage ("handoff
// export", "handoff send", "handoff receive", "handoff adopt").
func (p *Protocol[S, D]) Transfer(ctx context.Context, source S) (D, Receipt, error) {
	var zero D
	artifact, err := p.exporter.Export(ctx, source)
	if err != nil {
		return zero, Receipt{}, fmt.Errorf("handoff export: %w", err)
	}
	artifact = artifact.Clone()
	if err := artifact.Validate(); err != nil {
		return zero, Receipt{}, fmt.Errorf("handoff export: %w", err)
	}

	receipt, err := p.transport.Send(ctx, artifact.Clone())
	if err != nil {
		return zero, Receipt{}, fmt.Errorf("handoff send: %w", err)
	}
	if receipt.ArtifactID == "" || receipt.Location == "" {
		return zero, receipt, fmt.Errorf("handoff send: %w", lifecycle.ErrInvalidArtifact)
	}
	if receipt.ArtifactID != artifact.ID {
		return zero, receipt, fmt.Errorf("handoff send: %w: receipt artifact %q does not match %q", lifecycle.ErrIntegrity, receipt.ArtifactID, artifact.ID)
	}

	received, err := p.transport.Receive(ctx, receipt)
	if err != nil {
		return zero, receipt, fmt.Errorf("handoff receive: %w", err)
	}
	received = received.Clone()
	if err := received.Validate(); err != nil {
		return zero, receipt, fmt.Errorf("handoff receive: %w", err)
	}
	if received.ID != receipt.ArtifactID || received.ID != artifact.ID {
		return zero, receipt, fmt.Errorf("handoff receive: %w: received artifact %q does not match receipt %q", lifecycle.ErrIntegrity, received.ID, receipt.ArtifactID)
	}

	destination, err := p.adopter.Adopt(ctx, received.Clone())
	if err != nil {
		return zero, receipt, fmt.Errorf("handoff adopt: %w", err)
	}
	return destination, receipt, nil
}
