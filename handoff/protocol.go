package handoff

import (
	"context"
	"fmt"

	lifecycle "github.com/justusschock/go-lifecycle"
	"github.com/justusschock/go-lifecycle/internal/nilcheck"
)

type Exporter[S any] interface {
	Export(context.Context, S) (Artifact, error)
}

type Transport interface {
	Send(context.Context, Artifact) (Receipt, error)
	Receive(context.Context, Receipt) (Artifact, error)
}

type Adopter[D any] interface {
	Adopt(context.Context, Artifact) (D, error)
}

type Protocol[S, D any] struct {
	exporter  Exporter[S]
	transport Transport
	adopter   Adopter[D]
}

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
