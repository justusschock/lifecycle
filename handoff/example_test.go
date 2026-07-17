package handoff_test

import (
	"context"
	"fmt"

	"github.com/justusschock/go-lifecycle/handoff"
)

type exporterFunc func(context.Context, []byte) (handoff.Artifact, error)

func (function exporterFunc) Export(ctx context.Context, value []byte) (handoff.Artifact, error) {
	return function(ctx, value)
}

type adopterFunc func(context.Context, handoff.Artifact) (string, error)

func (function adopterFunc) Adopt(ctx context.Context, artifact handoff.Artifact) (string, error) {
	return function(ctx, artifact)
}

type memoryTransport struct {
	artifact handoff.Artifact
}

func (transport *memoryTransport) Send(_ context.Context, artifact handoff.Artifact) (handoff.Receipt, error) {
	transport.artifact = artifact.Clone()
	return handoff.Receipt{ArtifactID: artifact.ID, Location: "memory"}, nil
}

func (transport *memoryTransport) Receive(context.Context, handoff.Receipt) (handoff.Artifact, error) {
	return transport.artifact.Clone(), nil
}

func ExampleProtocol() {
	transport := &memoryTransport{}
	protocol, _ := handoff.NewProtocol[[]byte, string](
		exporterFunc(func(_ context.Context, value []byte) (handoff.Artifact, error) {
			return handoff.NewArtifact("application/octet-stream", 1, value, nil)
		}),
		transport,
		adopterFunc(func(_ context.Context, artifact handoff.Artifact) (string, error) {
			return string(artifact.Data), nil
		}),
	)
	value, receipt, _ := protocol.Transfer(context.Background(), []byte("state"))
	fmt.Println(value, receipt.ArtifactID == transport.artifact.ID)
	// Output: state true
}
