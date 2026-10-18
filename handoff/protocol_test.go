package handoff

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	lifecycle "github.com/justusschock/go-lifecycle"
)

// FR-LIFE-19
func TestProtocolTransfersInOrderAcrossCopyBoundaries(t *testing.T) {
	var order []string
	var exported Artifact
	protocol, err := NewProtocol[string, string](
		exporterFunc[string](func(_ context.Context, source string) (Artifact, error) {
			order = append(order, "export")
			artifact, err := NewArtifact("text/plain", 1, []byte(source), map[string]string{"origin": "exporter"})
			exported = artifact.Clone()
			return artifact, err
		}),
		&transportFuncs{
			send: func(_ context.Context, artifact Artifact) (Receipt, error) {
				order = append(order, "send")
				artifact.Data[0] = 'X'
				artifact.Metadata["origin"] = "transport"
				return Receipt{ArtifactID: exported.ID, Location: "memory"}, nil
			},
			receive: func(_ context.Context, receipt Receipt) (Artifact, error) {
				order = append(order, "receive")
				return exported.Clone(), nil
			},
		},
		adopterFunc[string](func(_ context.Context, artifact Artifact) (string, error) {
			order = append(order, "adopt")
			artifact.Data[0] = 'Y'
			return string(exported.Data), nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	value, receipt, err := protocol.Transfer(context.Background(), "state")
	if err != nil {
		t.Fatal(err)
	}
	if value != "state" || receipt.ArtifactID != exported.ID || receipt.Location != "memory" {
		t.Fatalf("Transfer = (%q,%+v), want state and matching receipt", value, receipt)
	}
	if got, want := order, []string{"export", "send", "receive", "adopt"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("call order = %v, want %v", got, want)
	}
	if !bytes.Equal(exported.Data, []byte("state")) || exported.Metadata["origin"] != "exporter" {
		t.Fatalf("exported artifact was mutated: data=%q metadata=%v", exported.Data, exported.Metadata)
	}
}

// FR-LIFE-19
func TestProtocolRejectsReceiptAndReceivedArtifactMismatchBeforeAdoption(t *testing.T) {
	exported, err := NewArtifact("application/octet-stream", 1, []byte("exported"), nil)
	if err != nil {
		t.Fatal(err)
	}
	received, err := NewArtifact("application/octet-stream", 1, []byte("received"), nil)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("receipt", func(t *testing.T) {
		var receiveCalls, adoptCalls int
		protocol := mustProtocol(t,
			exporterFunc[string](func(context.Context, string) (Artifact, error) { return exported.Clone(), nil }),
			&transportFuncs{
				send: func(context.Context, Artifact) (Receipt, error) {
					return Receipt{ArtifactID: "wrong", Location: "memory"}, nil
				},
				receive: func(context.Context, Receipt) (Artifact, error) { receiveCalls++; return exported.Clone(), nil },
			},
			adopterFunc[string](func(context.Context, Artifact) (string, error) { adoptCalls++; return "", nil }),
		)
		if _, _, err := protocol.Transfer(context.Background(), "source"); !errors.Is(err, lifecycle.ErrIntegrity) {
			t.Fatalf("Transfer error = %v, want %v", err, lifecycle.ErrIntegrity)
		}
		if receiveCalls != 0 || adoptCalls != 0 {
			t.Fatalf("calls after receipt mismatch = receive:%d adopt:%d", receiveCalls, adoptCalls)
		}
	})

	t.Run("received artifact", func(t *testing.T) {
		var adoptCalls int
		protocol := mustProtocol(t,
			exporterFunc[string](func(context.Context, string) (Artifact, error) { return exported.Clone(), nil }),
			&transportFuncs{
				send: func(context.Context, Artifact) (Receipt, error) {
					return Receipt{ArtifactID: exported.ID, Location: "memory"}, nil
				},
				receive: func(context.Context, Receipt) (Artifact, error) { return received.Clone(), nil },
			},
			adopterFunc[string](func(context.Context, Artifact) (string, error) { adoptCalls++; return "", nil }),
		)
		if _, _, err := protocol.Transfer(context.Background(), "source"); !errors.Is(err, lifecycle.ErrIntegrity) {
			t.Fatalf("Transfer error = %v, want %v", err, lifecycle.ErrIntegrity)
		}
		if adoptCalls != 0 {
			t.Fatalf("adopt calls = %d, want 0", adoptCalls)
		}
	})
}

// FR-LIFE-19
func TestProtocolPreservesStageErrors(t *testing.T) {
	artifact, err := NewArtifact("application/octet-stream", 1, []byte("state"), nil)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name      string
		stage     string
		exporter  Exporter[string]
		transport Transport
		adopter   Adopter[string]
	}{
		{
			name: "export", stage: "handoff export",
			exporter:  exporterFunc[string](func(context.Context, string) (Artifact, error) { return Artifact{}, errStage }),
			transport: successfulTransport(artifact), adopter: successfulAdopter(),
		},
		{
			name: "send", stage: "handoff send",
			exporter:  successfulExporter(artifact),
			transport: &transportFuncs{send: func(context.Context, Artifact) (Receipt, error) { return Receipt{}, errStage }},
			adopter:   successfulAdopter(),
		},
		{
			name: "receive", stage: "handoff receive",
			exporter: successfulExporter(artifact),
			transport: &transportFuncs{
				send: func(context.Context, Artifact) (Receipt, error) {
					return Receipt{ArtifactID: artifact.ID, Location: "memory"}, nil
				},
				receive: func(context.Context, Receipt) (Artifact, error) { return Artifact{}, errStage },
			},
			adopter: successfulAdopter(),
		},
		{
			name: "adopt", stage: "handoff adopt",
			exporter: successfulExporter(artifact), transport: successfulTransport(artifact),
			adopter: adopterFunc[string](func(context.Context, Artifact) (string, error) { return "", errStage }),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			protocol := mustProtocol(t, test.exporter, test.transport, test.adopter)
			if _, _, err := protocol.Transfer(context.Background(), "source"); !errors.Is(err, errStage) || !strings.Contains(err.Error(), test.stage) {
				t.Fatalf("Transfer error = %v, want wrapped %s: %v", err, test.stage, errStage)
			}
		})
	}
}

// FR-LIFE-19
func TestNewProtocolRejectsNilCollaborators(t *testing.T) {
	artifact, err := NewArtifact("application/octet-stream", 1, []byte("state"), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name      string
		exporter  Exporter[string]
		transport Transport
		adopter   Adopter[string]
	}{
		{name: "exporter", transport: successfulTransport(artifact), adopter: successfulAdopter()},
		{name: "transport", exporter: successfulExporter(artifact), adopter: successfulAdopter()},
		{name: "adopter", exporter: successfulExporter(artifact), transport: successfulTransport(artifact)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if protocol, err := NewProtocol(test.exporter, test.transport, test.adopter); protocol != nil || err == nil {
				t.Fatalf("NewProtocol = (%v,%v), want (nil,error)", protocol, err)
			}
		})
	}

	var nilExporter exporterFunc[string]
	var nilTransport *transportFuncs
	var nilAdopter adopterFunc[string]
	for _, test := range []struct {
		name      string
		exporter  Exporter[string]
		transport Transport
		adopter   Adopter[string]
	}{
		{name: "typed nil exporter", exporter: nilExporter, transport: successfulTransport(artifact), adopter: successfulAdopter()},
		{name: "typed nil transport", exporter: successfulExporter(artifact), transport: nilTransport, adopter: successfulAdopter()},
		{name: "typed nil adopter", exporter: successfulExporter(artifact), transport: successfulTransport(artifact), adopter: nilAdopter},
	} {
		t.Run(test.name, func(t *testing.T) {
			if protocol, err := NewProtocol(test.exporter, test.transport, test.adopter); protocol != nil || err == nil {
				t.Fatalf("NewProtocol = (%v,%v), want (nil,error)", protocol, err)
			}
		})
	}
}

var errStage = errors.New("stage failed")

type exporterFunc[S any] func(context.Context, S) (Artifact, error)

func (function exporterFunc[S]) Export(ctx context.Context, source S) (Artifact, error) {
	return function(ctx, source)
}

type adopterFunc[D any] func(context.Context, Artifact) (D, error)

func (function adopterFunc[D]) Adopt(ctx context.Context, artifact Artifact) (D, error) {
	return function(ctx, artifact)
}

type transportFuncs struct {
	send    func(context.Context, Artifact) (Receipt, error)
	receive func(context.Context, Receipt) (Artifact, error)
}

func (transport *transportFuncs) Send(ctx context.Context, artifact Artifact) (Receipt, error) {
	return transport.send(ctx, artifact)
}

func (transport *transportFuncs) Receive(ctx context.Context, receipt Receipt) (Artifact, error) {
	return transport.receive(ctx, receipt)
}

func mustProtocol[S, D any](t *testing.T, exporter Exporter[S], transport Transport, adopter Adopter[D]) *Protocol[S, D] {
	t.Helper()
	protocol, err := NewProtocol(exporter, transport, adopter)
	if err != nil {
		t.Fatal(err)
	}
	return protocol
}

func successfulExporter(artifact Artifact) Exporter[string] {
	return exporterFunc[string](func(context.Context, string) (Artifact, error) { return artifact.Clone(), nil })
}

func successfulTransport(artifact Artifact) Transport {
	return &transportFuncs{
		send: func(context.Context, Artifact) (Receipt, error) {
			return Receipt{ArtifactID: artifact.ID, Location: "memory"}, nil
		},
		receive: func(context.Context, Receipt) (Artifact, error) { return artifact.Clone(), nil },
	}
}

func successfulAdopter() Adopter[string] {
	return adopterFunc[string](func(_ context.Context, artifact Artifact) (string, error) { return string(artifact.Data), nil })
}
