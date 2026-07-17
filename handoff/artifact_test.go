package handoff

import (
	"bytes"
	"errors"
	"testing"

	lifecycle "github.com/justusschock/go-lifecycle"
)

func TestNewArtifactCopiesInputsAndComputesStableIdentity(t *testing.T) {
	data := []byte("state")
	metadata := map[string]string{"owner": "source"}
	artifact, err := NewArtifact("application/octet-stream", 7, data, metadata)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewArtifact("application/octet-stream", 7, []byte("state"), map[string]string{"other": "metadata"})
	if err != nil {
		t.Fatal(err)
	}
	if artifact.ID == "" || artifact.ID != second.ID || artifact.Digest != second.Digest {
		t.Fatalf("unstable identity: first=(%q,%x) second=(%q,%x)", artifact.ID, artifact.Digest, second.ID, second.Digest)
	}

	data[0] = 'X'
	metadata["owner"] = "changed"
	if !bytes.Equal(artifact.Data, []byte("state")) || artifact.Metadata["owner"] != "source" {
		t.Fatalf("artifact changed with inputs: data=%q metadata=%v", artifact.Data, artifact.Metadata)
	}
	if err := artifact.Validate(); err != nil {
		t.Fatalf("Validate() = %v", err)
	}

	clone := artifact.Clone()
	clone.Data[0] = 'Y'
	clone.Metadata["owner"] = "clone"
	if !bytes.Equal(artifact.Data, []byte("state")) || artifact.Metadata["owner"] != "source" {
		t.Fatalf("artifact changed with clone: data=%q metadata=%v", artifact.Data, artifact.Metadata)
	}
}

func TestArtifactRejectsInvalidFieldsAndTampering(t *testing.T) {
	for _, test := range []struct {
		name      string
		mediaType string
		version   uint64
		data      []byte
	}{
		{name: "empty media type", version: 1, data: []byte("state")},
		{name: "zero version", mediaType: "application/octet-stream", data: []byte("state")},
		{name: "empty data", mediaType: "application/octet-stream", version: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewArtifact(test.mediaType, test.version, test.data, nil); !errors.Is(err, lifecycle.ErrInvalidArtifact) {
				t.Fatalf("NewArtifact error = %v, want %v", err, lifecycle.ErrInvalidArtifact)
			}
		})
	}

	valid, err := NewArtifact("application/octet-stream", 1, []byte("state"), nil)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*Artifact)
	}{
		{name: "data", mutate: func(artifact *Artifact) { artifact.Data[0] = 'X' }},
		{name: "digest", mutate: func(artifact *Artifact) { artifact.Digest[0]++ }},
		{name: "ID", mutate: func(artifact *Artifact) { artifact.ID = "different" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			artifact := valid.Clone()
			test.mutate(&artifact)
			if err := artifact.Validate(); !errors.Is(err, lifecycle.ErrIntegrity) {
				t.Fatalf("Validate error = %v, want %v", err, lifecycle.ErrIntegrity)
			}
		})
	}
}
