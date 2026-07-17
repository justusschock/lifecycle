package handoff

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"

	lifecycle "github.com/justusschock/go-lifecycle"
)

type Artifact struct {
	ID        string
	MediaType string
	Version   uint64
	Digest    [32]byte
	Data      []byte
	Metadata  map[string]string
}

func NewArtifact(mediaType string, version uint64, data []byte, metadata map[string]string) (Artifact, error) {
	if mediaType == "" || version == 0 || len(data) == 0 {
		return Artifact{}, lifecycle.ErrInvalidArtifact
	}
	digest := sha256.Sum256(data)
	return Artifact{
		ID:        hex.EncodeToString(digest[:]),
		MediaType: mediaType,
		Version:   version,
		Digest:    digest,
		Data:      slices.Clone(data),
		Metadata:  maps.Clone(metadata),
	}, nil
}

func (a Artifact) Clone() Artifact {
	a.Data = slices.Clone(a.Data)
	a.Metadata = maps.Clone(a.Metadata)
	return a
}

func (a Artifact) Validate() error {
	if a.MediaType == "" || a.Version == 0 || len(a.Data) == 0 || a.ID == "" {
		return lifecycle.ErrInvalidArtifact
	}
	digest := sha256.Sum256(a.Data)
	if digest != a.Digest || a.ID != hex.EncodeToString(digest[:]) {
		return fmt.Errorf("%w: artifact %q", lifecycle.ErrIntegrity, a.ID)
	}
	return nil
}

type Receipt struct {
	ArtifactID string
	Location   string
}
