package handoff

import (
	"crypto/sha256"
	"encoding/binary"
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
	data = slices.Clone(data)
	metadata = maps.Clone(metadata)
	digest := digestArtifact(mediaType, version, data, metadata)
	return Artifact{
		ID:        hex.EncodeToString(digest[:]),
		MediaType: mediaType,
		Version:   version,
		Digest:    digest,
		Data:      data,
		Metadata:  metadata,
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
	digest := digestArtifact(a.MediaType, a.Version, a.Data, a.Metadata)
	if digest != a.Digest || a.ID != hex.EncodeToString(digest[:]) {
		return fmt.Errorf("%w: artifact %q", lifecycle.ErrIntegrity, a.ID)
	}
	return nil
}

func digestArtifact(mediaType string, version uint64, data []byte, metadata map[string]string) [sha256.Size]byte {
	hash := sha256.New()
	writeCanonicalBytes(hash, []byte("go-lifecycle/artifact/v1"))
	writeCanonicalBytes(hash, []byte(mediaType))
	writeCanonicalUint64(hash, version)
	writeCanonicalBytes(hash, data)

	keys := make([]string, 0, len(metadata))
	for key := range metadata {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	writeCanonicalUint64(hash, uint64(len(keys)))
	for _, key := range keys {
		writeCanonicalBytes(hash, []byte(key))
		writeCanonicalBytes(hash, []byte(metadata[key]))
	}

	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	return digest
}

func writeCanonicalBytes(hash interface{ Write([]byte) (int, error) }, value []byte) {
	writeCanonicalUint64(hash, uint64(len(value)))
	_, _ = hash.Write(value)
}

func writeCanonicalUint64(hash interface{ Write([]byte) (int, error) }, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = hash.Write(encoded[:])
}

type Receipt struct {
	ArtifactID string
	Location   string
}
