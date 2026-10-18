package handoff

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"

	lifecycle "github.com/justusschock/lifecycle"
)

// Artifact is an integrity-checked envelope of exported state. ID is the
// hex-encoded Digest of the canonical media type, version, data and
// metadata. Because the fields are exported, call Validate after modifying
// an artifact.
type Artifact struct {
	ID        string
	MediaType string
	Version   uint64
	Digest    [32]byte
	Data      []byte
	Metadata  map[string]string
}

// NewArtifact copies data and metadata and returns an artifact with its
// digest and ID computed. It returns lifecycle.ErrInvalidArtifact for an
// empty media type, zero version or empty data.
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

// Clone returns a deep copy that shares no mutable storage with a.
func (a Artifact) Clone() Artifact {
	a.Data = slices.Clone(a.Data)
	a.Metadata = maps.Clone(a.Metadata)
	return a
}

// Validate returns lifecycle.ErrInvalidArtifact for missing fields and
// lifecycle.ErrIntegrity when the digest or ID do not match the content.
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

// Receipt identifies an artifact that a Transport has sent.
type Receipt struct {
	ArtifactID string
	Location   string
}
