# ADR 0007: Handoff artifact identity format

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 (FR-LIFE-18, FR-LIFE-19), ADR-0004 |

## Context

Recorded retrospectively for the first public release. Handoff artifacts
cross process and storage boundaries (object-storage stores them as
objects; reconciliation and executor exchange them). Their identity
must be reproducible by any party from the content alone, must change when
any semantic field changes, and must not depend on Go map iteration order.

## Decision drivers

- D1 Content-addressed identity, verifiable without a registry.
- D2 Unambiguous encoding: no two different envelopes hash the same input.
- D3 Stable across Go versions and platforms.
- D4 A version tag so the encoding can evolve.

## Considered options

1. **SHA-256 over a domain-separated, length-prefixed canonical envelope**
   - Pro: unambiguous; independent of map order; cheap.
   - Con: a custom encoding that other implementations must reproduce
     exactly.
2. **SHA-256 over JSON encoding**
   - Pro: familiar.
   - Con: canonical JSON is subtle (key order, number and string
     escaping); ties identity to encoder behaviour.
3. **Random IDs plus a separate checksum**
   - Pro: simple.
   - Con: identity is not derived from content; duplicates are not
     detectable.

## Decision

We will compute `Artifact.Digest` as SHA-256 over: the domain tag
`go-lifecycle/artifact/v1`, the media type, the version (big-endian
`uint64`), the data, the metadata count and each metadata key and value in
sorted key order, where every byte string is prefixed by its length as a
big-endian `uint64`. `Artifact.ID` is the lowercase hex encoding of the
digest. `NewArtifact`, `Clone` and `Protocol` copy byte slices and maps at
every boundary. `Validate` recomputes the digest and rejects mismatches with
`ErrIntegrity`. This encoding is a compatibility contract: changing it
requires a new domain tag and a new ADR.

## Consequences

### Positive
- Any holder of an artifact can verify it; stored artifacts stay valid
  across releases.

### Negative
- `Topology.Digest` uses a different, text-based length-prefixed
  encoding; both encodings are frozen for compatibility because persisted
  snapshots and stored artifacts depend on them.

### Follow-ups
- None.

## Links

- `handoff/artifact.go`
