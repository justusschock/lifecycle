// Package handoff transfers state between owners through integrity-checked
// artifacts.
//
// A Protocol composes an Exporter, a Transport and an Adopter: Transfer
// exports source state into an Artifact, sends it, receives it back by
// Receipt and adopts it, validating the artifact's SHA-256 identity at every
// boundary. Errors are the sentinels lifecycle.ErrInvalidArtifact and
// lifecycle.ErrIntegrity, wrapped with the failing stage.
package handoff
