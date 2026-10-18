# ADR 0001: Record architecture decisions

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 |

## Context

Recorded retrospectively for the first public release. lifecycle grew
through test-first commits whose reasoning lived in commit messages and a
long README. Public contributors need a durable, reviewable record of what
the library must do and why it is built the way it is, and a process that
keeps that record current.

## Decision drivers

- D1 Decisions must be discoverable without reading the commit history.
- D2 Changes to public API, dependencies, formats or semantics must be
  reviewed before they are implemented.
- D3 The record must stay trustworthy: past decisions are not rewritten.

## Considered options

1. **MADR-style ADRs plus PRDs in `docs/`, docs first**
   - Pro: lightweight Markdown, reviewed in the same pull requests as code.
   - Pro: separates *what* (PRD) from *how and why* (ADR).
   - Con: requires discipline to write documents before code.
2. **README and commit messages only**
   - Pro: no extra files.
   - Con: rationale is scattered and hard to find; superseded reasoning is
     indistinguishable from current reasoning.
3. **External wiki or issue tracker**
   - Pro: rich editing.
   - Con: drifts from the code; not versioned with it.

## Decision

We will record requirements as PRDs (`docs/prd/NNNN-title.md`) and decisions
as MADR-style ADRs (`docs/adr/NNNN-title.md`), using the templates in those
directories. Behaviour is changed docs first: a PRD or ADR is proposed in a
pull request and becomes `Accepted` when merged; implementation follows.
Accepted ADRs are immutable except for their status line; a changed decision
gets a new ADR with `Supersedes: ADR-NNNN`, and the old one is marked
`Superseded by ADR-MMMM`. [docs/README.md](../README.md) indexes every
document with its status.

## Consequences

### Positive
- Rationale is versioned with the code and reviewed with it.
- Tests can cite stable requirement IDs (`FR-LIFE-NN`).

### Negative
- Every significant change carries a documentation cost.

### Follow-ups
- Keep the index in `docs/README.md` current with every document change.

## Links

- [MADR](https://adr.github.io/madr/)
- [AGENTS.md](../../AGENTS.md)
