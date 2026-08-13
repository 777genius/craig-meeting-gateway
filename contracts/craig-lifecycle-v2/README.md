# Craig lifecycle v2 contract bundle

This directory is an immutable producer export for cross-repository consumer contract tests. The JSON Schema mirrors the published v2 boundary in `craig-gateway-contracts`; the canonical fixtures contain synthetic identities only.

Consumers pin `BUNDLE.sha256`, copy the schema and fixture bytes without reformatting, verify `SHA256SUMS`, then parse every event with their published contract parser. Consumer tests must additionally enforce actor-ID uniqueness because JSON Schema `uniqueItems` alone cannot reject two entries with the same `actorId` and different `kind` values.

The authoritative-ready roster is the cumulative roster derived from started and joined events, including actors which later left. Its actor IDs exactly equal `authoritativeTrackActorIds`. V1 participant IDs carry no actor-kind evidence and must never be converted to `human`.
