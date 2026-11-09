# ADR-0003 — Slim Silver result contract with immutable Bronze lineage

Status: Implemented as an explicit local pilot; production migration not accepted or executed.

Date: 2026-11-08

## Context

The storage audit found duplicated raw representations in Bronze, Silver Parquet,
Silver Iceberg and processing caches. Gold uses only the source lot ID from result
child raw JSON; result root JSON and observation payload are excluded from its
input projection. The existing Silver writer is not content-aware and a schema
change must not silently migrate published tables or change revision identity.

The owner requested starting the reviewed schema/result pilot. This implementation
covers a bounded committed result day and keeps existing warehouse schemas intact.

## Decision

Introduce `silver_result_slim_v1` as an explicit projection of the existing typed,
validated result outputs. It contains all six resource tables:

| Table | Grain | Change |
|---|---|---|
| observation | Physical Bronze file/hash/ordinal identity | Remove payload_json; retain immutable raw locator and provenance |
| result_revision | Existing revision_id | Remove root_json; retain all typed fields and observation FK |
| award_lot | Existing award_lot_id, parent revision_id | Replace raw_json with nullable source_lot_id |
| award_contractor | Existing award_contractor_id | Preserve attributes and lot/revision FKs |
| quality_issue | issue_id | Preserve error code/path/severity and observation reference |
| quarantine | observation_id | Preserve reason and raw retrieval through observation |

Do not infer source_lot_id from lot_no. Its extraction matches the old Gold JSON
expression exactly, including nulls. The shared Silver→Gold adapter accepts the
typed field and reconstructs a tiny compatibility object for existing SQL. This
avoids requiring the persisted wide JSON column, while keeping consumer behavior.
Legacy tables continue to use the old projection. The projection change is covered
by the existing Gold logic hash fence; existing unfinished plans are not resumed.

The pilot uses the existing result transformer/assembler rather than introducing
new business mappings. A single local `pilot.duckdb` owns one resource/day. Candidate
tables and their receipt commit atomically. An identical immutable input and logic
identity reuses a verified candidate; force recomputes and compares output. Only
changed candidate tables are replaced. No S3 candidate export or Iceberg publication
is performed. Empty child tables participate in replacement and cannot leave stale
rows from the previous candidate.

This database is a replaceable pilot candidate, NOT the canonical revision-history
warehouse. Old candidate generations are not retained; raw source history remains
in Bronze. Transaction rollback is tested, but historical release rollback and
cross-table Iceberg publication remain separate work.

Raw retrieval verifies file SHA-256, row ordinal, run/date/source identity and
content hash before returning the original Bronze row. Raw retention and object
immutability remain prerequisites. A normal no-op trusts the committed source
identity; it does not rescan Bronze to detect violations of immutable storage.

## Scope and bounds

Default ceiling: 20,000 input records and 64 MiB serialized input, 1 DuckDB thread,
512 MB candidate engine and 256 MB projection engine. These are pilot admission
limits, not a claim that total Python/Arrow RSS is byte-bounded. The existing
assembler still materializes a complete admitted day and expands child rows.
Oversized days fail rather than silently split dependency/history semantics.

All 23 legacy Silver tables / 434 columns were inventoried in the preceding audit.
Only the six result resource tables have an implemented slim contract here. Other
resources, partition redesign, production no-op/force, durable source-change
receipts and Iceberg maintenance are not implemented by this pilot.

## Alternatives

1. Drop raw columns in existing production tables now: rejected because reader
   compatibility, rollback pins and storage reclamation are not established.
2. Keep full raw JSON in a second permanent Silver archive: rejected for the pilot;
   it recreates duplication when immutable Bronze already supports retrieval.
3. Pilot a separate typed contract with lineage and compatibility projection:
   selected; permits behavior checks before any migration, at the cost of a
   temporary legacy-to-slim projection step and no mapper memory reduction yet.

## Verification and next boundary

258 targeted regression tests passed, including the new pilot tests and Gold
comparison of all seven fact contribution tables and nine dimensions on synthetic
fixtures. These check raw retrieval, corrupted source rejection, candidate-integrity
fence, normal reuse, force unchanged output, changed/empty children, CLI wiring and
rollback before commit.

One complete real source day (2022-09-18, one observation/result/lot/contractor) was
processed locally with zero quality issues. Repeat and force semantics were verified.
Identical zstd serialization of the six tables used 40,873 bytes for legacy versus
27,763 for slim (~32.1% lower); Arrow payload sizes were 16,053 versus 2,086 bytes.
These are tiny-sample measurements, not expected whole-warehouse savings. Real
Gold-input multiset comparison for four pilot input tables reported zero differences.
Full Gold business parity on real history and Iceberg DML remain unverified.

Next: broaden representative source cohorts under explicit budgets, include malformed
and high-fanout cases, then implement an isolated Iceberg publication contract with
revision-history/retraction and recovery receipts. Partition selection requires a
separate measured layout comparison; this change does not alter partition specs.
