# Gold relationship-index / impact-local planner — 2026-11-07

User approved implementing the previously explained persisted-relationship,
affected-region strategy, then separately permitted Gold RAM up to 4GB. Interpret
the latter as the whole-process hard cap, not 4GB for each DuckDB connection.
Schema, business grain, correction/retraction semantics and acceptance gates remain.
No production migration, source rewrite or physical deletion is authorized here.

## Design / lifecycle

- `relation_cache.nodes/links` are narrow persisted SQL adjacency tables, indexed
  by node and reverse token. `meta(epoch,version)` binds them to the accepted public
  release (not an equivalent-source/no-op run ID).
- Missing/stale epoch rebuilds from accepted `input_cache` once. Never infer that
  absent adjacency means absent source data.
- Root differences stage replacement nodes/links in candidate `graph` tables.
  Observation and child changes also seed affected roots. Explore a deduplicated
  node/token frontier over BOTH accepted old links and replacement new links.
- Label and batch only the reached region. Separate token aggregation and node
  aggregation into durable intermediate tables rather than the former nested
  JOIN/GROUP BY statement. Whole connected components retain original labels,
  source-row charges and batch-row fail-closed bounds.
- Only after acknowledged activation, `PipelineBackend.accept` updates accepted
  index, input cache and contribution state inside the same existing transaction.
  Candidate planning, interrupted batches and acceptance rollback cannot publish
  the candidate index as accepted.
- This does NOT yet eliminate full-pin source capture or multiset-diff scans when
  no trusted change journal is present. It does not implement a new monthly source
  scheduler or manufacture monthly Silver receipts. The runtime can consume
  smaller attested increments; source chunking alone is not business grain.

## Evidence

- Preserved original runtime: `data/gold-v2-acceptance/planner-locality-v1/original-runtime.tar.gz`.
- Full old planner retained as independent test oracle in
  `tests/gold_v2/reference_planning.py` (only its import path changed).
- 121 Gold tests passed in 61.26 seconds: full pipeline, exact outputs, relation
  reparenting/deletion, child/observation changes, multiplicity, oversize rejection,
  accepted-index reuse, stale-epoch rebuild, rollback and restart tests.
  Log: `planner-locality-v1/regression.log`. Supplemental harness/contracts check:
  20 passed (overlaps existing contract tests; do NOT add counts as unique tests).
- Offline deterministic real-snapshot sample: 58,701 new-source rows; 17,880 old
  full graph nodes versus 13,748 reached nodes; exact impacted component labels,
  row charges and batch assignments match old planner. Reused-index replay matches.
  Process peak RSS 613,304 KiB; cgroup observed ~1.7GiB including file cache.
  `planner-locality-v1/real-sample.json`.
- No speedup claimed: reference 26.65s versus first-index 27.72s on this sample.
  It is a broad yearly change sample, not evidence for monthly/small-delta latency.
- The sample harness initially rejected parameterized ATTACH; fixed with an escaped
  path literal, preserved failed marker/log, then the bounded trial passed. A later
  attempted SIGINT found the successful transient unit already collected; no
  running trial was killed and its PASS was preserved. Do not repeat the benchmark.

## Real acceptance continuation / resources

Changing runtime code changes `logic_version`; never mutate/resume an old immutable
plan under a new hash. Preserve previous accepted 2022 state and failed 2023 state.
New isolated target/state uses the SAME two retained source receipts, not rewritten
Silver or a new four-year load.

Unit: `procurement-gold-v2-locality-acceptance-20261107.service`.
Evidence: `data/gold-v2-acceptance/two-year-locality-20261107-v2/`.
Scope: 2022-01-01 through 2023-12-28, prefix through 2022-12-31; last three days of
2023 excluded due to shared batch crossing into 2024. No whole-2023/2025 PASS claim.
Gold engine 2GB; reader 512MB; one CPU/thread; total hard cap 4,000,000,000 bytes,
MemoryHigh 3,500,000,000 bytes; swap cap 256MiB. Runtime cap 3,074 seconds, reduced
from the earlier 60-minute stage to account for previous execution/sample work.
On invariant, coverage or resource failure: preserve evidence, no automatic retry,
no further memory increase, no gate waiver. Watcher 93e05917 reports terminal once.

Both Silver bulk secondary writer and supervisor intentionally remain stopped.
Primary Silver also failed independently at 2024-10-16 (3,057 completed cohorts)
with IOException at 12:55:18; it was not stopped by this planner work. Do not
describe it as running or silently restart it. Retained source pins remain valid.

Status: Implemented; verified on automated tests and bounded real planner sample.
Real end-to-end acceptance is RUNNING, not verified or user-accepted yet. No migration.
