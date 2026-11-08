# Bounded, resumable link staging — 2026-11-07

User approved fixing the failed link-creation step, then supplied research on
partitioning, external processing, incremental computation, materialization and
query optimization. Apply key partitioning + durable materialization + query/
structure changes, with SSD spill as support and the prior impact-local semantics
preserved. Do not change grain, business rules, recovery gates or the 4GB total cap.

## Diagnosis correction

The previous integrated v2 run failed inserting candidate links at engine 2GB,
before impact closure. A bounded isolated measurement of that SAME source scope
completed at 2GB: 1,375,707 changed nodes, 5,078,584 links, 67.55 seconds. Therefore
the statement/index alone does NOT invariably exhaust memory. Total memory context
and transaction lifetime matter; exact attribution between index, diffs and other
intermediates remains unmeasured. Do not claim the ART index is the sole proven cause.
Measurement: `data/gold-v2-acceptance/planner-memory-v2/operator-before.json`.

## Implementation

- Source diffs materialize one table per transaction in private `relation_stage`.
  The immutable run identity binds staging to frozen inputs and accepted base.
- Link generation uses non-overlapping ordered keyset chunks, at most 25,000 node
  keys each. All versions and all alias families of each node stay together.
  DISTINCT(node,token) across all aliases in the chunk preserves old edge-set
  semantics. Do NOT deduplicate source revisions or business contribution rows.
- Candidate/accepted adjacency heap tables avoid large ART/PRIMARY KEY structures.
  Node disjointness, per-chunk DISTINCT, replacement of all prior links of changed
  nodes and atomic data+cursor writes preserve uniqueness by construction.
- Each link chunk and its cursor commit atomically. A failure before cursor commit
  rolls back the chunk; interruption after commit resumes after the saved key.
  No completed chunk is blindly appended again.
- Planner consumes only a COMPLETE stage with matching identity. Different plans
  cannot replace unfinished staging. A new run can discard old stage only after
  its owner run is COMMITTED. Staging is private, not accepted Gold or a public mart.
- Accepted source cache, adjacency epoch and contribution state still change only
  together in the existing post-activation accept transaction. Closure still sees
  old and new edges across ALL chunks; chunks do not split business components.
- No monthly source scheduler or fabricated monthly receipt was added. Source
  capture/multiset diff can still scan full pins without a trusted journal. Key
  count is not a universal byte bound: extreme versions/key or huge connected
  groups can still hit resource guards. Data skew is not yet a demonstrated cause.

## Verification and exact failed-scope trial

- Gold regression: 123 tests passed in 69.08 seconds. Three focused stage tests
  passed in 1.66 seconds, including a subsequently added failure-between-data-and-
  cursor rollback case; counts overlap, do not report 126 unique tests.
- Preserve original planner as test oracle. Tests cover old/new bridge removal,
  source/child/observation edits, alias collisions, all versions per key, exact
  components/charges/batches, interrupted staging, cross-plan rejection and replay.
- Real source trial: 57 chunks, 1,375,707 nodes, 5,078,584 links. Exact multiset
  mismatch counts versus old isolated operator: zero for nodes and links.
  Interruption after first durable chunk then resume produced no duplicates.
  Elapsed 131.43 seconds, peak RSS 2,270,684 KiB; systemd recorded rounded 3GiB
  cgroup peak including file cache, below the 4,000,000,000-byte cap.
- Trial only: no Gold publication, no original source/state modifications.
  `planner-memory-v2/operator-after.json`, `operator-after.log`, `regression.log`.
- Do NOT infer a speedup: old operator trial and new staging/recovery/parity trial
  have different work envelopes. The acceptance target is correctness + bounded
  resources + recovery, not an unproven faster benchmark.
- Ruff checks passed. Old runtime archive preserved with reconstructed logic hash
  `3b2d3822fc9e8c5e82a94670c5ba0d291ea22d726dcb18a54a8e14059a5da0d1`.

## Next authorized run

After the operator gate passed, started isolated real Gold acceptance:
`procurement-gold-v2-chunked-acceptance-20261107.service`, output
`data/gold-v2-acceptance/two-year-chunked-20261107-v3/`.
Same retained source bundles, 2022-01-01 through 2023-12-28; prefix 2022-12-31.
Keep older accepted and failed targets unchanged because logic_version changed.
Runtime hash `ca4e2cb7360bf28b7758d0eac1a039418460434c18eca0fb704c357a52274e0c`.
Engine 2GB, reader 512MB, one CPU, MemoryMax exactly 4,000,000,000 bytes,
MemoryHigh 3,500,000,000 bytes, swap 256MiB, runtime cap 2400 seconds.
No automatic retry, RAM escalation, Silver restart or gate waiver on failure.

Implemented and verified at helper/operator level. End-to-end Gold remains RUNNING
until its actual receipt passes; not user-accepted or migrated. No production writes.
