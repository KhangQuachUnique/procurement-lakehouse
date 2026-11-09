# ADR-005: End-to-End Daily Partition Alignment and Pilot Resource Isolation

## Status
Accepted

## Date
2026-11-09

## Context
1. **Partition Mismatch**: Bronze and Silver layers operated on daily partitions (`BRONZE_PARTITIONS`), while Gold facts were originally monthly partitioned. This prevented defining a unified end-to-end Dagster job where daily incremental runs could execute seamlessly from Bronze to Gold.
2. **Topological Disorder**: Due to missing upstream dependencies, static dimensions appeared at root level (Column 1) alongside Bronze, while lifecycle duration metrics appeared before demand planning facts.
3. **Pilot Leakage**: Experimental datasets (such as `contract_performance` contract execution data) were bundled into the primary daily pipeline, causing clutter and false alarm failures.

## Decision
1. **Align All Layers to Daily Partitions**:
   - Reconfigure all 7 Gold fact assets to use `partitions_def=BRONZE_PARTITIONS` and `BackfillPolicy.multi_run(max_partitions_per_run=1)`.
   - Implement idempotent partition overwrites (`DELETE WHERE report_date = target_date` followed by `INSERT`) in atomic transactions.
2. **Pragmatic Topological Sorting**:
   - Add explicit upstream dependencies from Silver anchor tables to the 9 Gold Dimensions, ensuring dimensions resolve at Depth 2 (Column 3).
   - Ensure all 7 Gold Facts depend on the dimensions stage, ensuring they all execute at Depth 3 (Column 4).
   - Remove non-existent schema elements (`gold_dim_bidding_type`).
3. **Core vs. Pilot Isolation**:
   - Keep `project` in the core pipeline (Bronze: `bronze_project`, Silver: `silver_project_*`, enriching `gold_dim_geography`).
   - Isolate `contract_performance` into dedicated groups (`bronze_pilot`, `silver_pilot`) and a separate on-demand job (`pilot_contract_performance_job`).

## Consequences
- The primary production job `lakehouse_end_to_end` contains exactly 33 core assets cleanly arranged in 4 sequential stages.
- Daily incremental execution is fully observable: nodes turn green sequentially from left to right.
- Rerunning any historical partition is 100% idempotent and safe from record duplication.
- Experimental modules remain runnable for testing without jeopardizing core production stability.
