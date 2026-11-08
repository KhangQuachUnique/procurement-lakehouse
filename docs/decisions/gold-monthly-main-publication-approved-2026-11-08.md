# Owner-requested publication of computed monthly Gold — 2026-11-08

Owner Khang Quach explicitly requested “nạp vô bản chính luôn đi, r mốt muốn đổi lại vãn ok mà đúng ko ?” after disclosure that all7facts/9dims were computed, schema/grain/FK and Parquet exact readback passed, but full-history independent business-SQL comparison failed at the1GB engine limit. This authorizes publication of these existing results into main; it does not make that unfinished comparison PASS or authorize source rewrites/cleanup/resource escalation.

Read-only REST preflight at06:57+07 found main namespace `gold_v2_main_2022_2025` and its `_control` namespace both without tables. No previous active main release exists. The cancelled full main job is not restarted. Source stays pinned to completed Silver2022–2025; observation day2026-11-07.

Use existing `IcebergPublisher`, exact published snapshot readback and existing manifest reader. Import outbox is all keys/rows from immutable sixteen Parquet files; separate publication checkpoint database, no trial-state modification. Activate only a complete manifest after exact checks. Each snapshot is tagged using the existing publisher. Main readers must use the manifest, not independently read table-current states during publication.

The owner was informed that first-release rollback means deactivating the newly published manifest to the previous *unpublished* baseline, not restoring a nonexistent old main release. Preserve all data, files and snapshots. Future version changes are reversible only while required snapshots/files and manifests are retained. No snapshot expiry or GC authorized. A guarded rollback command is included in the operational importer and tested locally; no destructive rollback is performed now.

Operational script: `/home/soul/.openclaw/workspace/.tmp/openclaw-spikes/gold-all-facts-monthly-20261107/publish_main.py`. Evidence: `data/gold-main-import-monthly-20261108/` (immutable plan/hashes, preflight, publication checkpoints, pinned snapshot manifest, exact readback, final receipt when successful).

Retain4GB whole-process, oneCPU, noSwap; local512MB + remote1GB; bounded8192-row transfer. Existing publisher still stages complete compact tables; a resource failure stops publication, not blind retries or automatic resource increase. Before activation, existing main remains unpublished.

Limitations: independent full-history business oracle remains NOT COMPLETED. This import does not manufacture incremental ledgers/source-index state from aggregate tables; do not resume the cancelled main state or enable incremental schedules against it. Future incremental integration needs an explicit state-alignment stage. Publication receipt status must distinguish exact storage/readback success from business-oracle acceptance.
