# Procurement Lakehouse architecture

## Mental model

The ingestion code follows one direction:

```text
Job -> Batch runner -> Daily engine -> Resource adapter/extractor -> Storage
                               \-> Retry engine -> Storage
Ops API -> Observability service/repositories -> Storage
```

### Ownership rules

1. `jobs/` only parses input and wires dependencies.
2. `ingestion/engine/` owns ingestion lifecycle, not source-specific endpoints.
3. `ingestion/sources/<source>/<resource>/` owns API/query/extraction/retry behavior for that resource.
4. Resource code never owns checkpoints, manifests, locks or physical storage layout.
5. `storage/` owns how/where data and state are persisted; it does not decide when ingestion steps run.
6. Bronze is append-only and at-least-once. Silver is responsible for canonical deduplication by source identity/version/content hash.
7. Original run manifests and error events are historical facts and are never rewritten after recovery.
8. Retry uses a new `retry_run_id`; mutable recovery state lives separately in `_error_state`.
9. `_SUCCESS` means the daily crawl lifecycle for a closed source date completed; it does not mean every record-level error has already recovered.
10. The Ops API derives current data health from the historical run + error events + current error resolutions.

## Terminology

- **Batch run**: one CLI execution over `start_date..end_date`; owns `run_id`.
- **Daily run**: one `(source, resource, source_date)` execution inside a batch run.
- **Page**: one source search page inside a daily run.
- **Record**: one append-only Bronze observation.
- **Error event**: immutable error captured during original ingestion.
- **Retry run**: separate execution that attempts to recover unresolved retryable error events.

## Storage planes

```text
Object storage
├── _raw_search/       # immutable source search snapshots
├── bronze/            # append-only Parquet observations
├── _control/          # page checkpoints, run manifests, locks, _SUCCESS
├── _errors/           # immutable error events
├── _error_state/      # mutable current resolution state per error_id
└── _retry_runs/       # immutable retry execution manifests
```

Engine decides **when** to save/checkpoint/retry. Storage decides **how and where**.

## Source-date invariant

Mua Sam Cong is crawled only after a day is closed (for example, after midnight on 12/09, crawl publicDate 11/09). The current ingestion design assumes a newly published version receives the new publication date, so versions re-enter a later daily batch naturally.

This invariant must be verified when adding/changing a resource. If a source later changes this behavior, the resource needs an updated incremental strategy rather than silently adding a rolling lookback.
