# Procurement Lakehouse architecture

## Ingestion mental model

```text
Job -> Range runner -> Daily runner -> ResourceSpec/source adapter -> Bronze
                         |                         |
                         +-------------------------+-> Control + Errors
```

The current scope ends at Bronze. Silver/Gold consumption and active-snapshot selection are intentionally not part of this ingestion refactor yet.

## Ownership rules

1. `jobs/` parses CLI input and wires dependencies.
2. `ingestion/batch_runner.py` owns the range run: one `run_id`, date splitting, and aggregate run status.
3. `ingestion/engine/daily_runner.py` owns the lifecycle of one `(run_id, source_date)` attempt.
4. `ingestion/sources/<source>/<resource>/` owns source endpoints and source-specific extraction.
5. `models/` owns persisted data contracts (`RunManifest`, `DayManifest`, `PageManifest`, `ErrorRecord`, `BronzeRecord`).
6. `storage/` owns physical object-store layout and serialization only.
7. Bronze is append-only and preserves the source payload inside a typed lineage envelope.
8. Any ingestion error makes the whole source-date attempt `FAILED`.
9. Recovery is a new `run_id` that crawls the entire source date again from page 0. Data/pages are never mixed across attempts.
10. `DayManifest.status == success` is the commit marker for a Bronze day attempt. Existing Parquet files alone do not mean the attempt is valid.

## Core hierarchy

```text
run_id
└── source_date
    └── page
```

A range run may contain successful and failed dates independently. For example:

```text
run A
├── 2026-09-01 SUCCESS
├── 2026-09-02 FAILED
└── 2026-09-03 SUCCESS

Run A => PARTIAL_FAILED
```

The successful dates remain valid attempts even though the overall range run is partial.

## Storage layout

```text
Object storage
├── bronze/
│   └── <dataset>/<table>/source_date=YYYY-MM-DD/run_id=<run_id>/*.parquet
│
├── _control/
│   └── <source>/<resource>/
│       ├── run_id=<run_id>/
│       │   ├── run.json
│       │   └── source_date=YYYY-MM-DD/
│       │       ├── day.json
│       │       └── pages/page-000000.json
│       └── _locks/source_date=YYYY-MM-DD.json
│
└── _errors/
    └── <source>/<resource>/run_id=<run_id>/
        └── source_date=YYYY-MM-DD/page-000000.jsonl
```

There is no separate raw-search/raw-detail layer in the current design. Bronze is the retained raw-ish data layer: source business data stays inside `payload`, while system lineage fields are typed and stable.

## Bronze contract

Each Bronze row contains:

```text
source_id
source_version (nullable; only if the source versions that exact entity)
run_id
source_date
ingested_at
content_hash
payload
```

Source payload schemas stay flexible in Bronze. Business normalization, canonical schemas and deduplication belong to Silver later.
