# Ingestion job flow

## Range run

```text
start_date..end_date
        |
        v
create run_id + RunManifest(RUNNING)
        |
        v
split into source dates
        |
        +--> daily_run(date 1)
        +--> daily_run(date 2)
        +--> ...
        |
        v
RunManifest
  SUCCESS | PARTIAL_FAILED | FAILED
```

A failed date does not stop later dates in the same range.

## Daily attempt

```text
(run_id, source_date)
        |
        v
DayManifest(RUNNING)
        |
        v
search page
        |
        v
PageManifest(RUNNING)
        |
        v
fetch detail records
        |
   +----+----+
   |         |
 error      ok
   |         |
   v         v
_errors    Bronze Parquet
   |         |
   v         v
Page FAILED Page SUCCESS
   |         |
   v         v
Day FAILED  next page
             |
             v
       all pages complete
             |
             v
       Day SUCCESS (commit)
```

Any search/detail/Bronze-load ingestion error fails the whole source-date attempt. The engine stops that date and the range runner proceeds to the next requested date.

## Recovery

There is no record retry and no cross-run page resume.

```text
run A / source_date D -> FAILED
            |
            v
run the same date again
            |
            v
run B / source_date D -> starts from page 0
```

Old attempts stay immutable in storage. A failed attempt may contain partial Parquet files, but downstream must never treat file existence as commit state; only a successful `DayManifest` commits that attempt.

## Command

```powershell
python -m procurement.jobs.crawl_khlcnt `
  --start-date 2026-09-11 `
  --end-date 2026-09-11 `
  --page-size 50
```

Running the command again for the same date creates a new `run_id` and recrawls the full day.

## Ops API

```text
GET /api/ops/runs
GET /api/ops/runs/{source}/{resource}/{run_id}
GET /api/ops/errors
```

The API exposes persisted run/error facts only. Retry-resolution state no longer exists because recovery is represented by a separate later run.
