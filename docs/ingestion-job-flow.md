# Ingestion and recovery flow

## Daily ingestion

```text
closed source date D-1
        |
        v
   search pages
        |
        +--> save raw search snapshot
        |
        v
  resource extractor
     /       \
 success    record error
    |           |
    v           v
 Bronze      _errors
    |
    v
page checkpoint
    |
    v
manifest + _SUCCESS
```

A page checkpoint is written only after raw storage and Bronze load succeed. Search page checkpoints also store an ordered page fingerprint so resume fails loudly if the upstream result ordering drifts.

Fatal page/day failures (search, raw storage, Bronze load) fail the daily run. Isolated detail errors are persisted as immutable error events and let the daily crawl complete with `completed_with_errors`.

## Retry flow

```text
_errors (immutable)
        |
        v
filter retryable + unresolved
        |
        v
retry run (new retry_run_id)
      /   \
 success  fail
   |       |
 Bronze    +--> pending (attempts remaining)
   |       +--> dead_letter (final/non-retryable)
   v
_error_state = recovered
```

Retry never changes the original ingestion run or original error event. A recovered Bronze record includes `_recovered_from_error_id` for provenance.

Run status remains historical, while current health is derived:

- `healthy`: original run has no errors.
- `recovered`: original errors existed but all are recovered.
- `partial`: unresolved pending/dead-letter/non-retryable errors remain.
- `failed`: original daily run failed.

## Commands

```powershell
python -m procurement.jobs.crawl_khlcnt `
  --start-date 2026-09-11 `
  --end-date 2026-09-11 `
  --page-size 50

python -m procurement.jobs.retry_errors `
  --resource khlcnt `
  --source-date 2026-09-11 `
  --max-attempts 3
```

`--force` intentionally reprocesses a completed date. Bronze is at-least-once, so force may append duplicate observations; Silver must canonicalize them.

## Ops API

Install the optional API dependencies:

```bash
pip install -e ".[ops]"
```

Run:

```bash
uvicorn procurement.api.main:app --reload
```

Endpoints:

```text
GET /api/ops/runs
GET /api/ops/runs/{source}/{resource}/{source_date}/{run_id}
GET /api/ops/errors
```

`GET run` returns both the historical ingestion status and derived current health/recovery counters. No historical manifest is rewritten after retry.
