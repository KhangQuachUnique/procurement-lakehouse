# Procurement Lakehouse architecture

## Ingestion mental model

```text
Job -> Range runner -> Daily runner -> ResourceSpec/source adapter -> Bronze
                         |                         |
                         +-------------------------+-> Control + Errors
```

Scope hiện tại dừng ở Bronze. Silver/Gold sẽ xử lý canonical entity, deduplication, history và analytical model sau.

## Ownership

1. `jobs/`: CLI và dependency wiring.
2. `ingestion/batch_runner.py`: một range run, `run_id`, date splitting, aggregate status.
3. `ingestion/engine/daily_runner.py`: lifecycle của một `(run_id, source_date)` attempt.
4. `ingestion/sources/<source>/<resource>/`: endpoint, search contract và source-specific detail extraction.
5. `models/`: persisted contracts.
6. `storage/`: object-store layout/serialization.
7. Bronze append-only, giữ source payload trong typed lineage envelope.

## Commit boundary

```text
run_id
└── source_date
    └── page
```

Mỗi `(run_id, source_date)` là một attempt độc lập.

- Có bất kỳ search/detail/load error -> day `FAILED`.
- Recovery luôn tạo `run_id` mới và crawl lại từ page 0.
- Không resume page giữa hai run.
- `DayManifest.status == success` mới là commit marker.
- Parquet tồn tại trong một failed attempt không có nghĩa là data đã commit.

## DLT attempt isolation

DLT pipeline state được tách theo từng attempt:

```text
<base_pipeline>_<resource>_<YYYYMMDD>_<run_id>
```

Ví dụ:

```text
muasamcong_bronze_khlcnt_20260901_a83f...
muasamcong_bronze_khlcnt_20260901_b91c...
```

Điều này ngăn pending load/state của failed run cũ bị dùng lại trong retry run mới. Dataset và Bronze table name không thay đổi; chỉ DLT working state được isolate.

## Pagination invariants

Crawler không chỉ tin `totalElements`. Mỗi search page phải nhất quán với request:

- `totalElements` không được thay đổi giữa các page.
- Nếu source trả page number metadata thì phải đúng page được request.
- Nếu source trả page size metadata thì phải đúng `page_size`.
- Nếu source trả `totalPages` thì phải khớp với `ceil(totalElements / page_size)`.
- Số item của mỗi page phải đúng số item mong đợi theo `totalElements` và `page_size`.
- Search window chạm giới hạn 10.000 items thì attempt fail để tránh silent truncation.

Mismatch được ghi với stage `pagination` và day fail.

## Concurrency

Không dùng custom daily lock.

Hai run có thể crawl cùng `resource + source_date` mà không ghi đè nhau vì:

- Bronze partition có `run_id`.
- Control/error path có `run_id`.
- DLT state có `run_id`.

Chạy trùng chỉ làm tăng request và tạo nhiều observations. Silver/downstream sau này phải chọn successful attempt theo policy của nó.

Nếu sau này chạy multi-worker orchestration và cần single-flight thật, concurrency nên được enforce bởi orchestrator hoặc một atomic lock store, không dùng read-then-write JSON lock.

## Storage layout

```text
Object storage
├── bronze/
│   └── <table>/source_date=YYYY-MM-DD/run_id=<run_id>/*.parquet
│
├── _control/
│   └── <source>/<resource>/run_id=<run_id>/
│       ├── run.json
│       └── source_date=YYYY-MM-DD/
│           ├── day.json
│           └── pages/page-000000.json
│
└── _errors/
    └── <source>/<resource>/run_id=<run_id>/
        └── source_date=YYYY-MM-DD/page-000000.jsonl
```

## Bronze contract

```text
source_id
source_version
run_id
source_date
ingested_at
content_hash
payload
```

`source_version` nullable và chỉ dùng khi source thực sự version entity đó. Business normalization thuộc Silver.
