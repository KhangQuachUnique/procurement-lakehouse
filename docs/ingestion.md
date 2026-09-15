# Ingestion

## Nguyên tắc

Bronze crawl theo ngày đã đóng.

```text
range run
  -> source_date
      -> search pages
          -> detail records
              -> Bronze
```

Một lỗi ở search, pagination, detail hoặc Bronze load làm toàn bộ `source_date` attempt `FAILED`.

Retry không sửa run cũ:

```text
run A / 2026-09-01 -> FAILED
run B / 2026-09-01 -> crawl lại từ page 0
```

Chỉ `DayManifest.status=success` được xem là committed attempt.

## Resources

### Project

Search theo `publicDate`, fetch project detail.

Bronze:

```text
project_detail
```

### KHLCNT

Search KHLCNT theo `publicDate`, fetch plan detail và bid-package detail.

Bronze:

```text
khlcnt_plan_detail
khlcnt_bid_package_detail
```

### Notify Contractor

Search `es-notify-contractor` theo `publicDate`.

```text
notify-contractor-* -> lcnt_tbmt_ttc_ldt
reoffer-price-*     -> online-reoffer/detail
```

Unknown workflow không fallback; attempt fail với stage `detail_routing`.

Bronze:

```text
notify_contractor_standard_detail
notify_contractor_reoffer_detail
```

### Contractor Result

Search KQLCNT theo:

```text
publicDateKqlcnt
type = es-notify-contractor
stepCode = notify-contractor-step-4-kqlcnt
```

Detail được gọi bằng `inputResultId`.

Bronze:

```text
contractor_result_detail
```

## Chạy ingestion

Chỉ crawl ngày đã đóng (`end-date` phải nhỏ hơn ngày hiện tại theo timezone Việt Nam).

### Project

```powershell
python -m procurement.jobs.crawl_project `
  --start-date 2026-09-01 `
  --end-date 2026-09-01 `
  --page-size 50
```

### KHLCNT

```powershell
python -m procurement.jobs.crawl_khlcnt `
  --start-date 2026-09-01 `
  --end-date 2026-09-01 `
  --page-size 50
```

### Notify Contractor

```powershell
python -m procurement.jobs.crawl_notify_contractor `
  --start-date 2026-09-01 `
  --end-date 2026-09-01 `
  --page-size 50
```

### Contractor Result

```powershell
python -m procurement.jobs.crawl_contractor_result `
  --start-date 2026-09-01 `
  --end-date 2026-09-01 `
  --page-size 50
```

## Output

Bronze:

```text
bronze/<table>/source_date=YYYY-MM-DD/run_id=<run_id>/*.parquet
```

Control:

```text
_control/<source>/<resource>/run_id=<run_id>/
```

Errors:

```text
_errors/<source>/<resource>/run_id=<run_id>/source_date=YYYY-MM-DD/
```

Error record giữ context (`resource`, `stage`, `source_date`, `page_number`, `source_id`) và diagnostic thật (`error_type`, `message`, `http_status`). Credential phổ biến trong message được redact trước khi persist.

## Pagination safety

Nếu source trả pagination không nhất quán, attempt fail thay vì tiếp tục với data có nguy cơ thiếu:

- page number sai
- page size bị clamp
- `totalElements` đổi giữa các page
- `totalPages` không khớp
- page thiếu/thừa item
- search chạm giới hạn 10.000 item

Các lỗi này dùng stage:

```text
pagination
```

## Ops API

```powershell
uvicorn procurement.api.main:app --reload
```

```text
GET /api/ops/runs
GET /api/ops/runs/{source}/{resource}/{run_id}
GET /api/ops/errors
```

Ví dụ:

```text
/api/ops/runs?source=muasamcong&resource=project
/api/ops/errors?source=muasamcong&resource=contractor_result
```
