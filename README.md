# Procurement Lakehouse

Data Lakehouse phục vụ thu thập và phân tích dữ liệu đấu thầu công từ **Hệ thống mạng đấu thầu quốc gia (Mua Sắm Công)**.

Project hiện tập trung vào **Bronze ingestion**: crawl dữ liệu theo ngày, giữ nguyên payload từ nguồn, bổ sung metadata/lineage, lưu xuống object storage và quản lý trạng thái run/error để có thể kiểm tra và chạy lại an toàn.

## Tổng quan kiến trúc

```text
MuaSamCong API
      |
      v
Resource Adapter
(project / khlcnt / notify_contractor / contractor_result)
      |
      v
Shared Ingestion Engine
      |
      +----> Bronze Parquet
      +----> Run / Day / Page Manifest
      +----> Error Records
      |
      v
S3-compatible Object Storage (SeaweedFS)
```

Luồng ingestion được tổ chức theo:

```text
run_id
└── source_date
    └── page
```

Mỗi `source_date` là một attempt độc lập. Nếu một ngày crawl lỗi thì ngày đó được đánh dấu `FAILED`; recovery được thực hiện bằng một `run_id` mới và crawl lại toàn bộ ngày đó từ page 0.

## Resource hiện có

### KHLCNT

Thu thập **Kế hoạch lựa chọn nhà thầu** và detail của các gói thầu liên quan.

```text
Search KHLCNT
   -> Plan detail
      -> Bid package detail
```

Bronze tables:

```text
khlcnt_plan_detail
khlcnt_bid_package_detail
```

### Project

Thu thập **Dự án** theo `publicDate`.

```text
Search Project
   -> Project detail
```

Bronze table:

```text
project_detail
```

Project detail được giữ nguyên payload, bao gồm các liên kết như `linkedPublishPlan` để Silver layer sau này có thể xây dựng quan hệ.

### Notify Contractor

Thu thập nhóm thông báo thuộc `es-notify-contractor` theo `publicDate`. Detail được route theo workflow hiện tại của portal:

```text
notify-contractor-* -> lcnt_tbmt_ttc_ldt
reoffer-price-*     -> online-reoffer/detail
```

Bronze tables:

```text
notify_contractor_standard_detail
notify_contractor_reoffer_detail
```

### Contractor Result

Thu thập **Kết quả lựa chọn nhà thầu (KQLCNT)** theo `publicDateKqlcnt` và `stepCode=notify-contractor-step-4-kqlcnt`.

```text
Search KQLCNT
   -> inputResultId
      -> contractor-input-result/get
```

Bronze table:

```text
contractor_result_detail
```

Bronze giữ nguyên response gồm kết quả, lot, contractor trúng và thông tin gói/KHLCNT nhúng. Silver layer sau này mới chuẩn hóa và liên kết:

```text
Project
   -> KHLCNT
      -> Bid Package
         -> Notify Contractor
            -> Contractor Result
```

> Hiện tại scope chính của repo là Bronze. Chuẩn hóa business schema, deduplication, entity relationship và analytical model sẽ được xử lý ở Silver/Gold sau.

## Error handling

Error record chỉ giữ context cần để xác định lỗi xảy ra ở đâu (`source`, `resource`, `stage`, `run_id`, `source_date`, `page_number`, `source_id`) và diagnostic gốc (`error_type`, `message`, `http_status`). Project không dùng error-code taxonomy; exception message được giữ gần nguyên bản và chỉ redact credential phổ biến như token/authorization trước khi persist.

## Yêu cầu

- Python 3.12
- Docker + Docker Compose
- MuaSamCong token

## Cài đặt

### Windows PowerShell

```powershell
git clone <repository-url>
cd procurement-lakehouse

python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev,ops]"

Copy-Item .env.example .env
```

### Linux / macOS

```bash
git clone <repository-url>
cd procurement-lakehouse

python3.12 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev,ops]"

cp .env.example .env
```

Sau đó cập nhật `MUASAMCONG_TOKEN` trong `.env`.

## Chạy object storage

```powershell
cd infra/docker
docker compose --env-file ../../.env up -d
cd ../..
```

SeaweedFS S3 API mặc định chạy tại:

```text
http://localhost:8333
```

## Chạy test

```powershell
pytest
```

## Crawl dữ liệu

Chỉ crawl các ngày đã đóng (`end-date` phải nhỏ hơn ngày hiện tại theo timezone Việt Nam).

### Crawl Project

PowerShell:

```powershell
python -m procurement.jobs.crawl_project --start-date 2026-09-01 --end-date 2026-09-01 --page-size 50
```

Hoặc xuống dòng trong PowerShell bằng backtick:

```powershell
python -m procurement.jobs.crawl_project `
  --start-date 2026-09-01 `
  --end-date 2026-09-01 `
  --page-size 50
```

Bash:

```bash
python -m procurement.jobs.crawl_project \
  --start-date 2026-09-01 \
  --end-date 2026-09-01 \
  --page-size 50
```

### Crawl KHLCNT

```powershell
python -m procurement.jobs.crawl_khlcnt --start-date 2026-09-01 --end-date 2026-09-01 --page-size 50
```

### Crawl Notify Contractor

```powershell
python -m procurement.jobs.crawl_notify_contractor --start-date 2026-09-01 --end-date 2026-09-01 --page-size 50
```

### Crawl Contractor Result

```powershell
python -m procurement.jobs.crawl_contractor_result --start-date 2026-09-01 --end-date 2026-09-01 --page-size 50
```

## Ops API

```powershell
uvicorn procurement.api.main:app --reload
```

Các endpoint chính:

```text
GET /api/ops/runs
GET /api/ops/runs/{source}/{resource}/{run_id}
GET /api/ops/errors
```

Ví dụ:

```text
/api/ops/runs?source=muasamcong&resource=project
/api/ops/errors?source=muasamcong&resource=khlcnt
```

## Cấu trúc chính

```text
src/procurement/
├── api/
├── common/
├── ingestion/
│   ├── engine/
│   └── sources/
│       └── muasamcong/
│           ├── contractor_result/
│           ├── khlcnt/
│           ├── notify_contractor/
│           └── project/
├── jobs/
├── models/
├── observability/
└── storage/
```

Chi tiết kiến trúc xem thêm tại `docs/architecture.md`.
