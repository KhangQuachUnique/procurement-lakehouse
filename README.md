# Procurement Lakehouse

Data Lakehouse phục vụ thu thập và phân tích dữ liệu đấu thầu công từ **Hệ thống mạng đấu thầu quốc gia (Mua Sắm Công)**.

Project hiện tập trung vào **Bronze ingestion**: crawl dữ liệu theo ngày, giữ nguyên payload từ nguồn, bổ sung metadata/lineage, lưu xuống object storage và quản lý trạng thái run/error để có thể kiểm tra và chạy lại an toàn.

## Tổng quan kiến trúc

```text
MuaSamCong API
      |
      v
Resource Adapter
(project / khlcnt / ...)
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

Project detail được giữ nguyên payload, bao gồm các liên kết như `linkedPublishPlan` để Silver layer sau này có thể xây dựng quan hệ:

```text
Project
   -> KHLCNT
      -> Bid Package
         -> TBMT / KQLCNT
```

> Hiện tại scope chính của repo là Bronze. Chuẩn hóa business schema, deduplication, entity relationship và analytical model sẽ được xử lý ở Silver/Gold sau.

---

# Chạy project local

## 1. Yêu cầu

- Python **3.12**
- Docker + Docker Compose
- Git
- Token hợp lệ của `muasamcong.mpi.gov.vn`

Clone repo và chuyển sang branch ingestion:

```bash
git clone https://github.com/KhangQuachUnique/procurement-lakehouse.git
cd procurement-lakehouse
git switch feature/ingestion
```

## 2. Tạo virtual environment

### Windows PowerShell

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### macOS / Linux

```bash
python3.12 -m venv .venv
source .venv/bin/activate
```

## 3. Cài dependencies

Cài ingestion + test:

```bash
pip install -e ".[dev]"
```

Nếu muốn chạy luôn Ops API:

```bash
pip install -e ".[dev,ops]"
```

## 4. Cấu hình environment

Tạo `.env` từ file mẫu.

### Windows

```powershell
Copy-Item .env.example .env
```

### macOS / Linux

```bash
cp .env.example .env
```

Sau đó sửa `.env`:

```env
APP_ENV=dev
LOG_LEVEL=INFO

MUASAMCONG_BASE_URL=https://muasamcong.mpi.gov.vn
MUASAMCONG_TIMEOUT_SECONDS=30
MUASAMCONG_TOKEN=<YOUR_VALID_TOKEN>

OBJECT_STORAGE_IMAGE=chrislusf/seaweedfs:4.46
OBJECT_STORAGE_ENDPOINT=http://localhost:8333
OBJECT_STORAGE_ACCESS_KEY=procurement_admin
OBJECT_STORAGE_SECRET_KEY=change_this_to_a_long_random_password
OBJECT_STORAGE_BUCKET=procurement-lakehouse
OBJECT_STORAGE_API_PORT=8333
```

Không commit token thật hoặc secret lên Git.

## 5. Khởi động object storage

Từ thư mục root của project:

```bash
docker compose --env-file .env -f infra/docker/compose.yaml up -d
```

Kiểm tra container:

```bash
docker compose --env-file .env -f infra/docker/compose.yaml ps
```

Dừng infra:

```bash
docker compose --env-file .env -f infra/docker/compose.yaml down
```

SeaweedFS cung cấp S3-compatible endpoint mặc định tại:

```text
http://localhost:8333
```

## 6. Chạy test

```bash
pytest
```

Có thể chạy lint:

```bash
ruff check .
```

## 7. Crawl dữ liệu

Ingestion chỉ cho phép crawl **closed source date**, tức là ngày đã kết thúc. Không crawl ngày hiện tại hoặc ngày tương lai.

### Crawl Project

Ví dụ crawl ngày `2026-09-01`:

```bash
python -m procurement.jobs.crawl_project \
  --start-date 2026-09-01 \
  --end-date 2026-09-01 \
  --page-size 50
```

Crawl một khoảng ngày:

```bash
python -m procurement.jobs.crawl_project \
  --start-date 2026-09-01 \
  --end-date 2026-09-05 \
  --page-size 50
```

### Crawl KHLCNT

```bash
python -m procurement.jobs.crawl_khlcnt \
  --start-date 2026-09-01 \
  --end-date 2026-09-05 \
  --page-size 50
```

> Với PowerShell, có thể viết command trên một dòng hoặc dùng backtick `` ` `` thay cho `\` để xuống dòng.

## 8. Chạy Ops API (optional)

Nếu đã cài dependencies `ops`:

```bash
uvicorn procurement.api.main:app --reload
```

Mở Swagger UI:

```text
http://127.0.0.1:8000/docs
```

Một số endpoint hiện có:

```text
GET /api/ops/runs
GET /api/ops/runs/{source}/{resource}/{run_id}
GET /api/ops/errors
```

Ví dụ xem run của resource Project:

```text
GET /api/ops/runs?source=muasamcong&resource=project
```

---

# Bronze storage

Dữ liệu business được giữ trong `payload`; metadata ingestion được lưu bằng schema ổn định:

```text
source_id
source_version
run_id
source_date
ingested_at
content_hash
payload
```

Layout tổng quát:

```text
Object Storage
├── bronze/
│   └── <dataset>/<table>/
│       └── source_date=YYYY-MM-DD/
│           └── run_id=<run_id>/
│               └── *.parquet
│
├── _control/
│   └── <source>/<resource>/...
│
└── _errors/
    └── <source>/<resource>/...
```

Bronze là append-only. `DayManifest.status == SUCCESS` mới được xem là commit marker cho một attempt hợp lệ; chỉ có file Parquet tồn tại chưa đủ để kết luận ingestion thành công.

## Cấu trúc source chính

```text
src/procurement/
├── common/          # settings, logging, error types
├── ingestion/
│   ├── engine/      # shared ingestion engine
│   └── sources/
│       └── muasamcong/
│           ├── client.py
│           ├── khlcnt/
│           └── project/
├── jobs/            # CLI crawl jobs
├── models/          # persisted contracts
├── observability/   # run/error query services
├── storage/         # object storage + manifests + errors
└── api/             # Ops API
```

## Trạng thái hiện tại

- [x] Bronze ingestion engine
- [x] Pagination theo ngày
- [x] Run / Day / Page manifests
- [x] Error tracking
- [x] KHLCNT ingestion
- [x] Project ingestion
- [x] S3-compatible object storage
- [x] Ops API cơ bản
- [ ] TBMT ingestion
- [ ] KQLCNT ingestion
- [ ] Silver normalization
- [ ] Entity relationship / history tracking
- [ ] Gold analytical models

---

## Mục tiêu tiếp theo

Mở rộng nguồn dữ liệu và xây dựng pipeline hoàn chỉnh:

```text
Raw Public Procurement Data
          |
          v
       Bronze
          |
          v
       Silver
(clean + normalize + entity/history)
          |
          v
        Gold
(analytics / warehouse / serving)
```
