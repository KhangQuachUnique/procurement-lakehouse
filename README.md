# Procurement Lakehouse

Data Lakehouse phục vụ thu thập và phân tích dữ liệu đấu thầu công từ Hệ thống mạng đấu thầu quốc gia (Mua Sắm Công).

## Yêu cầu

- Python 3.12 - 3.14
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

python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev,ops]"

cp .env.example .env
```

Cập nhật `MUASAMCONG_TOKEN` và các biến object storage trong `.env` nếu cần.

## Chạy object storage

```powershell
cd infra/docker
docker compose --env-file ../../.env up -d
cd ../..
```

SeaweedFS S3 API mặc định:

```text
http://localhost:8333
```

## Backfill ingestion

Chạy đủ 4 resource với tối đa 2 worker process.

Nguyên năm:

```powershell
python -m procurement.jobs.crawl_all `
  --year 2022 `
  --page-size 50
```

Theo khoảng ngày:

```powershell
python -m procurement.jobs.crawl_all `
  --start-date 2025-03-01 `
  --end-date 2025-03-31 `
  --page-size 50
```

Chỉ crawl ngày đã đóng. Hướng dẫn chi tiết, semantics retry/commit và cách chạy từng resource nằm tại [docs/ingestion.md](docs/ingestion.md).

## Xem dữ liệu Bronze

Không cần tự cấu hình DuckDB/S3. Khi SeaweedFS đang chạy và `.env` đã đúng, chạy:

```powershell
python -m procurement.tools.bronze_explorer
```

Browser sẽ mở DuckDB UI tại `http://localhost:4213`. Các table Bronze được expose tự động dưới schema `bronze_raw`, ví dụ:

```sql
SELECT *
FROM bronze_raw.notify_contractor_standard_detail
LIMIT 100;
```

`bronze_raw` là dữ liệu vật lý để inspect; failed attempt có thể để lại partial Parquet. Trạng thái committed vẫn dựa trên `DayManifest SUCCESS` và xem qua Ops. Chi tiết tại [docs/bronze-explorer.md](docs/bronze-explorer.md).

## Kiểm tra project

```powershell
pytest
ruff check .
```

## Tài liệu

- [Kiến trúc](docs/architecture.md)
- [Ingestion](docs/ingestion.md)
- [Bronze Explorer](docs/bronze-explorer.md)
- [Ops](docs/ops.md)
