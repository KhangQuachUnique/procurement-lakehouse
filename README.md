# Procurement Lakehouse

Data Lakehouse phục vụ thu thập và phân tích dữ liệu đấu thầu công từ Hệ thống mạng đấu thầu quốc gia (Mua Sắm Công).

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

## Kiểm tra project

```powershell
pytest
ruff check .
```

## Tài liệu

- [Kiến trúc](docs/architecture.md)
- [Ingestion](docs/ingestion.md)
- [Ops](docs/ops.md)
