# Procurement Lakehouse

> Core refactor đã được tích hợp vào `main`: xem [thiết kế core và bản đồ tổ chức code](docs/refactor/core-architecture.md). Core metadata lưu trên PostgreSQL (`bronze_meta`).

Hệ thống thu thập, kiểm định và lưu trữ dữ liệu Mua Sắm Công vào **Bronze Parquet** và **Apache Iceberg (Format v2)**, quản lý metadata giao dịch bằng PostgreSQL (`bronze_meta`), điều phối bằng **Dagster** và giám sát qua **Ops UI**.

| Cần làm gì?                                                            | Hướng dẫn                                         |
| ---------------------------------------------------------------------- | ------------------------------------------------- |
| Cấu hình, crawl/recovery, Ops, query, quality, profile, chuyển dữ liệu | [Vận hành](docs/operations.md)                    |
| Hiểu bảng/identity, validation, mô hình Silver và Gold                 | [Kiến trúc dữ liệu](docs/architecture.md)         |
| Chạy tests, lint, build và thêm resource                               | [Công cụ phát triển](docs/development.md)         |
| Dagster, partition backfill và chuyển scheduler                        | [Orchestration](docs/orchestration.md)            |
| Đánh giá và tiến độ modernization                                      | [Đánh giá kế hoạch](docs/modernization_review.md) |

---

## 🚀 Khởi chạy nhanh (Turnkey Docker - Mặc định)

Toàn bộ hệ thống (PostgreSQL metadata, SeaweedFS S3 Storage & Iceberg REST Catalog, Dagster Orchestrator, Dagster Web UI, Ops Dashboard) được đóng gói và vận hành qua Docker Compose chuẩn.

### 1. Chuẩn bị môi trường

Tạo file cấu hình `.env` từ file mẫu:

- **Linux / macOS (Bash / Zsh):**
  ```bash
  cp .env.example .env
  ```
- **Windows (PowerShell):**
  ```powershell
  if (-not (Test-Path -LiteralPath .env)) { Copy-Item .env.example .env }
  ```

_(Nếu cần cào dữ liệu mới trực tiếp từ cổng Mua Sắm Công quốc gia, hãy cập nhật `MUASAMCONG_TOKEN` trong `.env`)_.

### 2. Khởi chạy toàn bộ hệ thống bằng Docker Compose

```bash
docker compose up -d
```

*(Docker Compose sẽ tự động đọc file `.env` và `compose.yaml` ở thư mục gốc để khởi chạy tất cả 7 dịch vụ).*

### 3. Khởi tạo Schema Metadata (Chỉ chạy 1 lần khi dựng mới)

Khởi tạo các bảng metadata PostgreSQL (`bronze_meta`):

```bash
uv run alembic upgrade head
```

### 4. Dừng hoặc quản lý hệ thống

- **Xem trạng thái các containers:**
  ```bash
  docker compose ps
  ```
- **Xem logs theo thời gian thực:**
  ```bash
  docker compose logs -f
  ```
- **Dừng toàn bộ hệ thống:**
  ```bash
  docker compose down
  ```
- **Xóa sạch containers và toàn bộ dữ liệu volumes (Dựng lại từ đầu):**
  ```bash
  docker compose down -v
  ```

---

## 🌐 Các cổng dịch vụ & Giao diện quản trị

Sau khi khởi chạy, các dịch vụ sẵn sàng tại:

- **Dagster Web UI**: [http://localhost:3000](http://localhost:3000) (Điều phối Asset, trigger pipeline, xem log jobs)
- **Ops Web UI & REST API**: [http://localhost:8000/ops](http://localhost:8000/ops) (Heatmap giám sát nghiệp vụ thầu)
- **SeaweedFS S3 Storage**: [http://localhost:8333](http://localhost:8333) (S3-compatible Object Storage chứa Bronze Parquet)
- **Apache Iceberg REST Catalog**: [http://localhost:8181](http://localhost:8181) (Iceberg REST Catalog quản lý Silver Warehouse)
- **Application PostgreSQL**: `127.0.0.1:25432` (Database metadata `bronze_meta`, user: `procurement`, db: `procurement`)

---

## 📦 Nạp dữ liệu lịch sử (Historical Bronze Data)

Nếu bạn có các file nén dữ liệu lịch sử trong thư mục `exports/` (`bronze-2022.zip`, `bronze-2023.zip`, v.v.):

1. **Import Parquet vào S3 Storage**:
   ```bash
   uv run python -m procurement.tools.bronze_transfer import --archive exports/bronze-2025.zip
   ```
2. **Đồng bộ Metadata vào PostgreSQL**:
   ```bash
   uv run python -m procurement.tools.import_metadata --resource all
   ```

---

## 🛠️ Chạy cục bộ / Phát triển (Dành cho Developer)

Nếu bạn muốn debug trực tiếp trên máy host thay vì dùng Docker:

- Yêu cầu Python 3.12+, `uv`.
- Cài đặt thư viện: `uv sync --locked --extra dev --extra ops --extra metadata`
- Xem chi tiết tại [Tài liệu Phát triển](docs/development.md) và [Hướng dẫn Vận hành](docs/operations.md).
