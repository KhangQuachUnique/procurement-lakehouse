# Bước 1: Thiết Lập Môi Trường & Hạ Tầng Docker (Environment Setup & Docker Infrastructure)

- **Trạng thái:** CHÍNH THỨC (Cẩm nang hướng dẫn cài đặt từ A đến Z)
- **Hệ thống mục tiêu:** Linux (Ubuntu 22.04+ / Debian 12 / WSL2)
- **Công cụ yêu cầu:** Docker 24+, Docker Compose v2, Python 3.12+, uv

---

## 1. Yêu Cầu Tiên Quyết (Prerequisites)

Trước khi bắt đầu, hãy đảm bảo máy chủ/máy phát triển đã cài đặt các công cụ sau:

```bash
# 1. Kiểm tra Docker & Docker Compose
docker --version
docker compose version

# 2. Cài đặt trình quản lý gói Python siêu tốc (uv)
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.bashrc

# 3. Kiểm tra uv
uv --version
```

---

## 2. Cấu Hình Biến Môi Trường (`.env`)

Tại thư mục gốc của dự án `procurement-lakehouse`, sao chép file mẫu và thiết lập các biến môi trường:

```bash
cp .env.example .env
```

Nội dung cơ bản của `.env`:

```ini
# Storage & Catalog
OBJECT_STORAGE_ENDPOINT=http://127.0.0.1:8333
OBJECT_STORAGE_BUCKET=procurement-lakehouse
OBJECT_STORAGE_ACCESS_KEY=procurement_admin
OBJECT_STORAGE_SECRET_KEY=change_this_to_a_long_random_password
ICEBERG_CATALOG_URI=http://127.0.0.1:8181
ICEBERG_WAREHOUSE=s3://warehouse

# PostgreSQL Databases
APP_POSTGRES_PORT=25432
APP_POSTGRES_USER=procurement
APP_POSTGRES_PASSWORD=password
APP_POSTGRES_DB=procurement
DAGSTER_POSTGRES_PASSWORD=dagster

# Target Namespaces (Mặc định chuẩn xuất xưởng là 'silver' và 'gold')
# Đặt thành 'silver_v2', 'gold_v2' khi bạn phát triển/thử nghiệm phiên bản mới
SILVER_TARGET_NAMESPACE=silver
SILVER_RELEASE_NAMESPACE=silver
GOLD_TARGET_NAMESPACE=gold
GOLD_RELEASE_NAMESPACE=gold

# API Mua Sắm Công (Bộ Tài chính)
MUASAMCONG_BASE_URL=https://muasamcong.mof.gov.vn
```

---

## 3. Khởi Chạy Toàn Bộ Hệ Sinh Thái Docker (7 Services)

Khởi động toàn bộ cụm dịch vụ thông qua một lệnh duy nhất:

```bash
docker compose up -d
```

### Danh mục 7 Containers Hoạt Động:
| Tên Container | Cổng Host | Chức năng chính | Healthcheck |
| :--- | :--- | :--- | :--- |
| `procurement-lakehouse-object-storage-1` | `8333`, `8181` | SeaweedFS S3 API & Apache Iceberg REST Catalog | Port ready |
| `procurement-lakehouse-app-postgres-1` | `25432` | Database lưu trữ Commit Lineage, Partition Attempts | `pg_isready` |
| `procurement-lakehouse-dagster-postgres-1` | `5432` *(internal)* | Database lưu trữ Runs, Events & Schedules của Dagster | `pg_isready` |
| `procurement-lakehouse-dagster-code-location-1` | `4000` *(internal)* | gRPC Server nạp definitions và thực thi Assets | `grpc-health-check` |
| `procurement-lakehouse-dagster-daemon-1` | *(internal)* | Tiến trình điều phối Run Coordinator, Sensor & Schedules | `liveness-check` |
| `procurement-lakehouse-dagster-webserver-1` | `3002` | Dashboard giao diện người dùng trực quan của Dagster | `/server_info (200)` |
| `procurement-lakehouse-ops-1` | `8000` | FastAPI Web Dashboard theo dõi tiến độ thầu | `/health/ready (200)` |

### Kiểm tra Trạng thái Cụm:
```bash
docker compose ps
```
*Tất cả 7 dịch vụ phải ở trạng thái `Up (healthy)`.*

---

## 4. Thiết Lập Môi Trường Python Trên Máy Host (Local Virtualenv)

Sử dụng `uv` để tạo môi trường ảo và cài đặt dependencies trong vòng vài giây:

```bash
# 1. Cài đặt toàn bộ dependencies (bao gồm orchestration & test tools)
uv sync --extra orchestration

# 2. Kích hoạt virtualenv
source .venv/bin/activate
```

---

## 5. Xác Nhận Kiểm Thử Hệ Thống (Running Tests)

Chạy bộ kiểm thử tự động để đảm bảo môi trường đã sẵn sàng 100%:

```bash
# Chạy bộ test đơn vị và kiểm tra ranh giới kiến trúc Hexagonal
pytest tests/unit/gold/ tests/gold/ tests/orchestration/

# Hoặc chạy kiểm tra tính toàn vẹn thông qua uv
uv run pytest tests/gold/
```
*Kết quả mong đợi: Toàn bộ test suite màu xanh (PASSED), không có lỗi nào.*
