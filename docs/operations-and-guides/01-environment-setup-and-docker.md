# Bước 1: Thiết Lập Môi Trường & Hạ Tầng Docker (Environment Setup)

- **Hệ thống mục tiêu:** Linux (Arch/CachyOS, Ubuntu 22.04+, Debian 12) hoặc Windows 11 (WSL2 Ubuntu)
- **Công cụ yêu cầu:** Docker 24+ (Docker Compose v2), Python 3.12+, `uv`, Java 21 JRE (chỉ cần trên máy host nếu chạy CLI `silver`/`gold` trực tiếp ngoài Docker)

---

## 1. Yêu Cầu Tiên Quyết (Prerequisites)

```bash
# 1. Kiểm tra Docker & Docker Compose v2
docker --version
docker compose version

# 2. Cài đặt uv (Python package manager)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 3. Cài Java 21 JRE (nếu chạy Spark Driver trực tiếp từ máy host)
# Arch / CachyOS:
sudo pacman -S jre21-openjdk-headless
# Ubuntu / Debian:
# sudo apt-get update && sudo apt-get install -y openjdk-21-jre-headless

java -version
```

---

## 2. Cấu Hình Biến Môi Trường (`.env`)

Sao chép file mẫu từ thư mục gốc của repository:

```bash
cp .env.example .env
```

Các nhóm cấu hình chính trong `.env`:

```ini
# 1. Storage & Apache Iceberg REST Catalog (SeaweedFS)
OBJECT_STORAGE_BIND_ADDRESS=127.0.0.1      # đổi sang IP LAN hoặc IP Tailscale khi chạy multi-node K3s
ICEBERG_CATALOG_BIND_ADDRESS=127.0.0.1
OBJECT_STORAGE_ENDPOINT=http://127.0.0.1:8333
OBJECT_STORAGE_BUCKET=procurement-lakehouse
OBJECT_STORAGE_ACCESS_KEY=procurement_admin
OBJECT_STORAGE_SECRET_KEY=change_this_to_a_long_random_password
ICEBERG_CATALOG_URI=http://127.0.0.1:8181
ICEBERG_WAREHOUSE=s3://warehouse

# 2. PostgreSQL (dùng chung 1 instance cho cả metadata ứng dụng & Dagster)
APP_POSTGRES_PORT=25432
APP_POSTGRES_USER=procurement
APP_POSTGRES_PASSWORD=password
APP_POSTGRES_DB=procurement
DAGSTER_POSTGRES_PASSWORD=dagster

# 3. Apache Spark 4.0 + Iceberg Namespaces
SPARK_SILVER_NAMESPACE=silver_spark
SPARK_MASTER=local[4]
SPARK_DRIVER_MEMORY=4g
SPARK_EXECUTOR_MEMORY=4g
SPARK_EXECUTOR_CORES=6
SPARK_SHUFFLE_PARTITIONS=24
# Khi chạy cụm K3s multi-node:
# SPARK_K3S_MASTER=spark://<MASTER_IP>:7077
# SPARK_LOCAL_IP=<HOST_IP>

# 4. API Mua Sắm Công
MUASAMCONG_BASE_URL=https://muasamcong.mof.gov.vn
```

---

## 3. Khởi Chạy Hạ Tầng Docker Compose

Khởi chạy toàn bộ stack cốt lõi (Storage + Catalog + PostgreSQL + Dagster tích hợp sẵn Java 21 & PySpark 4.0.0):

```bash
docker compose up -d --build
```

*(Lần đầu build image Dagster mất ~3–4 phút để cài Java 21 + `pyspark==4.0.0`; các lần build lại sau dùng cache chỉ mất ~5–10 giây.)*

### Danh sách các dịch vụ cốt lõi

| Service | Cổng Host | Chức năng |
| :--- | :--- | :--- |
| `object-storage` | `8333` (S3), `8181` (Iceberg REST) | SeaweedFS S3 API + Apache Iceberg REST Catalog |
| `app-postgres` | `25432` | PostgreSQL 17 lưu Bronze metadata, Spark watermarks & Dagster DB |
| `db-migrate` | *(one-shot)* | Tự động chạy `alembic upgrade head` trước khi Dagster lên |
| `dagster-code-location` | `4000` *(internal)* | gRPC Server nạp `procurement.orchestration.definitions` (có sẵn JVM + PySpark) |
| `dagster-webserver` | `3002` | Giao diện web Dagster: **http://127.0.0.1:3002** |
| `dagster-daemon` | *(internal)* | Thực thi lịch chạy tự động (Schedules) và Sensors |

Kiểm tra trạng thái:
```bash
docker compose ps
```
Tất cả service phải ở trạng thái `Up (healthy)`.

> **Tùy chọn Spark trên Docker cục bộ:** Nếu không dùng K3s nhưng muốn test cụm Spark Standalone bằng Docker trên 1 máy, chạy `docker compose --profile spark up -d`. **Không bật** `--profile spark` cùng lúc với K3s để tránh trùng cổng `7077`/`8080`.

---

## 4. Thiết Lập Môi Trường Python Trên Máy Host

```bash
# Cài đặt đầy đủ dependencies (bao gồm Spark 4.0.0, Dagster & pytest/ruff)
uv sync --extra orchestration --extra spark --extra dev

# Kiểm tra 3 CLI chính thức của dự án
.venv/bin/ingest --help
.venv/bin/silver --help
.venv/bin/gold --help
```

---

## 5. Kiểm Thử Nhanh (Verification)

```bash
# Chạy toàn bộ unit tests (không cần kết nối mạng / DB thật)
.venv/bin/pytest -q

# Chạy thử Silver & Gold incremental cục bộ
.venv/bin/silver run --master local[4]
.venv/bin/gold run --master local[4]
```

---

## 6. Mở Rộng Sang Cụm Multi-Node Spark Trên K3s

Khi cần cộng dồn CPU/RAM từ nhiều máy (Linux + Windows WSL2), xem cẩm nang chi tiết cho **2 mô hình mạng (LAN Wi-Fi & Tailscale VPN)** tại:

👉 **[Hướng dẫn triển khai K3s Multi-Node Spark (LAN & Tailscale)](../../infra/k3s/README.md)**
