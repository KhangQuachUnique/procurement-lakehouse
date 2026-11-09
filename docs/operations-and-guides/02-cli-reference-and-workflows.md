# Bước 2: Sổ Tay Tra Cứu Toàn Bộ Lệnh CLI (CLI Reference & End-to-End Workflows)

- **Trạng thái:** CHÍNH THỨC (Cẩm nang hướng dẫn sử dụng dòng lệnh từ A đến Z)
- **Công cụ:** Typer CLI (`procurement`, `silver`, `gold`)

Hệ thống cung cấp bộ công cụ CLI mạnh mẽ, cho phép thực thi độc lập từng giai đoạn của pipeline dữ liệu mà không nhất thiết phải mở giao diện Dagster.

---

## 1. CLI Thu Thập Dữ Liệu Thô (`procurement ingest`)

Thực hiện thu thập dữ liệu thô từ API Cổng Thông tin Đấu thầu Quốc gia, đóng gói Parquet và ghi nhận commit vào PostgreSQL.

```bash
# Cào một ngày cụ thể cho tài nguyên Kế hoạch LCNT (khlcnt)
uv run python -m procurement.cli.ingest --resource khlcnt --start-date 2025-10-09 --end-date 2025-10-09

# Cào toàn bộ 4 tài nguyên chính cho một khoảng thời gian
for res in khlcnt project notify_contractor contractor_result; do
    uv run python -m procurement.cli.ingest --resource $res --start-date 2025-10-01 --end-date 2025-10-31
done

# Chế độ kiểm tra (Dry-run: chỉ đếm số bản ghi, không ghi S3/Postgres)
uv run python -m procurement.cli.ingest --resource notify_contractor --date 2025-10-09 --dry-run
```

---

## 2. CLI Chuẩn Hóa Tầng Silver (`procurement silver`)

Chuyển đổi dữ liệu Bronze sang 23 bảng quan hệ Iceberg Format v2.

### 2.1. Khởi tạo 23 Bảng Vật lý trong Iceberg
```bash
uv run silver init-tables
# Kết quả: Tạo thành công schema lake.silver_v2 và 23 bảng quan hệ
```

### 2.2. Chuẩn hóa & Nạp Dữ liệu theo Ngày (Stage & Publish)
```bash
# Bước 1: Đọc commit Bronze từ PostgreSQL và stage sang Arrow
uv run silver stage --date 2025-10-09

# Bước 2: Xuất bản dữ liệu vào Apache Iceberg lake.silver_v2
uv run silver publish --date 2025-10-09

# Bước 3: Đối soát số lượng bản ghi giữa Bronze và Silver
uv run silver reconcile --date 2025-10-09
```

---

## 3. CLI Tính Toán & Báo Cáo Tầng Gold (`gold`)

Công cụ hợp nhất quản lý toàn bộ vòng đời tính toán Star Schema tầng Gold.

### 3.1. Kiểm tra Hợp đồng & Phiên bản Logic
```bash
# Xem định nghĩa hợp đồng 16 bảng (Columns, PK, FK, Grains)
gold schema

# Kiểm tra mã băm 64-ký tự toàn vẹn của logic tính toán hiện tại
gold version
```

### 3.2. Khởi tạo Hạ Tầng & Dữ Liệu Tham Chiếu
```bash
# 1. Tự động tạo 16 bảng trong Iceberg (lake.gold_v2) với phân vùng tháng
gold ensure-tables --namespace gold_v2

# 2. Nạp dữ liệu danh mục 63 tỉnh/thành phố chuẩn vào dim_geography
gold seed-geography --namespace gold_v2
```

### 3.3. Chuyển Đổi Views Báo Cáo Không Cần Copy (Zero-Copy Promote)
```bash
# Tạo/cập nhật toàn bộ View trong schema gold.* trỏ tới lake.gold_v2.*
gold promote --source-namespace gold_v2 --target-schema gold
```

### 3.4. Kiểm Tra Đối Soát Tài Chính 3 Chiều (Reconcile Audit)
```bash
# So khớp số tiền lập kế hoạch, số tiền trúng thầu và khóa ngoại mồ côi
gold reconcile --gold-schema lake.gold_v2 --silver-schema silver
```
*Kết quả xuất định dạng JSON: `is_valid: true`, giá trị lệch = 0, orphan_fks = 0.*

---

## 4. Hướng Dẫn Mẫu: Chạy Thủ Công Một Ngày Từ A $\rightarrow$ Z (Walkthrough)

Quy trình chuẩn để xử lý trọn vẹn dữ liệu ngày `2025-10-09`:

```bash
# -------------------------------------------------------------
# BƯỚC 1: Cào dữ liệu thô (Bronze)
# -------------------------------------------------------------
uv run python -m procurement.cli.ingest --resource khlcnt --date 2025-10-09
uv run python -m procurement.cli.ingest --resource project --date 2025-10-09
uv run python -m procurement.cli.ingest --resource notify_contractor --date 2025-10-09
uv run python -m procurement.cli.ingest --resource contractor_result --date 2025-10-09

# -------------------------------------------------------------
# BƯỚC 2: Chuẩn hóa quan hệ (Silver)
# -------------------------------------------------------------
uv run silver stage --date 2025-10-09
uv run silver publish --date 2025-10-09

# -------------------------------------------------------------
# BƯỚC 3: Tính toán Star Schema (Gold)
# -------------------------------------------------------------
# Gọi engine tính toán trực tiếp thông qua processor
docker exec procurement-lakehouse-dagster-code-location-1 python -c "
from procurement.gold.application.processor import process_gold_day
res = process_gold_day('2025-10-09', gold_namespace='gold_v2')
print('GOLD RESULT:', res)
"

# -------------------------------------------------------------
# BƯỚC 4: Chuyển đổi View & Đối soát
# -------------------------------------------------------------
gold promote --source-namespace gold_v2 --target-schema gold
gold reconcile --gold-schema lake.gold_v2 --silver-schema silver
```
