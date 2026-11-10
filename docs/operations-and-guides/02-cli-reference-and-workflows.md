# Bước 2: Sổ Tay Tra Cứu Toàn Bộ Lệnh CLI (CLI Reference & End-to-End Workflows)

- **Trạng thái:** CHÍNH THỨC (Cẩm nang hướng dẫn sử dụng dòng lệnh chuẩn 100% mã nguồn)
- **Công cụ:** CLI Bộ ba Medallion (`ingest`, `silver`, `gold`)

Hệ thống cung cấp bộ công cụ CLI mạnh mẽ, cho phép thực thi độc lập từng giai đoạn của pipeline dữ liệu mà không bắt buộc phải mở giao diện Dagster.

---

## 0. Quy Ước Về Namespace Mặc Định & Cơ Chế Thử Nghiệm

Hệ thống phân tách rạch ròi giữa **Bản phát hành chính thức (Canonical Release)** và **Bản thử nghiệm (Experimental/Candidate)**:

* **Mặc định xuất xưởng (Out-of-the-box):**  
  Người dùng hoặc kỹ sư mới clone repo về sẽ luôn chạy mặc định trên hai namespace chuẩn là **`silver`** (23 bảng quan hệ) và **`gold`** (16 bảng Star Schema).
* **Cơ chế dành cho Kỹ sư khi Cải tiến / Thử nghiệm (R&D):**  
  Khi bạn muốn nâng cấp logic (thêm cột, sửa thuật toán gom cụm) mà **không muốn ảnh hưởng đến dữ liệu chính thức đang chạy**, bạn chỉ cần đặt trong file `.env`:
  ```ini
  SILVER_TARGET_NAMESPACE=silver_v2   # hoặc silver_v3, silver_test
  GOLD_TARGET_NAMESPACE=gold_v2       # hoặc gold_v3, gold_test
  ```
  CLI và Dagster sẽ tự động chuyển hướng ghi vào namespace thử nghiệm đó. Sau khi kiểm chứng hoàn tất, dùng lệnh `promote` để đưa sang bản chính.

---

## 1. CLI Thu Thập Dữ Liệu Thô Tầng Bronze (`ingest`)

Đăng ký qua lệnh `ingest` (hoặc `python -m procurement.cli.ingest`).  
Chức năng: Cào dữ liệu từ API Cổng Thông tin Đấu thầu Quốc gia, đóng gói Parquet lưu lên S3 và ghi nhận Commit Lineage vào PostgreSQL (`app-postgres`).

```bash
# 1. Cào toàn bộ 4 tài nguyên chính cho 1 ngày cụ thể
uv run ingest --resource all --date 2025-10-09

# 2. Cào một tài nguyên theo dải ngày (ví dụ: Kế hoạch LCNT trong tháng 10/2025)
uv run ingest --resource khlcnt --start-date 2025-10-01 --end-date 2025-10-31

# 3. Chế độ kiểm tra (Dry-run: chỉ đọc PostgreSQL để kiểm tra ngày còn thiếu, không gọi API, không ghi đè)
uv run ingest --resource project --date 2025-10-09 --dry-run

# 4. Ép buộc cào lại ngay cả khi ngày đó đã có commit thành công
uv run ingest --resource contractor_result --date 2025-10-09 --refresh
```

*Các tài nguyên (`--resource`) hỗ trợ:* `all`, `khlcnt`, `project`, `notify_contractor`, `contractor_result`, `contract_performance`, `bid_opening`.

---

## 2. CLI Chuẩn Hóa Quan Hệ Tầng Silver (`silver`)

Đăng ký qua Typer CLI `silver` (mã nguồn: `src/procurement/cli/silver.py`).  
Chức năng: Đọc Bronze Parquet $\rightarrow$ Kiểm soát chất lượng & chuẩn hóa $\rightarrow$ Xuất bản vào **23 bảng quan hệ Iceberg Format v2**.

### 2.1. Lệnh Thực Thi Chủ Lực (`silver all`)
Tự động xử lý trọn gói toàn bộ 4 tài nguyên chính và ghi thẳng vào Apache Iceberg:

```bash
# 1. Chạy chuẩn hóa cho một ngày cụ thể (mặc định vào target namespace được cấu hình)
uv run silver all --date 2025-10-09

# 2. Chạy dải ngày (tự động kích hoạt đa luồng worker song song theo năm)
uv run silver all --start-date 2025-10-01 --end-date 2025-10-31 --workers 4

# 3. Chỉ định namespace mục tiêu cụ thể (ví dụ chạy thử nghiệm vào silver_v2)
uv run silver all --date 2025-10-09 --namespace silver_v2

# 4. Bỏ qua các ngày đã có dữ liệu trong kho (tránh chạy lại)
uv run silver all --start-date 2025-01-01 --end-date 2025-12-31 --skip-existing
```

### 2.2. Chạy Lẻ Từng Tài Nguyên Riêng Biệt
```bash
uv run silver khlcnt --date 2025-10-09              # Kế hoạch & Gói thầu
uv run silver project --date 2025-10-09             # Dự án đầu tư
uv run silver notice --date 2025-10-09              # Thông báo mời thầu & HSMT
uv run silver result --date 2025-10-09              # Kết quả trúng thầu
uv run silver contract_performance --date 2025-10-09# Hợp đồng pilot
```

### 2.3. Công Cụ Quản Trị & Tra Cứu Kho Silver
```bash
# 1. Quét toàn bộ file Parquet trên S3 và đồng bộ hàng loạt vào Iceberg Format v2
uv run silver sync-iceberg --namespace silver

# 2. Kiểm tra Snapshot ID, số lượng dòng và metadata của 23 bảng trong Iceberg
uv run silver inspect --namespace silver

# 3. Ánh xạ các bảng vật lý thành Zero-Copy SQL Views (Release Promotion)
uv run silver promote --from silver_v2 --to silver

# 4. Mở DuckDB Web SQL Studio cục bộ trên trình duyệt để truy vấn SQL trực tiếp
uv run silver ui --port 8080

# 5. Xem tóm tắt kết quả JSON của lần chạy gần nhất
uv run silver report
```

---

## 3. CLI Tính Toán & Báo Cáo Tầng Gold (`gold`)

Đăng ký qua Typer CLI `gold` (mã nguồn: `src/procurement/gold/adapters/driving/cli.py`).  
Chức năng: Quản lý vòng đời tính toán **Star Schema 16 bảng** (9 Dim + 7 Fact) theo kiến trúc Hexagonal.

### 3.1. Kiểm Tra Hợp Đồng & Mã Băm Logic
```bash
# Xem chi tiết hợp đồng 16 bảng (Tên cột, kiểu dữ liệu, PK, FK, Grain)
uv run gold schema

# Xem mã băm toàn vẹn (64-char logic hash) của mã nguồn tính toán hiện tại
uv run gold version
```

### 3.2. Khởi Tạo Hạ Tầng & Dữ Liệu Danh Mục
```bash
# 1. Tự động khởi tạo 16 bảng trong Iceberg (lake.gold) với phân vùng tháng cho Facts
uv run gold ensure-tables

# 2. Nạp dữ liệu danh mục 63 tỉnh/thành phố chuẩn vào dim_geography
uv run gold seed-geography

# Hoặc chỉ định namespace thử nghiệm:
uv run gold ensure-tables --namespace gold_v3
uv run gold seed-geography --namespace gold_v3
```

### 3.3. Tính Toán Star Schema Tầng Gold (`gold run`)
Thực thi tính toán chuyển đổi từ Silver sang Star Schema 16 bảng của Gold (tự động upsert Dimensions và partition overwrite Fact theo ngày/tháng):

```bash
# 1. Chạy tính toán cho một ngày cụ thể (mặc định silver -> gold)
uv run gold run --date 2025-10-09

# 2. Chạy tính toán trọn gói cho một tháng (ví dụ tháng 10/2025)
uv run gold run --month 2025-10

# 3. Chạy dải ngày lịch sử (tự động phân trang xử lý từng tháng để tối ưu RAM)
uv run gold run --start-date 2022-01-01 --end-date 2025-12-31

# 4. Tùy chọn chỉ định namespace thử nghiệm (ví dụ silver_v3 sang gold_v3)
uv run gold run --start-date 2022-01-01 --end-date 2025-12-31 --silver-namespace silver_v3 --gold-namespace gold_v3
```

### 3.4. Chuyển Đổi Views Báo Cáo Không Cần Copy (Zero-Copy Promote)
```bash
# Tạo/cập nhật 16 View trong schema gold.* trỏ tới bảng vật lý lake.gold.*
uv run gold promote

# Hoặc trỏ tới bản thử nghiệm (ví dụ lake.gold_v3.*):
uv run gold promote --source-namespace gold_v3 --target-schema gold
```

### 3.5. Kiểm Tra Đối Soát Tài Chính 3 Chiều (Reconciliation Audit)
```bash
# So khớp tổng tiền kế hoạch, tổng tiền trúng thầu và khóa ngoại mồ côi (mặc định lake.gold vs lake.silver)
uv run gold reconcile

# Hoặc chỉ định rõ schema khi kiểm thử đối soát:
uv run gold reconcile --gold-schema lake.gold_v3 --silver-schema lake.silver_v3
```
*Kết quả xuất định dạng JSON: `is_valid: true`, giá trị tiền kế hoạch/trúng thầu, `orphan_fks: 0`.*

### 3.6. Quản Lý RunPlan & Khôi Phục Sự Cố
```bash
uv run gold status                          # Xem trạng thái chạy local
uv run gold plan --source ... --target ...  # Đóng băng kế hoạch tính toán
uv run gold resume --plan-file ...          # Tiếp tục kế hoạch đang chạy dở
uv run gold backup --output ...             # Sao lưu local state
uv run gold verify --root ...               # So sánh đối chiếu snapshot từ xa
```

---

## 4. Hướng Dẫn Mẫu: Chạy Vận Hành Một Ngày Từ A $\rightarrow$ Z (Daily Walkthrough)

Quy trình chuẩn thực tế để xử lý trọn vẹn dữ liệu cho ngày `2025-10-09`:

```bash
# =============================================================
# BƯỚC 1: Cào dữ liệu thô (Bronze)
# =============================================================
uv run ingest --resource all --date 2025-10-09

# =============================================================
# BƯỚC 2: Chuẩn hóa 23 bảng quan hệ (Silver)
# =============================================================
uv run silver all --date 2025-10-09

# =============================================================
# BƯỚC 3: Tính toán Star Schema 16 bảng (Gold)
# =============================================================
uv run gold run --date 2025-10-09

# =============================================================
# BƯỚC 4: Kiểm tra Đối soát Toàn vẹn (Reconcile)
# =============================================================
uv run gold reconcile
```

---

## 5. Hướng Dẫn Mẫu: Chạy Toàn Bộ Dữ Liệu Lịch Sử 2022 – 2025 (Historical Backfill)

Quy trình chuẩn để tái lập toàn bộ kho dữ liệu từ 2022 đến hết 2025 bằng CLI:

```bash
# 1. Khởi tạo cấu trúc bảng Gold & nạp 63 tỉnh thành chuẩn
uv run gold ensure-tables
uv run gold seed-geography

# 2. Xử lý Silver đa luồng song song 4 năm (Workers=4, Batch Size=30 ngày/commit)
uv run silver all --start-date 2022-01-01 --end-date 2025-12-31 --workers 4

# 3. Tính toán Star Schema Gold toàn bộ 4 năm (Tự động duyệt 48 tháng)
uv run gold run --start-date 2022-01-01 --end-date 2025-12-31

# 4. Kiểm toán đối soát tài chính 3 chiều toàn diện
uv run gold reconcile

# 5. (Tùy chọn) Public Views phục vụ báo cáo BI
uv run silver promote
uv run gold promote
```
