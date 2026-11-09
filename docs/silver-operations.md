# Cẩm Nang Vận Hành & Hướng Dẫn Sử Dụng CLI Tầng Silver (Silver Operations & CLI Manual)

- **Trạng thái:** CHÍNH THỨC (Tài liệu hướng dẫn vận hành cho kỹ sư Data Platform & SRE)
- **Công cụ thực thi:** Typer CLI (`procurement silver` hoặc `python -m procurement.cli.silver`)
- **Tài liệu kiến trúc:** [Kiến Trúc Tầng Silver Lakehouse](silver-architecture.md)
- **Hạ tầng liên quan:** Docker (`compose.yaml`), SeaweedFS REST Catalog (`http://localhost:8333`), PostgreSQL Metadata

---

## 1. Tổng Quan CLI Tầng Silver

Module CLI của Silver được xây dựng dựa trên thư viện **Typer**, cung cấp giao diện dòng lệnh mạnh mẽ, hiển thị thanh tiến trình Rich UI đa luồng trực quan và tích hợp đầy đủ các cơ chế bảo vệ (fail-safe), ghi log có cấu trúc và quản lý bộ nhớ.

### 1.1. Cách gọi lệnh

Bạn có thể thực thi CLI theo 2 cách tương đương:

```bash
# Cách 1: Sử dụng package entrypoint trực tiếp (khuyên dùng qua uv)
uv run procurement silver <command> [OPTIONS]

# Cách 2: Sử dụng module Python chuẩn
uv run python -m procurement.cli.silver <command> [OPTIONS]
```

### 1.2. Danh mục các lệnh chính (Commands)

| Lệnh | Alias | Tài nguyên xử lý | Bảng vật lý liên quan |
|---|---|---|---|
| `notice` | `tbmt` | Thông báo mời thầu (TBMT) | `notice_revision`, `notice_lot`, `notice_location`, `notice_reoffer_submission`, `notice_amendment` |
| `project` | `da` | Dự án đầu tư phát triển | `project_revision`, `project_location` |
| `result` | `kq`, `contractor_result` | Kết quả lựa chọn nhà thầu | `result_revision`, `award_lot`, `award_contractor`, `bid_participation` |
| `khlcnt` | `plan`, `package` | Kế hoạch LCNT & Gói thầu | `plan_revision`, `package_revision`, `package_lot`, `package_location` |
| `contract_performance` | `thhd`, `contract` | Hợp đồng & Tình hình thực hiện | `contract_revision`, `contract_performance_revision`, `contract_payment` |
| `all` | — | Chạy toàn bộ 5 tài nguyên | Cập nhật toàn bộ 23 bảng Silver theo thứ tự phụ thuộc chuẩn |
| `promote` | — | Thăng hạng Blue-Green | Tạo/cập nhật 33 logical views trong schema `silver.*` trỏ sang namespace vật lý |
| `reset` | — | Làm sạch dữ liệu | Xóa bỏ dữ liệu Silver trong namespace đích để chuẩn bị nạp lại từ đầu |

---

## 2. Chi Tiết Các Tham Số & Cờ Tùy Chọn (Options & Flags)

### 2.1. Nhóm chỉ định phạm vi thời gian (Date Range)
* `--date YYYY-MM-DD`:
  * Xử lý chính xác dữ liệu của **duy nhất một ngày nguồn**.
  * Không thể dùng chung với `--start-date` hoặc `--end-date`.
  * *Ví dụ:* `--date 2024-05-15`.
* `--start-date YYYY-MM-DD` & `--end-date YYYY-MM-DD`:
  * Xử lý một khoảng thời gian liên tục (bao gồm cả hai ngày đầu và cuối).
  * *Ví dụ:* `--start-date 2022-01-01 --end-date 2026-12-31`.

### 2.2. Nhóm tối ưu hóa hiệu năng & Micro-Batching
* `-b`, `--batch-size INTEGER` (Mặc định: `30`):
  * **Số ngày tích lũy tối đa trong bộ đệm** trước khi ghép khối qua PyArrow và ghi 1 snapshot commit vào Iceberg Format v2.
  * *Khuyến nghị:* Giữ nguyên mặc định `30` khi chạy khoảng thời gian lớn để giảm 96.7% overhead HTTP commit lên SeaweedFS Catalog. Đặt `--batch-size 1` nếu muốn kiểm tra chi tiết từng ngày độc lập.
* `--skip-existing` (Cờ boolean):
  * Tự động kiểm tra nhanh (chỉ mất 0.15s) danh sách các ngày đã tồn tại trong bảng `observation` của Silver.
  * Bỏ qua không xử lý lại các ngày đã có, giúp tiếp tục tiến trình backfill bị gián đoạn cực nhanh mà không làm trùng lặp dữ liệu.
* `--parallel / --no-parallel` (Mặc định: `--parallel`):
  * Khi phát hiện khoảng ngày trải rộng qua nhiều năm (ví dụ 2022 - 2026), CLI sẽ tự động chia nhỏ khoảng ngày theo từng năm (chunk per year) và khởi chạy đồng thời các worker threads.
  * Hiển thị giao diện Rich UI đa thanh tiến trình, cập nhật tiến độ real-time từng năm độc lập.
  * Sử dụng `--no-parallel` nếu muốn chạy tuần tự từng ngày trên 1 thread duy nhất để dễ debug.
* `-w`, `--workers INTEGER` (Mặc định: tự động chọn 2-4):
  * Số lượng worker threads chạy song song đồng thời khi xử lý multi-year.
  * *Khuyến nghị:* Nên đặt bằng số core CPU hoặc từ 2 đến 4 để tránh làm nghẽn I/O SeaweedFS.

### 2.3. Nhóm hạ tầng lưu trữ & xử lý lỗi
* `--iceberg / --no-iceberg` (Mặc định: `--iceberg`):
  * Bật/tắt ghi dữ liệu trực tiếp vào Apache Iceberg Format v2 tables thông qua SeaweedFS REST Catalog.
  * Nếu dùng `--no-iceberg`, hệ thống chỉ xuất bản các file Parquet phân vùng theo Hive mà không commit Iceberg snapshot.
* `--continue-on-error` (Cờ boolean):
  * Tiếp tục xử lý các ngày tiếp theo ngay cả khi có một ngày bị lỗi nghiệp vụ hoặc vỡ dữ liệu. Báo cáo cuối cùng vẫn trả mã lỗi exit code 1 nếu có ngày thất bại.

---

## 3. Các Kịch Bản Vận Hành Chuẩn (Runbooks)

### Kịch bản 1: Nạp toàn bộ lịch sử 5 năm (Full 5-Year Backfill 2022-2026)

Đây là quy trình nạp toàn diện toàn bộ 5 năm dữ liệu với hiệu năng cao nhất, áp dụng cơ chế micro-batching 30 ngày và song song đa năm:

```bash
# Bước 1: Nạp Notice (Thông báo mời thầu)
uv run procurement silver notice --start-date 2022-01-01 --end-date 2026-12-31 --skip-existing -b 30

# Bước 2: Nạp Project (Dự án)
uv run procurement silver project --start-date 2022-01-01 --end-date 2026-12-31 --skip-existing -b 30

# Bước 3: Nạp Result (Kết quả LCNT)
uv run procurement silver result --start-date 2022-01-01 --end-date 2026-12-31 --skip-existing -b 30

# Bước 4: Nạp KHLCNT (Kế hoạch LCNT & Gói thầu)
uv run procurement silver khlcnt --start-date 2022-01-01 --end-date 2026-12-31 --skip-existing -b 30

# Bước 5: Nạp Contract Performance (Hợp đồng & Tiến độ)
uv run procurement silver contract_performance --start-date 2022-01-01 --end-date 2026-12-31 --skip-existing -b 30
```

> [!TIP]
> Bạn cũng có thể dùng lệnh duy nhất `uv run procurement silver all --start-date 2022-01-01 --end-date 2026-12-31 --skip-existing -b 30` để hệ thống tự động chạy tuần tự từng tài nguyên theo thứ tự phụ thuộc chuẩn.

---

### Kịch bản 2: Vận hành hàng ngày (Daily Scheduled Run / Dagster Cron)

Trong môi trường sản xuất hàng ngày (ví dụ chạy lúc 02:00 AM sáng), hệ thống chỉ cần xử lý ngày hôm qua (`YYYY-MM-DD`):

```bash
# Xử lý toàn bộ tài nguyên của ngày hôm qua
YESTERDAY=$(date -d "yesterday" +%Y-%m-%d)

uv run procurement silver all --date "$YESTERDAY" --batch-size 1
```

* Thời gian thực thi trung bình: **3 đến 5 giây** cho toàn bộ 5 tài nguyên.
* Ghi log kết quả chi tiết ra báo cáo JSON lưu tại S3: `s3://procurement-lakehouse/silver/_reports/.../report.json`.

---

### Kịch bản 3: Sửa lỗi & Nạp lại một ngày cụ thể (Idempotent Replay)

Khi phát hiện dữ liệu Bronze của một ngày nào đó bị cập nhật lại (crawl lại), bạn cần chạy lại ngày đó trên Silver:

```bash
# Chạy nạp đè ngày cụ thể mà không dùng --skip-existing
uv run procurement silver result --date 2024-09-20 --batch-size 1
```

* **Cơ chế Idempotent (Bất biến):** Hệ thống sẽ tự động đối chiếu các bản ghi incoming theo `(entity_id, semantic_hash)`:
  * Nếu bản ghi không thay đổi nội dung: giữ nguyên revision cũ.
  * Nếu bản ghi có thay đổi: tự động tạo `revision_id` mới và cập nhật cờ `is_current = TRUE`, đánh dấu bản ghi cũ `is_current = FALSE`.
  * Các bảng con (award_lot, contractor) của revision đó được thay thế nguyên khối (Atomic cohort replacement), hoàn toàn không để lại rác hay dòng mồ côi.

---

### Kịch bản 4: Thăng hạng Blue-Green (Promote Release)

Sau khi hoàn thành nạp mới hoặc nâng cấp phiên bản dữ liệu vào namespace vật lý (ví dụ `lake.silver_v2`), thực hiện thăng hạng để các ứng dụng downstream (Gold Layer, BI, Dashboard) có thể truy cập ngay lập tức:

```bash
# Thăng hạng silver_v2 lên logical views trong schema silver.*
uv run procurement silver promote --target-namespace silver_v2
```

**Kết quả thực thi:**
* Tạo mới hoặc cập nhật **33 Views logic** trong schema `silver.*` (gồm 23 views ánh xạ bảng vật lý và 10 views `v_latest_*` khử trùng lặp).
* Cập nhật file `.env`: `SILVER_RELEASE_NAMESPACE=silver_v2`.
* Quá trình diễn ra trong **0.2 giây, zero-downtime** và không tốn thêm byte lưu trữ nào.

---

## 4. Giám Sát, Báo Cáo & Truy Vấn Kiểm Tra

### 4.1. Cấu trúc Báo Cáo Thực Thi (Report JSON)

Mỗi lần chạy, pipeline đều ghi một bản báo cáo đầy đủ lên S3 tại đường dẫn:
`s3://procurement-lakehouse/silver/_reports/<resource>/<run_id>/report.json`.

Cấu trúc file báo cáo:
```json
{
  "run_id": "r_2024_abc123",
  "resource": "notify_contractor",
  "status": "completed",
  "start_date": "2024-01-01",
  "end_date": "2024-01-31",
  "total_days": 31,
  "passed_days": 31,
  "failed_days": 0,
  "records": {
    "accepted": 1250,
    "quarantined": 0,
    "issues": 15
  },
  "iceberg_tables_updated": 6
}
```

### 4.2. Truy Vấn Dữ Liệu Silver Bằng DuckDB CLI / Python

Bạn có thể mở một kết nối DuckDB nhanh để kiểm tra trực tiếp dữ liệu Silver đã promote:

```python
import duckdb
from procurement.common.settings import settings
from procurement.storage.iceberg import connect

config = settings.iceberg_config()
with connect(config) as db:
    # 1. Đếm tổng số bản ghi quan sát
    total_obs = db.sql("SELECT count(*) FROM silver.observation").fetchone()[0]
    print(f"Total Observations: {total_obs:,}")

    # 2. Truy vấn danh sách gói thầu mới nhất qua SIMD view
    latest_packages = db.sql("""
        SELECT package_code, package_name, price_vnd, public_date
        FROM silver.v_latest_package
        WHERE price_vnd > 10000000000
        LIMIT 5
    """).df()
    print(latest_packages)
```

---

## 5. Xử Lý Sự Cố Thường Gặp (Troubleshooting)

### Sự cố 1: `Failure when receiving data from the peer` (HTTP curl 56)
* **Nguyên nhân:** SeaweedFS REST Catalog hoặc S3 API bị quá tải kết nối khi nhiều luồng commit cùng lúc hoặc bộ nhớ SeaweedFS bị nghẽn.
* **Xử lý:**
  1. Đảm bảo tham số `--batch-size` được đặt tối thiểu là `30` để giảm số lượng kết nối commit.
  2. Kiểm tra container SeaweedFS: `docker compose ps seaweedfs`.
  3. Cấp thêm RAM cho SeaweedFS trong file `compose.yaml` (đã được cấu hình khuyến nghị 8GB).
  4. Hệ thống đã có cơ chế tự động thử lại 5 lần (Exponential Backoff), tiến trình sẽ tự phục hồi sau vài giây.

### Sự cố 2: `Exclusive file lock failed / Resource temporarily unavailable`
* **Nguyên nhân:** Một tiến trình CLI Silver khác đang chạy ngầm và giữ lock file tại `data/locks/silver_*.lock`.
* **Xử lý:**
  * Kiểm tra các tiến trình đang chạy: `ps aux | grep procurement.cli.silver`.
  * Nếu tiến trình trước đã bị dừng đột ngột mà chưa kịp giải phóng lock, kiểm tra và xóa file lock tương ứng trong thư mục `data/locks/`.

### Sự cố 3: Dữ liệu bị trùng lặp hoặc cần nạp lại từ đầu (Clean Slate)
* **Xử lý:**
  * Không bao giờ xóa thủ công các file trên S3 vì sẽ làm hỏng metadata của Iceberg Catalog.
  * Sử dụng lệnh reset an toàn của CLI:
    ```bash
    uv run procurement silver reset --namespace silver_v2 --force
    ```
  * Sau đó tiến hành chạy lại pipeline backfill bình thường.
