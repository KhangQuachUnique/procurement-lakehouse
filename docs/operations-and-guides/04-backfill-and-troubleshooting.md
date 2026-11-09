# Bước 4: Sổ Tay Chạy Backfill Lịch Sử & Xử Lý Sự Cố (Backfill & Troubleshooting Guide)

- **Trạng thái:** CHÍNH THỨC
- **Mục tiêu:** Cung cấp quy trình nạp bù dữ liệu 5 năm (2022–2025) và các cẩm nang xử lý sự cố trong 30 giây.

---

## 1. Quy Trình Chạy Backfill Dữ Liệu Lịch Sử (Historical Backfill)

Hệ thống hỗ trợ nạp bù dữ liệu lịch sử một cách an toàn mà không làm gián đoạn các luồng dữ liệu đang chạy:

### 1.1. Chạy Backfill Thông qua Giao Diện Dagster (Khuyên dùng)
1. Truy cập **`http://localhost:3002/jobs/lakehouse_end_to_end`**.
2. Nhấn vào **Backfill** (bên cạnh nút Materialize).
3. Nhập dải ngày cần chạy: ví dụ từ `2025-01-01` đến `2025-01-31`.
4. Thiết lập **Max Concurrent Runs = 1** (bảo đảm an toàn bộ nhớ cho DuckDB và Iceberg).
5. Nhấn **Submit Backfill**. Daemon Dagster sẽ tự động thực thi tuần tự từng ngày một cách hoàn toàn tự động.

### 1.2. Chạy Backfill Thông qua CLI Script
Bạn cũng có thể chạy script backfill độc lập cho tầng Gold theo từng tháng:
```bash
python scripts/run_gold_full_backfill.py --start-month 2025-01 --end-month 2025-12
```

---

## 2. Sổ Tay Xử Lý Các Sự Cố Thường Gặp (Troubleshooting Runbooks)

### Sự cố 1: Sửa mã nguồn trên Host nhưng Dagster vẫn chạy code cũ
- **Nguyên nhân:** Container Docker không mount thư mục `src/` trực tiếp mà chạy từ thư viện Python đã đóng gói sẵn trong image lúc build (`/app/.venv/lib/python3.12/site-packages/procurement/`).
- **Cách xử lý nhanh (trong 10 giây):**
  ```bash
  # Đồng bộ mã nguồn mới nhất vào cả 3 container Dagster
  docker cp src/procurement procurement-lakehouse-dagster-code-location-1:/app/.venv/lib/python3.12/site-packages/
  docker cp src/procurement procurement-lakehouse-dagster-daemon-1:/app/.venv/lib/python3.12/site-packages/
  docker cp src/procurement procurement-lakehouse-dagster-webserver-1:/app/.venv/lib/python3.12/site-packages/

  # Khởi động lại các container
  docker restart procurement-lakehouse-dagster-code-location-1 procurement-lakehouse-dagster-daemon-1 procurement-lakehouse-dagster-webserver-1
  ```

---

### Sự cố 2: Lỗi `ValidationError: Target namespace must match semantic versioning`
- **Nguyên nhân:** Namespace mục tiêu không tuân thủ quy tắc đặt tên Semantic Versioning (`gold_v<N>[_<suffix>]`).
- **Cách xử lý:** Đảm bảo namespace tuân theo định dạng: `gold_v2`, `gold_v3`, `gold_v4`, hoặc `gold_v2_accepted`, `gold_v3_test`.

---

### Sự cố 3: Lỗi `Table with name dim_date does not exist!`
- **Nguyên nhân:** Chạy pipeline với một namespace Iceberg mới hoàn toàn nhưng chưa khởi tạo cấu trúc bảng.
- **Cách xử lý:** Chạy lệnh tạo bảng trước khi nạp dữ liệu:
  ```bash
  gold ensure-tables --namespace gold_v2
  gold seed-geography --namespace gold_v2
  ```

---

### Sự cố 4: Xuất hiện Node "Ma" (Ghost Asset) trên giao diện Dagster
- **Hiện tượng:** Sau khi đổi tên hoặc xóa một asset trong code (ví dụ `gold_dim_bidding_type`), giao diện Dagster vẫn hiển thị node này với màu xám do còn lưu vết lịch sử trong Database.
- **Cách xử lý:** Xóa sạch bản ghi cũ trong database Dagster Postgres:
  ```bash
  docker exec procurement-lakehouse-dagster-postgres-1 psql -U dagster -d dagster -c "
  DELETE FROM event_logs WHERE asset_key = '[\"gold_dim_bidding_type\"]';
  DELETE FROM asset_keys WHERE asset_key = '[\"gold_dim_bidding_type\"]';
  "
  docker restart procurement-lakehouse-dagster-webserver-1
  ```

---

### Sự cố 5: Xung đột Khóa DuckDB (`Database file is locked by another process`)
- **Nguyên nhân:** Hai Job cùng chạy đồng thời và cố gắng mở file `state.duckdb` của cùng một thư mục làm việc.
- **Cách xử lý:** Hệ thống đã được cấu hình trong `infra/dagster/dagster.yaml` với `max_concurrent_runs: 1`. Đảm bảo không mở file `state.duckdb` bằng CLI bên ngoài trong lúc Dagster đang xử lý partition đó.
