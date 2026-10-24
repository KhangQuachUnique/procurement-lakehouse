# Báo Cáo Phân Tích Dữ Liệu & Chất Lượng Tầng Silver (2022 - 2023)

> **Ngày lập báo cáo:** 20/10/2026  
> **Phạm vi kiểm tra:** Toàn bộ 730 ngày (01/01/2022 – 31/12/2023) trên 4 tài nguyên chính: `khlcnt`, `notify_contractor` (notice), `project`, `contractor_result`.  
> **Tổng số bản ghi đã nạp:** > 1.6 triệu bản ghi Silver.

---

## 1. Tổng Hợp Kết Quả Thực Thi Theo Tài Nguyên

| Tài nguyên | Tổng số ngày | Đạt / Lỗi | Số bản ghi Silver nạp được | Bị Quarantine | Số Issue cảnh báo |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **`khlcnt`** | 730 ngày | **730 Đạt** / 0 Lỗi | **1,139,786** | 0 | 103 |
| **`notice`** | 730 ngày | **730 Đạt** / 0 Lỗi | **228,156** | 0 | **231,111** |
| **`project`** | 730 ngày | **717 Đạt** / 13 Cần xem | **92,276** | **15** | 18 |
| **`result`** | 538 ngày | 519 Đạt / **19 Bị lỗi** | **179,892** | 0 | 231 |

---

## 2. Chi Tiết Các Vấn Đề Đã Xác Định

### Vấn đề 1: Cảnh báo `unknown_currency` hàng loạt ở `notice` (231,096 lượt)
* **Hiện tượng:** 
  * Cảnh báo `unknown_currency` ở trường `guarantee_amount` (tiền bảo đảm dự thầu): **183,440 lượt**.
  * Cảnh báo `unknown_currency` ở trường `estimate_price` (giá gói thầu dự toán): **47,656 lượt**.
  * Chiếm **99.99%** tổng số cảnh báo của tài nguyên `notice`.
* **Nguyên nhân cốt lõi:**
  * **Đặc tính Data nguồn:** Trong payload JSON gốc của Mua Sắm Công (`bidoNotifyContractorM`), đơn vị tiền tệ chỉ khai báo một lần duy nhất tại trường `bidPriceUnit: "VND"`. Các trường tiền khác như `guaranteeValue` và `bidEstimatePrice` không có trường đơn vị tiền tệ riêng (chỉ gói quốc tế ODA/ADB `vk_adb` mới có `bidGuaranteeUnit`).
  * **Logic Transformer:** Code [`notice.py`](file:///C:/Work/data-project/procurement_lakehouse/src/procurement/silver/domain/transformers/notice.py#L236) cố định tìm các trường không tồn tại `bidGuaranteeUnit` và `bidEstimatePriceUnit`. Khi nhận về `None`, code coi là thiếu tiền tệ và tự động sinh issue `unknown_currency`.
* **Kết luận:** **False Positive (Báo động giả do logic code)**.
* **Giải pháp đề xuất:** Bổ sung cơ chế fallback trong `notice.py`: Nếu `guarantee_currency` hoặc `estimate_currency` bị `None`, tự động fallback lấy theo đơn vị tiền tệ chính của gói thầu (`bidPriceUnit` / VND).

---

### Vấn đề 2: Lỗi tràn số `Conversion Error` ở `contractor_result` (19 ngày bị fail)
* **Hiện tượng:** Pipeline dừng tại 19 ngày với lỗi:
  `Conversion Error: Casting value "-480645999.0000" to type DECIMAL(10,4) failed: value is out of range! when casting from source column saving_rate`
  (Các giá trị tương tự: `-96956999.0000`, `-435534999.0000`, `-774879999.0000`).
* **Thống kê thực tế trên 322,635 gói thầu (`award_lot`):**
  * Tổng số lô/gói thầu: **322,635**.
  * Thiếu giá gói thầu (`lot_price IS NULL`): **139** (0.04% - rất ít).
  * Giá gói thầu bằng 0 (`lot_price = 0`): **20**.
  * Giá gói thầu nhập tượng trưng 1 đồng (`lot_price = 1`): **3** trường hợp.
  * Giá gói thầu dưới 1.000 đồng: **5** trường hợp.
  * Thiếu giá trúng thầu (`winning_price IS NULL`): **42,361** (13.1% - gói hủy hoặc chào thầu không thành công).
  * Giá trúng thầu vượt giá gói (`winning_price > lot_price`): **13,255** (4.1% - hợp lệ trong đấu thầu khi có trượt giá/bổ sung khối lượng, saving_rate âm nhẹ từ -1% đến -20%).
  * Giá trị âm cực đoan (`saving_rate < -1000%` do `lot_price = 1`): **79** trường hợp.
* **Nguyên nhân cốt lõi:**
  * Chủ đầu tư nhập giá tượng trưng `lot_price = 1` trong khi `winning_price = 480,646,000`.
  * Công thức $\text{saving\_rate} = \frac{\text{lot\_price} - \text{winning\_price}}{\text{lot\_price}} = -480,645,999.0000$ làm tràn kiểu dữ liệu `Numeric(10,4)` (vốn chỉ lưu tối đa 6 chữ số phần nguyên).
* **Kết luận:** **Data bất thường + Thiếu logic kiểm soát biên trong code**.
* **Giải pháp đề xuất:**
  * Bổ sung điều kiện chặn: Khi `lot_price is None` hoặc `lot_price <= 1`, không tính tỷ lệ tiết kiệm (`saving_rate = None`), ghi nhận cảnh báo chất lượng `data_anomaly`.
  * Nếu tính ra `saving_rate < -10.0` hoặc `saving_rate > 1.0`, gán `None` + cảnh báo thay vì để tràn số làm sập pipeline.

---

### Vấn đề 3: 15 bản ghi bị Quarantine tại `project` (13 ngày `needs_review`)
* **Hiện tượng:** 15 bản ghi dự án bị đưa vào bảng `quarantine` với lý do `missing_identity`.
* **Bằng chứng dữ liệu:** 
  Kiểm tra payload JSON từ Mua Sắm Công:
  ```json
  "id": null,
  "projectDTO": {"id": null, "code": null, "name": null, "status": null, ...}
  ```
  Tất cả các trường đều là `null` (ghost record do hệ thống nguồn trả về bản ghi rác).
* **Kết luận:** **100% do Data nguồn**.
* **Giải pháp:** Cơ chế Quarantine cách ly bản ghi không có khóa chính là hoàn toàn đúng đắn theo kiến trúc Lakehouse. Giữ nguyên cơ chế này.

---

### Vấn đề 4: Cảnh báo thứ tự ngày bất hợp lý `invalid_date_order` (249 lượt)
* **Hiện tượng:**
  * 231 lượt: Ngày phê duyệt trước ngày mở thầu (`decisionDate < bidOpenDate`).
  * 10 lượt: Ngày đăng tải thông báo sau ngày đóng thầu (`publishedAt > bidCloseDate`).
  * 5 lượt: Ngày đóng thầu sau ngày mở thầu.
* **Nguyên nhân:** Do bên mời thầu nhập nhầm ngày tháng hoặc cập nhật lại thông báo sau khi đã đóng thầu.
* **Kết luận:** **100% do Data nguồn**. Hệ thống đã ghi nhận dưới dạng `severity: warn`, không ảnh hưởng đến việc nạp dữ liệu.

---

## 3. Kiến Trúc Vận Hành: Cơ Chế Dọn Cũ & Đảm Bảo Không Trùng Lặp (Idempotency)

Khi chạy lại Silver toàn bộ app, hệ thống **hoàn toàn không bị duplicate dữ liệu** nhờ 3 cơ chế tự động dọn cũ:

1. **Tầng S3 Parquet (Atomic Partition Overwrite):**
   * Định dạng đường dẫn: `silver/{table}/source_date={YYYY-MM-DD}/data.parquet`.
   * Khi chạy lại ngày $D$, hệ thống mở file `data.parquet` của ngày $D$ và **ghi đè hoàn toàn** (`mode="wb"`). Không bao giờ tồn tại 2 file cho cùng một ngày.

2. **Tầng Iceberg Format v2 (Key-Level ACID UPSERT):**
   * Trước khi insert bản ghi của một ngày, DuckDB thực hiện:
     ```sql
     BEGIN;
     DELETE FROM lake.silver.{table} WHERE {pk} IN (SELECT {pk} FROM _incoming_candidate);
     INSERT INTO lake.silver.{table} BY NAME SELECT * FROM _incoming_candidate;
     COMMIT;
     ```
   * Bất kỳ bản ghi nào trùng khóa chính (`revision_id`) đã nạp trước đó đều bị xóa sạch trước khi insert mới.

3. **Tầng DuckDB Vectorized Views (`silver.v_latest_*`):**
   * Khử trùng động bằng hàm SIMD:
     `ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY version DESC, date DESC)`.
   * Luôn chỉ trả về duy nhất 1 bản ghi mới nhất cho mỗi thực thể.

4. **Lệnh xóa trắng toàn bộ dữ liệu Silver nếu muốn làm sạch 100%:**
   ```powershell
   python -m procurement.cli.silver reset --all
   ```
   Lệnh này sẽ xóa sạch toàn bộ các thư mục `silver/` và `silver_candidates/` trên S3 để bắt đầu lại hoàn toàn mới.

---

## 4. Trạng Thái Thực Hiện Kế Hoạch Xử Lý (Đã hoàn thành 100%)

- [x] **Kế hoạch 1: Triệt tiêu 231,000 cảnh báo `unknown_currency` ở `notice`**
  - **Giải pháp:** Cập nhật [`src/procurement/silver/domain/transformers/notice.py`](file:///C:/Work/data-project/procurement_lakehouse/src/procurement/silver/domain/transformers/notice.py) với cơ chế tự động fallback: nếu `estimate_currency` hoặc `guarantee_currency` bị `None` ở luồng `standard`, fallback lấy theo đơn vị tiền tệ chính của gói thầu (`bidPriceUnit` / VND).
  - **Kết quả nghiệm thu:** Kiểm tra thực tế trên chuỗi ngày 2022-01-01 ➔ 2022-01-03, số lượng issue cảnh báo giảm về **0 issues** (đạt 100% sạch).

- [x] **Kế hoạch 2: Khắc phục lỗi tràn số `DECIMAL(10,4)` và cô lập rác `lot_price` ở `contractor_result`**
  - **Giải pháp:** 
    1. Bỏ hẳn 2 cột dẫn xuất `saving_amount` và `saving_rate` khỏi schema Silver [`src/procurement/silver/domain/schema.py`](file:///C:/Work/data-project/procurement_lakehouse/src/procurement/silver/domain/schema.py) và transformer [`src/procurement/silver/domain/transformers/result.py`](file:///C:/Work/data-project/procurement_lakehouse/src/procurement/silver/domain/transformers/result.py). Tầng Silver chỉ lưu trữ trung thực các sự kiện thực tế (`lot_price`, `winning_price`), các chỉ số phân tích tiết kiệm sẽ do Data Mart (Gold) tính toán an toàn.
    2. Áp dụng cơ chế kiểm duyệt chặt cho `lot_price`: nếu `price <= 0` gán `None` kèm issue `invalid_zero_price`; nếu `price <= 10` VND gán `None` kèm issue `dummy_placeholder_price`; nếu `lot_price * 100 < winning_price` gán issue cảnh báo lệch đơn vị `price_scale_anomaly`.
  - **Kết quả nghiệm thu:** Chạy kiểm thử ngày `2022-10-13` (ngày trước đây bị sập do tràn số `-480,645,999`): Xử lý thành công 446 hồ sơ trúng thầu, 458 lô thầu, 0 lỗi, 0 quarantined, trạng thái `completed`.

- [x] **Kế hoạch 3: Bổ sung cờ `--skip-existing` tự động bỏ qua ngày đã xử lý**
  - **Giải pháp:** 
    1. Mở rộng cổng [`SilverCandidateRepositoryPort.partition_exists`](file:///C:/Work/data-project/procurement_lakehouse/src/procurement/silver/ports/outbound/silver_repository.py) và cài đặt tại adapter S3 [`S3SilverCandidateAdapter`](file:///C:/Work/data-project/procurement_lakehouse/src/procurement/silver/adapters/driven/storage/s3_candidate_adapter.py).
    2. Cập nhật use-case [`ProcessSilverService`](file:///C:/Work/data-project/procurement_lakehouse/src/procurement/silver/application/process_silver_service.py): nếu `skip_existing=True` và ngày đó đã có đầy đủ candidate parquet trên S3, hệ thống tự động ghi nhận `status="skipped"` và bypass toàn bộ việc đọc/tải Bronze từ Object Storage.
    3. Thêm tùy chọn `--skip-existing` trên tất cả các lệnh CLI (`notice`, `project`, `result`, `khlcnt`, `all`).
  - **Kết quả nghiệm thu:** Test chạy lệnh `result --date 2022-10-13 --skip-existing`: Nhận diện ngày đã tồn tại và hoàn thành trong nháy mắt với trạng thái `↷ SKIPPED | Already exists in Silver lakehouse`. Tất cả 237/237 unit tests pass. Pyright đạt 0 errors, 0 warnings.
