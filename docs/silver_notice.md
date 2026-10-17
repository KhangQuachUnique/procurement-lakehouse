# Silver Notice — Contract notice-2

Tài liệu đặc tả kiến trúc, mô hình dữ liệu quan hệ thuần túy, và quy trình xử lý của Thông báo mời thầu (TBMT) trên nhánh `refactor/silver`.

---

## 1. Phạm vi và Kiến trúc Hexagonal

Hợp đồng `notice-2` xử lý chuẩn hóa 3 bảng Bronze của Thông báo mời thầu thuộc nguồn `muasamcong`:
1. `notify_contractor_standard_detail` (Standard - Thông báo tiêu chuẩn)
2. `notify_contractor_vk_adb_detail` (VK/ADB - Vốn vay quốc tế / Ngân hàng Phát triển Châu Á)
3. `notify_contractor_reoffer_detail` (Reoffer - Chào giá trực tuyến rút gọn)

### Các thành phần theo Kiến trúc Lục giác (Hexagonal Architecture)
- **Domain Layer:**
  - `BaseResourceTransformer` & `NoticeTransformer` (`src/procurement/silver/domain/transformers/notice.py`): Triển khai bóc tách theo quy tắc nghiệp vụ chuyên biệt cho từng family.
  - `schema.py` (`src/procurement/silver/domain/schema.py`): Định nghĩa tập trung bằng SQLAlchemy 2.0 cho bảng `notice_revision` và 3 bảng con kiểu (`notice_lot`, `notice_location`, `notice_reoffer_submission`).
  - `parsers.py`: Xử lý số tiền độ chính xác cao `Numeric(38, 6)`, timezone `Asia/Ho_Chi_Minh` cho timestamp không có múi giờ, boolean chuẩn (không dùng truthiness), và mã phân cấp pipe (`|115|`).
- **Application Layer:**
  - `ProcessNoticeService` (`src/procurement/silver/application/process_notice_service.py`): Đóng gói use case, điều phối Bronze Repository Port và Silver Candidate Repository Port.
  - `revisions.py`: Gom cụm các bản ghi theo quan hệ 1-hop (`observation_id`), tính toán snapshot hiệu lực gần nhất (`is_current`), và kiểm tra tính toàn vẹn khóa ngoại `revision_id`.
  - `resources.py`: Quản lý declarative contract `notice-2` và danh mục bảng đầu ra.
  - `warehouse.py`: Ép kiểu kiểm tra (typed casting) qua DuckDB SQL schema contract thành các bảng `PyArrow Table`.
- **Adapters Layer:**
  - `ArrowBronzeRepositoryAdapter`: Đọc commit manifests đã kiểm chứng từ `_control` trên S3/MinIO.
  - `S3SilverCandidateAdapter`: Ghi dữ liệu Parquet chuẩn Hive-partitioning lên S3 và lưu báo cáo thực thi `report.json`.

---

## 2. Vận hành CLI

CLI sử dụng thư viện [Typer](https://typer.tiangolo.com/), định hướng theo tài nguyên (resource-first):

```powershell
# Chạy một ngày cụ thể (tự động xử lý cả 3 family: standard, vk_adb, reoffer)
uv run python -m procurement.cli.silver notice --date 2025-01-01

# Chạy theo khoảng ngày liên tục (inclusive cả ngày đầu và ngày cuối)
uv run python -m procurement.cli.silver notice --start-date 2025-01-01 --end-date 2025-01-05

# Alias tiếng Việt tương đương
uv run python -m procurement.cli.silver tbmt --date 2025-01-01

# Lọc chạy riêng một family để kiểm thử hoặc đối soát
uv run python -m procurement.cli.silver notice --date 2025-01-02 --family reoffer

# Bỏ qua ngày lỗi để tiếp tục các ngày kế tiếp (exit code cuối vẫn khác 0 nếu có lỗi)
uv run python -m procurement.cli.silver notice --start-date 2025-01-01 --end-date 2025-01-05 --continue-on-error

# Kiểm tra báo cáo thực thi gần nhất của Notice
uv run python -m procurement.cli.silver report --resource notice
```

---

## 3. Cấu trúc lưu trữ Lakehouse trên S3

Toàn bộ dữ liệu Silver Notice được ghi trực tiếp vào Lakehouse theo cấu trúc **Hive Partitioning**:

```text
s3://procurement-lakehouse/
  silver/
    ├── notice_revision/
    │     ├── source_date=2025-01-01/
    │     │     └── data.parquet
    │     └── source_date=2025-01-02/
    │           └── data.parquet
    ├── notice_lot/
    │     └── source_date=2025-01-01/
    │           └── data.parquet
    ├── notice_location/
    │     └── source_date=2025-01-01/
    │           └── data.parquet
    ├── notice_reoffer_submission/
    │     └── source_date=2025-01-01/
    │           └── data.parquet
    ├── observation/
    │     └── source_date=2025-01-01/
    │           └── data.parquet
    ├── quality_issue/
    │     └── source_date=2025-01-01/
    │           └── data.parquet
    ├── quarantine/
    │     └── source_date=2025-01-01/
    │           └── data.parquet
    └── _reports/
          └── notice/
                ├── <run_id>/
                │     └── report.json
                └── latest.json
```

### Tính Idempotent (Idempotency)
Mỗi ngày xử lý tạo ra chính xác một file `data.parquet` cho mỗi bảng. Khi chạy lại một ngày đã xử lý, dữ liệu của ngày đó được thay thế nguyên tử (atomic overwrite) trên phân vùng `source_date=YYYY-MM-DD`, ngăn ngừa hoàn toàn tình trạng trùng lặp dữ liệu.

---

## 4. Đặc tả Schema & Mô hình Quan hệ thuần túy

Mô hình dữ liệu đã loại bỏ hoàn toàn các bảng RDF/Graph (`entity`, `entity_revision`, `revision_observation`, `relationship`, `child`). Mọi quan hệ được tổ chức theo chuẩn quan hệ 1-hop:

```
[observation] (Physical Raw Data + Hash + Lineage)
      ^
      | (1-hop via observation_id)
[notice_revision] (Core Entity Revision, is_current=true/false)
      |
      +----< (1-N via revision_id) [notice_lot]
      |
      +----< (1-N via revision_id) [notice_location]
      |
      +----< (1-N via revision_id) [notice_reoffer_submission]
```

### 4.1. Bảng chính: `notice_revision`
Mỗi dòng đại diện cho một phiên bản thông báo mời thầu được quan sát. DDL chi tiết tại `src/procurement/silver/domain/schema.py`:

| Nhóm trường | Tên cột chính | Nguồn / Quy tắc chuẩn hóa |
|---|---|---|
| **Truy vết (Lineage)** | `revision_id`, `entity_id`, `observation_id`, `is_current` | `observation_id` trỏ trực tiếp tới bản ghi `observation` vật lý; `is_current` đánh dấu snapshot có hiệu lực gần nhất của thông báo. |
| **Nguồn (Provenance)** | `source_table`, `source_family`, `source_root_path`, `source_document_id` | `standard`: bidoNotifyContractorM hoặc bidNoContractorResponse.bidNotification; `VK`: bidoNotifyContractorP; `reoffer`: root |
| **Gói / Kế hoạch** | `package_id`, `plan_id`, `plan_no`, `plan_version`, `plan_name`, `project_name` | Trích xuất từ `bidId`, `planNo`, `projectName`. `plan_id`/`plan_version` chỉ lấy từ embedded package khi `package_id` khớp. |
| **Bên mời thầu / Chủ đầu tư** | `investor_code`, `investor_name`, `procuring_entity_code`, `procuring_entity_name`, `buyer_id`, `buyer_name` | Tách bạch vai trò Chủ đầu tư và Bên mời thầu; `buyer_id`/`buyer_name` là alias chuẩn hóa của bên mời thầu. |
| **Thời gian** | `published_at`, `original_published_at`, `bid_open_date`, `bid_close_date`, `plan_decision_date` | Giờ mở/đóng thầu tiêu chuẩn. ISO không múi giờ được gán timezone `Asia/Ho_Chi_Minh`. Ngày mở/đóng thầu thông thường không bị ghi đè bởi ngày chào giá lại. |
| **Chào giá lại (Reoffer)** | `reoffer_no`, `reoffer_version`, `reoffer_status_raw`, `reoffer_open_at`, `reoffer_close_at`, `initial_price`, `price_step`, `reoffer_currency` | Dành riêng cho family `reoffer`. Không gán giá khởi điểm chào lại thành giá dự toán gói thầu. |
| **Giá trị & Bảo đảm** | `bid_price`, `amount`, `currency`, `estimate_price`, `estimate_currency`, `guarantee_amount`, `guarantee_currency`, `guarantee_form` | `Numeric(38, 6)`. Tiền tệ chỉ ghi khi có trực tiếp tại nguồn, không tự suy đoán. Thiếu currency phát sinh `QualityIssue(code="unknown_currency")`. |
| **Hợp đồng & Hình thức** | `contract_type`, `contract_duration_raw`, `contract_duration_unit`, `bid_form`, `bid_mode`, `notification_type` | Chuẩn hóa mã hình thức/phương thức và loại hợp đồng. |
| **Điều kiện dự thầu** | `bid_validity_raw`, `bid_validity_unit`, `is_internet`, `is_domestic`, `is_multi_lot`, `is_prequalification` | Boolean chuẩn hóa bằng parser; các mã thô chưa chuẩn hóa ngữ nghĩa được giữ nguyên trong cột `*_raw`. |
| **Hủy thầu & Tài liệu** | `cancelled_at`, `cancel_reason`, `cancel_decision_no`, `cancel_decision_date`, `cancel_file_id`, `report_file_id` | Thông tin quyết định hủy thầu và ID văn bản đính kèm. |
| **Bảo toàn dữ liệu** | `field_sources_json`, `root_json` | Lưu vết đường dẫn JSON của từng trường và toàn bộ JSON gốc của root. |

### 4.2. Các bảng con kiểu (Typed Sub-entities)
Mỗi bảng con đều chứa `child_id` (khóa chính), `revision_id` (khóa ngoại trỏ về `notice_revision`), `source_id`, `source_path`, `ordinal`, và `raw_json`:

1. **`notice_lot` (Lô/Phần của gói thầu):**
   - Trường: `lot_no`, `lot_name`, `lot_price`, `estimate_price`, `currency`, `guarantee_amount`, `contract_duration_raw`, `contract_duration_unit`, `notify_id`, `notify_no`.
   - Quy tắc chọn Mirror: Đối với standard notice, nếu có danh sách mirror tại `bidNoContractorResponse.bidNotification.lotDTOList`, hệ thống ưu tiên sử dụng danh sách này (kể cả khi rỗng). Cả hai danh sách gốc vẫn được lưu vết trong `payload_json`.
2. **`notice_location` (Địa bàn thực hiện gói thầu):**
   - Trường: `province_code`, `province_name`, `district_code`, `district_name`, `ward_code`, `ward_name`.
   - Trích xuất từ các mảng `bidpBidLocationList`, `lsBidpBidLocationDTO`, hoặc `bidDetail.bidLocation`.
3. **`notice_reoffer_submission` (Lượt nộp chào giá trực tuyến rút gọn):**
   - Trường: `source_list`, `submission_id`, `reoffer_id`, `lot_no`, `contractor_code`, `contractor_name`, `venture_code`, `venture_name`, `offered_at`, `offered_at_raw`, `times_raw`, `price`, `final_price`, `currency`, `is_newest`, `form_code`, `form_value`.
   - Trích xuất từ cả hai danh sách `contractorsResult` và `contractorsResultAll` (phân biệt qua cột `source_list`).

---

## 5. Kết quả kiểm chứng thực nghiệm trên toàn bộ Bronze Archive (2022–2025)

Trong quá trình thiết kế hợp đồng `notice-2`, toàn bộ dữ liệu lịch sử Bronze đã được khảo sát thực nghiệm:

### Phân tích toàn tập 6.966 bản ghi Reoffer:
- Cả 6.966 bản ghi đều có `bidForm=CGTTRG` và `notifyType=CGTTRG`.
- Cả 6.966 bản ghi đều có `notifyNo = reofferNo` và `notifyId = id`.
- **Kết luận:** Reoffer là một family nghiệp vụ của thông báo chào giá trực tuyến rút gọn, không phải thực thể độc lập tách rời khỏi thông báo. Do đó, toàn bộ dữ liệu reoffer được lưu vào `notice_revision`, và các lượt chào nằm tại bảng con `notice_reoffer_submission`.
- Hai danh sách `contractorsResult` và `contractorsResultAll` cùng có dữ liệu ở 6.498 bản ghi, cùng rỗng ở 351 bản ghi, và cùng null ở 117 bản ghi.
- Trong `contractorsResultAll`, tồn tại các giá trị `times=2, 3...` (riêng `times=2` có 9.323 occurrences), chứng minh quy trình chào lại có ghi nhận nhiều vòng chào giá.

### Thống kê kiểm thử mẫu trên 40 file Bronze Parquet (1.467 observations):
- **1.467 accepted, 0 quarantined, 0 blocking issues.**
- **4.901 notice_lot**, **1.532 notice_location**, **2.535 notice_reoffer_submission** được tạo ra và nạp thành công vào DuckDB với độ chính xác cao.
- **1.648 cảnh báo `unknown_currency`** được ghi nhận và lưu vào bảng `quality_issue` (do một số trường giá ở nguồn không đi kèm đơn vị tiền tệ trực tiếp; hệ thống giữ nguyên giá trị số và gắn cờ cảnh báo thay vì tự đoán tiền tệ).

---

## 6. Truy vấn dữ liệu với DuckDB

```sql
INSTALL httpfs;
LOAD httpfs;
SET s3_endpoint = 'localhost:9000';
SET s3_access_key_id = 'minioadmin';
SET s3_secret_access_key = 'minioadmin';
SET s3_use_ssl = false;
SET s3_url_style = 'path';

-- 1. Xem danh sách TBMT mới nhất cùng trạng thái hiệu lực
SELECT 
    notify_no,
    notify_version,
    title,
    bid_price,
    currency,
    procuring_entity_name,
    bid_close_date,
    source_date,
    is_current
FROM read_parquet('s3://procurement-lakehouse/silver/notice_revision/**/*.parquet', hive_partitioning=true)
WHERE source_date >= '2025-01-01' AND is_current = true
ORDER BY bid_price DESC
LIMIT 10;

-- 2. Truy vấn liên kết 1-hop từ Lô thầu sang TBMT
SELECT 
    n.notify_no,
    n.title AS notice_title,
    l.lot_no,
    l.lot_name,
    l.lot_price,
    l.currency
FROM read_parquet('s3://procurement-lakehouse/silver/notice_lot/**/*.parquet') l
JOIN read_parquet('s3://procurement-lakehouse/silver/notice_revision/**/*.parquet') n
    ON l.revision_id = n.revision_id
LIMIT 5;

-- 3. Thống kê số lượng chào giá theo nhà thầu trong Reoffer
SELECT 
    contractor_name,
    count(*) AS total_submissions,
    min(final_price) AS lowest_price,
    max(currency) AS currency
FROM read_parquet('s3://procurement-lakehouse/silver/notice_reoffer_submission/**/*.parquet')
WHERE contractor_name IS NOT NULL
GROUP BY contractor_name
ORDER BY total_submissions DESC
LIMIT 10;
```
