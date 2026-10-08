# Cẩm Nang Phát Triển Tầng Silver (Silver Lakehouse Specification & Guide)

> **Dành cho:** Toàn bộ thành viên phát triển Data Lakehouse.  
> **Nguyên tắc kỹ thuật:** Độc lập module theo Resource, tuân thủ Schema chuẩn 15 bảng, ép kiểu chặt chẽ (Decimal, Date, Clean Text), không làm rơi rụng bản ghi (Quarantine policy).

---

## 1. Kiến Trúc Tổng Thể 15 Bảng Dữ Liệu Tầng Silver

Dữ liệu Silver được lưu trữ vật lý dưới dạng **Parquet trên S3/SeaweedFS** và quản lý bởi **Apache Iceberg Format v2** qua REST Catalog. DuckDB truy vấn trực tiếp thông qua schema `lake.muasamcong`.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                       15 BẢNG DỮ LIỆU SILVER (TRÊN S3/ICEBERG)                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│ 1. Nhóm Truy vết & Phiên bản (4 bảng):                                          │
│    ├── observation                 (Vết vật lý: trỏ về file Bronze Parquet nào) │
│    ├── entity                      (Định danh canonical duy nhất của thực thể)  │
│    ├── entity_revision             (Lịch sử thay đổi nội dung qua các version)  │
│    └── current_entity              (Snapshot bản ghi mới nhất đang có hiệu lực) │
│                                                                                 │
│ 2. Nhóm Bảng Nghiệp vụ Cốt lõi - Typed Revisions (6 bảng):                      │
│    ├── project_revision            (Dự án đầu tư: tổng mức đầu tư, địa bàn...)  │
│    ├── plan_revision               (Kế hoạch LCNT: mục tiêu, quy mô, mã DA...)  │
│    ├── package_revision            (Gói thầu: giá gói, giá dự toán, loại HĐ...) │
│    ├── notice_revision             (Thông báo mời thầu: ngày đóng/mở thầu...)   │
│    ├── result_revision             (Kết quả LCNT: quyết định trúng thầu...)     │
│    └── opening_revision            (Biên bản mở thầu: tình trạng nộp HSDT...)   │
│                                                                                 │
│ 3. Nhóm Bảng Thành phần Con & Quan hệ (3 bảng):                                 │
│    ├── lot                         (Chi tiết các lô thầu trong gói/kết quả)     │
│    ├── bid_participation           (Nhà thầu tham gia: Thắng/Thua, MST, giá...) │
│    └── relationship                (Mạng lưới liên kết Khóa Ngoại giữa các bảng)│
│                                                                                 │
│ 4. Nhóm Quản trị Chất lượng (2 bảng):                                           │
│    ├── quarantine                  (Cách ly các bản ghi lỗi thiếu ID / rỗng)    │
│    └── quality_issue               (Cảnh báo chất lượng: thiếu tiền tệ, sai date│
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Chi Tiết Schema 15 Bảng Chuẩn (Data Dictionary)

### Nhóm 1: Metadata & Lineage (4 bảng)
1. **`observation`**:  
   `observation_id VARCHAR (PK), resource VARCHAR, source_date DATE, run_id VARCHAR, table_name VARCHAR, file_key VARCHAR, file_sha256 VARCHAR, row_ordinal BIGINT, observed_at TIMESTAMPTZ, source_id VARCHAR, source_version VARCHAR, content_hash VARCHAR, payload_json VARCHAR`
2. **`entity`**:  
   `entity_id VARCHAR (PK), namespace VARCHAR, entity_type VARCHAR, id_scheme VARCHAR, source_identity VARCHAR`
3. **`entity_revision`**:  
   `revision_id VARCHAR (PK), entity_id VARCHAR (FK), source_version VARCHAR, semantic_hash VARCHAR, mapping_version VARCHAR, payload_json VARCHAR`
4. **`current_entity`**:  
   `entity_id VARCHAR (PK), revision_id VARCHAR (FK), selection_status VARCHAR, observed_at TIMESTAMPTZ`

---

### Nhóm 2: Bảng Nghiệp Vụ Cốt Lõi (6 bảng)

#### 1. `project_revision` (Dự án đầu tư)
| Tên cột | Kiểu dữ liệu | Ý nghĩa |
| :--- | :--- | :--- |
| `revision_id` | `VARCHAR (PK)` | Mã băm định danh phiên bản |
| `entity_id` | `VARCHAR (FK)` | Khóa ngoại trỏ bảng `entity` |
| `project_no` | `VARCHAR` | Số hiệu dự án (VD: 20220100277) |
| `title` | `VARCHAR` | Tên dự án |
| `investor_code` | `VARCHAR` | Mã chủ đầu tư |
| `investor_name` | `VARCHAR` | Tên chủ đầu tư |
| `total_investment` | `DECIMAL(38,6)` | Tổng mức đầu tư (chuẩn Decimal) |
| `currency` | `VARCHAR` | Đơn vị tiền tệ (VND, USD...) |
| `decision_no` | `VARCHAR` | Số quyết định phê duyệt dự án |
| `decision_date` | `DATE` | Ngày ban hành quyết định (YYYY-MM-DD) |
| `prov_code` | `VARCHAR` | Mã tỉnh/thành phố |
| `district_code` | `VARCHAR` | Mã quận/huyện |
| `public_date` | `DATE` | Ngày đăng tải công khai |
| `status_raw` | `VARCHAR` | Trạng thái nguồn |
| `root_json` | `VARCHAR` | Payload JSON sau trích xuất |

#### 2. `plan_revision` (Kế hoạch LCNT)
* Cột: `revision_id, entity_id, entity_type, plan_no, plan_version, title, invest_target, invest_scale, investor_name, project_id (FK), project_no, total_investment DECIMAL(38,6), currency, public_date DATE, status_raw, root_json`.

#### 3. `package_revision` (Gói thầu KHLCNT)
* Cột: `revision_id, entity_id, entity_type, package_no, title, plan_no, plan_id (FK), bid_price DECIMAL(38,6), estimate_price DECIMAL(38,6), currency, bid_field (Lĩnh vực: Xây lắp, Hàng hóa...), bid_form (Hình thức: Đấu thầu rộng rãi...), bid_mode (Phương thức: 1 túi, 2 túi), contract_type (Loại hợp đồng: Trọn gói...), execution_period, is_domestic BOOLEAN, is_internet BOOLEAN, public_date DATE, status_raw, root_json`.

#### 4. `notice_revision` (Thông báo mời thầu TBMT)
* Cột: `revision_id, entity_id, entity_type, notify_no, notify_version, package_no, title, procuring_entity_code, procuring_entity_name (Bên mời thầu), investor_name (Chủ đầu tư), bid_open_date TIMESTAMPTZ, bid_close_date TIMESTAMPTZ, bid_price DECIMAL(38,6), currency, bid_field, bid_form, bid_mode, notification_type, public_date DATE, status_raw, root_json`.

#### 5. `result_revision` (Kết quả lựa chọn nhà thầu)
* Cột: `revision_id, entity_id, entity_type, result_id, result_version, notify_no, notify_version, package_no, title, decision_no, decision_date DATE, total_winning_price DECIMAL(38,6), currency, public_date DATE, status_raw, root_json`.

#### 6. `opening_revision` (Biên bản mở thầu)
* Cột: `revision_id, entity_id, entity_type, notify_no, notify_version, title, bid_mode, actual_open_date TIMESTAMPTZ, total_bidders BIGINT, public_date DATE, status_raw, root_json`.

---

### Nhóm 3: Thành Phần Con & Khóa Ngoại (3 bảng)
1. **`bid_participation`** (Nhà thầu tham gia & Trúng thầu):  
   `participation_id VARCHAR (PK), revision_id VARCHAR (FK), lot_no VARCHAR, contractor_code VARCHAR, contractor_name VARCHAR, tax_code VARCHAR, is_consortium BOOLEAN, consortium_name VARCHAR, bid_price DECIMAL(38,6), winning_price DECIMAL(38,6), currency VARCHAR, is_winner BOOLEAN, evaluation_rank BIGINT, raw_json VARCHAR`
2. **`lot`** (Lô thầu):  
   `lot_id VARCHAR (PK), revision_id VARCHAR (FK), lot_no VARCHAR, lot_name VARCHAR, lot_price DECIMAL(38,6), estimate_price DECIMAL(38,6), currency VARCHAR, winning_code VARCHAR, status_raw VARCHAR, raw_json VARCHAR`
3. **`relationship`** (Cầu nối Foreign Key động):  
   `relationship_id VARCHAR (PK), from_revision_id VARCHAR (FK), target_type VARCHAR, id_scheme VARCHAR, target_identity VARCHAR, field VARCHAR, resolution_status VARCHAR, target_entity_id VARCHAR`  
   * Trạng thái `resolution_status`:
     * `resolved`: Đã tìm thấy đúng thực thể đích trong Lakehouse.
     * `unresolved`: Khóa có tồn tại nhưng thực thể đích chưa được cào về (tự động resolve khi cào bổ sung).
     * `ambiguous`: Trùng lặp nhiều thực thể đích.
     * `missing`: Nguồn để trống.

---

### Nhóm 4: Quản Trị Chất Lượng (2 bảng)
1. **`quarantine`**: `observation_id VARCHAR (PK), reason VARCHAR` (Chứa các bản ghi thiếu ID hoặc rỗng root).
2. **`quality_issue`**: `issue_id VARCHAR (PK), observation_id VARCHAR, code VARCHAR, path VARCHAR, severity VARCHAR` (`warn` | `error`).

---

## 3. Quản Lý Nhật Ký Chạy (Operational Metadata trong PostgreSQL)

Hệ thống lưu nhật ký thực thi Silver trong schema **`silver_meta`** trên PostgreSQL (cổng `25432`):
* `silver_meta.runs`: Ghi nhận `run_id`, `partition_date`, `status` (`RUNNING`, `SUCCESS`, `FAILED`), thời gian chạy, Dagster Run ID.
* `silver_meta.reconciliation`: Báo cáo đối soát:
  $$\text{input\_bronze\_records} = \text{accepted\_records} + \text{quarantined\_records}$$

---

## 4. Hướng Dẫn Dành Cho Thành Viên Phát Triển

### Phân Công Công Việc
| Dev | Bảng Bronze | Tên File Transformer | File Unit Test |
| :--- | :--- | :--- | :--- |
| **Lead (Mẫu)** | `project_detail` | `transformers/project.py` | `tests/unit/silver/test_project_transformer.py` |
| **Dev A** | `khlcnt_plan_detail` | `transformers/plan.py` | `tests/unit/silver/test_plan_transformer.py` |
| **Dev B** | `khlcnt_bid_package_detail` | `transformers/package.py` | `tests/unit/silver/test_package_transformer.py` |
| **Dev C** | `notify_contractor_*` | `transformers/notice.py` | `tests/unit/silver/test_notice_transformer.py` |
| **Dev D** | `contractor_result_detail` | `transformers/result.py` | `tests/unit/silver/test_result_transformer.py` |
| **Dev E** | `bid_opening_detail` | `transformers/opening.py` | `tests/unit/silver/test_opening_transformer.py` |

### Quy Chuẩn Code (Coding Standards)
1. Kế thừa `BaseResourceTransformer` và đăng ký `@register_transformer("tên_bảng_bronze")`.
2. Sử dụng các hàm parse chuẩn từ `procurement.processing.silver.parsers`:
   * `parse_money()` cho tiền tệ (bắt buộc `Decimal(38,6)`, không dùng float).
   * `clean_text()` cho mã, tên, chuỗi (giữ nguyên số 0 đầu).
   * `parse_date()` cho ngày tháng (`YYYY-MM-DD`).
   * `parse_boolean()` cho cờ (không dùng `bool("0")`).
3. Khai báo quan hệ qua `extract_references()` để hệ thống tự động ghi nhận vào `relationship`.
4. Trích xuất nhà thầu tham gia dự thầu vào `extract_children()` với `kind="participant"`.

---

## 5. Công Cụ Kiểm Thử & Vận Hành (CLI)

### Kiểm Tra Biến Đổi 1 Bản Ghi Bronze Thật
```powershell
uv run python -m procurement.cli.silver inspect --resource project --date 2022-01-01
```

### Xem Danh Sách Các Transformer Đã Đăng Ký
```powershell
uv run python -m procurement.cli.silver list
```

### Chạy Toàn Bộ Test Suite
```powershell
uv run pytest tests/unit/silver
uv run ruff check src/procurement/cli src/procurement/processing/silver tests/unit/silver
```

### Xem Dữ Liệu Bronze Bằng Web SQL DuckDB
```powershell
uv run python -m procurement.tools.bronze_explorer --port 4213
# Mở trình duyệt: http://localhost:4213
```
