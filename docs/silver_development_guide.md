# Hướng dẫn phát triển Silver Layer (Silver Lakehouse Development Guide)

Tài liệu này quy định kiến trúc, quy trình phát triển, chuẩn lưu trữ và các hợp đồng kỹ thuật cho tầng Silver của Procurement Lakehouse.

---

## 1. Kiến trúc tổng thể (Hexagonal / Ports & Adapters Architecture)

Tầng Silver được thiết kế theo mô hình **Kiến trúc Lục giác (Hexagonal Architecture)** nhằm tách biệt tuyệt đối giữa nghiệp vụ chuẩn hóa dữ liệu mua sắm công và các công nghệ hạ tầng (S3, MinIO, DuckDB, PyArrow, PostgreSQL):

```
                   +-------------------------------------------------------+
                   |                 Procurement CLI (Typer)               |
                   +-------------------------------------------------------+
                                              |
                                              v (Inbound Port)
+--------------------------------------------------------------------------------------------------+
| APPLICATION LAYER                                                                                |
|   - ProcessNoticeService / ProcessSilverService (Use Case Orchestration)                         |
|   - resources.py (Declarative Resource Contracts: notice-2, project-1)                          |
|   - observations.py (RawRecordEnvelope & Dispatching)                                            |
|   - revisions.py (Relational Assembly & 1-hop Lineage Validation)                                |
|   - warehouse.py (DuckDB SQL Contract & Typed PyArrow Casting)                                   |
+--------------------------------------------------------------------------------------------------+
       |                                                                             |
       v (Domain Model & Registry)                                                   |
+-----------------------------------------------------------+                        |
| DOMAIN LAYER                                              |                        |
|   - BaseResourceTransformer / Protocol                    |                        |
|   - Transformers: notice.py, project.py                   |                        |
|   - parsers.py (Strict decimal, timezone, pipe codes)     |                        |
|   - schema.py (SQLAlchemy 2.0 SSOT, DDL & Arrow Schemas)  |                        |
+-----------------------------------------------------------+                        |
       ^                                                                             v (Outbound Ports)
+------------------------------------+             +-----------------------------------------------+
| DRIVEN ADAPTER: BRONZE REPOSITORY  |             | DRIVEN ADAPTER: SILVER STORAGE                |
|   - ArrowBronzeRepositoryAdapter   |             |   - S3SilverCandidateAdapter                  |
|   - Reads committed S3 manifests   |             |   - Writes Hive-partitioned Parquet files     |
|     under `_control/...`           |             |   - Saves execution run & latest reports      |
+------------------------------------+             +-----------------------------------------------+
```

### Luồng xử lý duy nhất (Single Processing Pipeline)
```
CLI Command → ProcessSilverService → select_committed_partitions (Bronze)
  → stream_partition_records → require_transformer (Registry)
  → transform → assemble (1-hop relational) → validate (reconciliation & FKs)
  → typed_arrow_rows (Schema cast) → S3 write_candidate_tables → save_run_report
```

---

## 2. Mô hình quan hệ thuần túy & Xóa bỏ Graph Tables

Trong phiên bản kiến trúc mới, toàn bộ các bảng trung gian thừa hưởng từ mô hình RDF/Graph cũ (`entity`, `entity_revision`, `revision_observation`, `current_entity`, `relationship`, `child`) đã được **xóa bỏ triệt để**. 

Thay vào đó, hệ thống tổ chức theo **mô hình dữ liệu quan hệ thuần túy (pure relational model)** với các nguyên tắc:
1. **Truy vết 1-hop trực tiếp (1-hop lineage):** Mọi bảng `<domain>_revision` đều chứa trực tiếp cột `observation_id` trỏ thẳng tới bảng `observation` vật lý, bảo toàn nguồn gốc file và digest SHA-256 của từng bản ghi Bronze.
2. **Trạng thái mới nhất trực tiếp (`is_current`):** Mọi bảng revision đều có cờ boolean `is_current` xác định snapshot có hiệu lực gần nhất của thực thể tại ngày xử lý.
3. **Khóa ngoại trực tiếp:** Mọi bảng con / typed projections (`notice_lot`, `notice_location`, `notice_reoffer_submission`, `project_location`) liên kết trực tiếp với bảng revision cha thông qua khóa ngoại `revision_id`.

### Danh mục Schema (Single Source of Truth)
Được định nghĩa tập trung bằng SQLAlchemy 2.0 tại `src/procurement/silver/domain/schema.py`:
- **Tier 1 (Traceability & Quality):** `observation`, `quality_issue`, `quarantine`.
- **Tier 2 (Domain Revisions):** `notice_revision`, `project_revision`, `plan_revision`, `package_revision`, `result_revision`, `opening_revision`.
- **Tier 3 (Typed Sub-entities):** `notice_lot`, `notice_location`, `notice_reoffer_submission`, `project_location`, `package_lot`, `award_lot`, `award_contractor`, `opening_submission`.

---

## 3. Chuẩn lưu trữ Lakehouse trên S3 (Hive-style Partitioning)

Dữ liệu Silver được lưu trữ theo cấu trúc chuẩn Hive Partitioning trên S3/MinIO, tương thích hoàn toàn với DuckDB, Trino, PySpark, và Apache Iceberg:

```text
s3://procurement-lakehouse/
  silver/
    ├── <table_name>/
    │     ├── source_date=2025-01-01/
    │     │     └── data.parquet
    │     ├── source_date=2025-01-02/
    │     │     └── data.parquet
    │     └── ...
    └── _reports/
          └── <resource>/
                ├── <run_id>/
                │     └── report.json
                └── latest.json
```

### Ưu điểm vượt trội:
- **Tách biệt phân vùng theo bảng:** Mỗi bảng nằm ở một prefix riêng, không bị trộn lẫn như cấu trúc candidate thư mục ngày cũ.
- **Tự động nhận diện phân vùng (Partition Pruning):** Truy vấn DuckDB chỉ cần đọc thư mục cha và bật `hive_partitioning=true`. Cột `source_date` được tự động gán kiểu `DATE` mà không cần parse timestamp.
- **Idempotency (Tính bất biến & Idempotent):** Mỗi ngày xử lý tạo đúng một file `data.parquet` chuẩn mực trên từng bảng. Ghi đè hoặc chạy lại cùng ngày sẽ thay thế nguyên tử dữ liệu của phân vùng đó mà không gây trùng lặp.

---

## 4. Hướng dẫn viết Transformer cho Resource mới

Để bổ sung một domain resource mới (ví dụ: `plan`, `bidder`, `award`), thực hiện theo 6 bước:

### Bước 1: Khai báo Schema trong `schema.py`
Mở `src/procurement/silver/domain/schema.py`:
- Định nghĩa bảng `<domain>_revision_table` với các cột chuẩn: `revision_id`, `entity_id`, `entity_type`, `observation_id`, `is_current`, các trường nghiệp vụ, và `root_json`.
- Định nghĩa các bảng con trong Tier 3 nếu có (ví dụ: `<domain>_<child>_table` chứa khóa ngoại `revision_id`).
- Đăng ký tên bảng con vào hàm `get_child_schemas()`.

### Bước 2: Tạo module Transformer
Tạo file `src/procurement/silver/domain/transformers/<resource>.py`:
- Kế thừa `BaseResourceTransformer`.
- Sử dụng decorator `@register_transformer("<bronze_table_name>")`.
- Implement các phương thức bắt buộc:
  - `extract_root(self, payload: dict) -> dict | None`: Trích xuất dictionary gốc chứa thông tin thực thể. Trả về `None` nếu payload rỗng/không đúng cấu trúc (hệ thống sẽ tự động chuyển vào `quarantine` với lý do `missing_root`).
  - `extract_source_identity(self, root: dict) -> str | None`: Lấy ID định danh nguồn. Trả về `None` nếu thiếu identity (sẽ vào `quarantine` với lý do `missing_identity`).
  - `extract_attributes(...) -> dict`: Chuẩn hóa các trường nghiệp vụ bằng bộ parser chuẩn:
    - Tiền tệ: `parse_money()` (trả về chuỗi `Decimal` chính xác 6 chữ số thập phân). Nếu có số tiền mà không có đơn vị tiền tệ, phát sinh `QualityIssue(code="unknown_currency", severity="warn")`.
    - Ngày tháng: `parse_date()` cho `Date` và `parse_iso_datetime()` cho `DateTime(timezone=True)`.
    - Boolean: `parse_boolean()` (không dùng truthiness `bool(x)` của Python để tránh nhầm chuỗi `"0"`/`"false"`).
    - Mã hành chính: `clean_pipe_code()` để bóc tách `|115|` thành `"115"`.
    - Chuỗi: `clean_text()` loại bỏ khoảng trắng thừa và rỗng.
  - `extract_references(self, root: dict, revision_id: str) -> list[ExtractedReference]`: Trích xuất liên kết sang entity khác (ví dụ: `project_id`, `plan_no`).
  - `extract_children(...) -> list[ExtractedChild]`: Trích xuất các danh sách con lặp lại. Nếu có typed projection, thiết lập `typed_table="<child_table_name>"` và `typed_attributes={...}` chứa `revision_id` và `child_id` (hoặc `<child>_id`).

### Bước 3: Đăng ký Transformer trong `__init__.py`
Import transformer mới trong `src/procurement/silver/domain/transformers/__init__.py` để tự động kích hoạt registry khi nạp package.

### Bước 4: Khai báo Contract trong `resources.py`
Mở `src/procurement/silver/application/resources.py`:
- Thêm hằng số `<RESOURCE>_MAPPING_VERSION` và `<RESOURCE>_OUTPUT_TABLES`.
- Cập nhật hàm `get_silver_resource(name: str)` để trả về đối tượng `SilverResource` tương ứng khi CLI gọi.

### Bước 5: Bổ sung lệnh CLI trong `src/procurement/cli/silver.py`
Thêm command Typer tương ứng (ví dụ `@app.command("<resource>")`) gọi hàm `run_resource(fs, bucket, "<resource>", first, last, ...)`.

### Bước 6: Viết Unit Test & Kiểm thử
Tạo test suite trong `tests/unit/silver/test_<resource>_transformer.py`:
- Test protocol conformance và registry lookup.
- Test trường hợp dữ liệu hợp lệ: kiểm tra các thuộc tính kiểu Decimal, Date, chuỗi, và bảng con.
- Test các trường hợp lỗi dữ liệu: thiếu root, thiếu ID (quarantine), sai định dạng tiền tệ (quality issue).
- Test full assembly & typed schema casting: gọi `assemble()`, `validate()`, và `typed_arrow_rows()` để đảm bảo tương thích 100% với DuckDB/PyArrow schema.

---

## 5. Hướng dẫn vận hành CLI

```powershell
# Xem danh sách các resource đã sẵn sàng
uv run python -m procurement.cli.silver list

# Chạy Silver cho Thông báo mời thầu (Notice) theo một ngày
uv run python -m procurement.cli.silver notice --date 2025-01-01

# Chạy Silver cho Notice theo khoảng ngày (tự động chạy cả 3 family: standard, vk_adb, reoffer)
uv run python -m procurement.cli.silver notice --start-date 2025-01-01 --end-date 2025-01-05

# Chạy riêng một family của Notice khi cần đối soát
uv run python -m procurement.cli.silver notice --date 2025-01-02 --family reoffer

# Chạy Silver cho Dự án đầu tư (Project)
uv run python -m procurement.cli.silver project --date 2025-08-15
uv run python -m procurement.cli.silver project --start-date 2025-08-01 --end-date 2025-08-31

# Xem báo cáo thực thi gần nhất của một resource
uv run python -m procurement.cli.silver report --resource notice
uv run python -m procurement.cli.silver report --resource project

# Reset sạch sẽ toàn bộ dữ liệu Silver và candidates trên S3 khi cần rebuild
uv run python -m procurement.cli.silver reset --all
```

---

## 6. Hướng dẫn truy vấn dữ liệu bằng DuckDB

Nhờ cấu trúc Hive-partitioning, người dùng có thể trực tiếp truy vấn dữ liệu Silver trên MinIO/S3 bằng DuckDB:

```sql
-- Cấu hình kết nối MinIO / S3
INSTALL httpfs;
LOAD httpfs;
SET s3_endpoint = 'localhost:9000';
SET s3_access_key_id = 'minioadmin';
SET s3_secret_access_key = 'minioadmin';
SET s3_use_ssl = false;
SET s3_url_style = 'path';

-- 1. Truy vấn các dự án có vốn đầu tư lớn nhất với Hive Partitioning tự động
SELECT 
    project_no, 
    title, 
    total_investment, 
    currency, 
    prov_code, 
    source_date,
    is_current
FROM read_parquet('s3://procurement-lakehouse/silver/project_revision/**/*.parquet', hive_partitioning=true)
WHERE is_current = true
ORDER BY total_investment DESC
LIMIT 10;

-- 2. Truy vấn liên kết 1-hop từ địa bàn dự án sang dự án
SELECT 
    p.project_no,
    p.title,
    l.province_name,
    l.district_name,
    l.location_name
FROM read_parquet('s3://procurement-lakehouse/silver/project_location/**/*.parquet') l
JOIN read_parquet('s3://procurement-lakehouse/silver/project_revision/**/*.parquet') p
    ON l.revision_id = p.revision_id
WHERE l.province_code = '14'
LIMIT 5;

-- 3. Kiểm tra chất lượng dữ liệu và các vấn đề phát sinh
SELECT 
    code, 
    path, 
    severity, 
    count(*) as total_issues
FROM read_parquet('s3://procurement-lakehouse/silver/quality_issue/**/*.parquet')
GROUP BY code, path, severity;
```

---

## 7. Bộ lệnh kiểm thử chất lượng mã nguồn (Quality Assurance)

Trước khi commit mã nguồn, bắt buộc chạy kiểm tra toàn bộ unit test, lint và format:

```powershell
# Chạy toàn bộ 102 unit tests (sử dụng --basetemp cục bộ trên Windows)
uv run python -m pytest tests/unit -q -p no:cacheprovider --basetemp=.tmp_pytest

# Kiểm tra lint với Ruff
uv run ruff check src/procurement/silver src/procurement/cli/silver.py tests/unit/silver

# Kiểm tra định dạng với Ruff
uv run ruff format --check src/procurement/silver src/procurement/cli/silver.py tests/unit/silver
```
