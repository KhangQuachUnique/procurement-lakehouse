# Cẩm Nang Phát Triển & Đặc Tả Kỹ Thuật Tầng Silver Lakehouse (Silver Master Specification & Dev Guide)

- **Trạng thái:** CHÍNH THỨC (Single Source of Truth - SSOT cho toàn bộ Tầng Silver)
- **Phiên bản kiến trúc:** Pure Relational 1-Hop Model (Nhánh `refactor/silver`)
- **Mã nguồn định nghĩa:** [`src/procurement/silver/domain/schema.py`](file:///home/soul/Dự%20án/procurement/procurement-lakehouse/src/procurement/silver/domain/schema.py)
- **Tài liệu tham chiếu:** [Kiến trúc tổng thể](architecture.md) · [Vận hành](operations.md) · [Phát triển](development.md)

---

## 1. Kiến Trúc Tổng Thể (Hexagonal Architecture)

Tầng Silver được thiết kế theo mô hình **Kiến trúc Lục giác (Hexagonal / Ports & Adapters Architecture)** nhằm tách biệt tuyệt đối giữa nghiệp vụ chuẩn hóa dữ liệu mua sắm công và hạ tầng công nghệ (S3, MinIO, DuckDB, PyArrow, PostgreSQL):

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
|   - Transformers: notice.py, project.py, plan.py, etc.    |                        |
|   - parsers.py (Strict decimal, timezone, pipe codes)     |                        |
|   - schema.py (SQLAlchemy 2.0 SSOT, DDL & Arrow Schemas)  |                        |
+-----------------------------------------------------------+                        |
       ^                                                                             v (Outbound Ports)
+------------------------------------+             +-----------------------------------------------+
| DRIVEN ADAPTER: BRONZE REPOSITORY  |             | DRIVEN ADAPTER: SILVER STORAGE                |
|   - ArrowBronzeRepositoryAdapter   |             |   - S3SilverCandidateAdapter                  |
|   - Queries PostgreSQL `bronze_meta|             |   - Writes Hive-partitioned Parquet files     |
|     (partitions, commits, files)   |             |   - Saves execution run & latest reports      |
|   - Streams Parquet from S3/MinIO  |             |                                               |
+------------------------------------+             +-----------------------------------------------+
```

### Luồng xử lý dữ liệu chuẩn (Processing Pipeline)
```
CLI Command → ProcessSilverService → select_committed_partitions (Bronze)
  → stream_partition_records → require_transformer (Registry)
  → transform → assemble (1-hop relational) → validate (reconciliation & FKs)
  → typed_arrow_rows (Schema cast) → S3 write_candidate_tables → save_run_report
```

---

## 2. Mô Hình Quan Hệ Thuần Túy (Pure Relational 1-Hop)

Trong kiến trúc mới, toàn bộ các bảng trung gian thừa hưởng từ mô hình RDF/Graph cũ (`entity`, `entity_revision`, `revision_observation`, `current_entity`, `relationship`, `child`, `entity_release`) đã được **xóa bỏ hoàn toàn**.

Hệ thống tổ chức theo **Mô hình Dữ liệu Quan hệ Thuần túy 1-Hop (Pure Relational 1-Hop Model)** với 4 nguyên tắc:
1. **Truy vết 1-hop trực tiếp (`observation_id`):** Mọi bảng revision đều chứa trực tiếp `observation_id` trỏ thẳng tới bảng vật lý `silver.observation`, bảo toàn xuất xứ file Parquet, số dòng và mã băm SHA-256 gốc từ Bronze.
2. **Snapshot hiệu lực trực tiếp (`is_current`):** Quản lý phiên bản lịch sử theo SCD Type 2 với cờ `is_current` xác định bản ghi có hiệu lực mới nhất.
3. **Khóa ngoại trực tiếp:** Mọi bảng con liên kết trực tiếp với bảng cha thông qua khóa ngoại `revision_id`.
4. **Phân rã mảng lồng triệt để:** Bóc tách các mảng con (địa bàn, lô thầu, nhà thầu) thành các bảng con kiểu độc lập.

---

## 3. Danh Mục 15 Bảng Chuẩn Silver Trong 3 Tầng (Tiers)

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        MÔ HÌNH DỮ LIỆU SILVER LAKEHOUSE CHUẨN                         │
├────────────────────────────────────────────────────────────────────────────────────────┤
│ [TIER 1: AUDIT & QUẢN TRỊ]    [TIER 2: CORE REVISIONS (6)]   [TIER 3: 1-HOP SUB-ENTITIES (8)]│
│  - silver.observation          - project_revision             - project_location       │
│  - silver.quality_issue        - plan_revision      (KHLCNT)  - package_lot   (KHLCNT) │
│  - silver.quarantine           - package_revision   (KHLCNT)  - package_loc   (KHLCNT) │
│                                - notice_revision              - notice_lot             │
│                                - result_revision              - notice_location        │
│                                - opening_revision             - notice_reoffer_sub     │
│                                                               - award_lot              │
│                                                               - award_contractor       │
│                                                               - opening_submission     │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

### 3.1. Tier 1: Quản Trị & Truy Vết Dữ Liệu (3 bảng)
- **`silver.observation`**: Lưu trữ toàn bộ siêu dữ liệu của từng bản ghi Bronze đọc được (`observation_id`, `resource`, `source_date`, `run_id`, `table_name`, `file_key`, `file_sha256`, `row_ordinal`, `observed_at`, `source_id`, `source_version`, `content_hash`, `payload_json`).
- **`silver.quality_issue`**: Sổ nhật ký ghi nhận các bất thường chất lượng dữ liệu (`issue_id`, `observation_id`, `code`, `path`, `severity`). Không làm sập pipeline mà ghi cảnh báo để phục vụ kiểm toán dữ liệu.
- **`silver.quarantine`**: Khu vực cách ly các bản ghi lỗi nhận diện (`observation_id`, `reason`: `missing_root`, `missing_identity`). Bảo vệ kho Silver khỏi dữ liệu rác/ghost records.

### 3.2. Tier 2: Thực Thể Phiên Bản Lõi (6 bảng Revisions)
Mỗi bảng revision đại diện cho một phiên bản snapshot bất biến của thực thể nghiệp vụ:
- **`silver.project_revision`**: Dự án đầu tư xây dựng / mua sắm (`project_no`, `title`, `investor_code`, `investor_name`, `buyer_name`, `total_investment`, `currency`, `decision_no`, `decision_date`, `public_date`...).
- **`silver.plan_revision`**: Kế hoạch lựa chọn nhà thầu cha (`plan_no`, `title`, `investor_name`, `project_id`, `project_no`, `plan_type`, `total_investment`, `decision_no`, `decision_date`, `bid_pack`, `public_date`...).
- **`silver.package_revision`**: Gói thầu trong KHLCNT (`package_no`, `title`, `plan_no`, `plan_id`, `bid_price`, `process_apply`, `bid_field`, `bid_form`, `bid_mode`, `contract_type`, `is_domestic`, `is_internet`, `bid_start_year`, `bid_start_quarter`, `public_date`...).
- **`silver.notice_revision`**: Thông báo mời thầu TBMT (hợp đồng `notice-2` hợp nhất Standard, VK/ADB, Reoffer) (`notify_no`, `notify_version`, `title`, `procuring_entity_name`, `buyer_name`, `bid_open_date`, `bid_close_date`, `bid_price`, `guarantee_amount`, `contract_type`, `is_internet`...).
- **`silver.result_revision`**: Kết quả lựa chọn nhà thầu (`result_id`, `notify_no`, `package_no`, `total_winning_price`, `decision_no`, `decision_date`, `decision_agency`...).
- **`silver.opening_revision`**: Biên bản mở thầu (`opening_id`, `notify_no`, `bid_mode`, `actual_open_date`, `total_bidders`...).

### 3.3. Tier 3: Bảng Con Quan Hệ 1-Hop (8 bảng Sub-entities)
Các bảng con liên kết trực tiếp với bảng revision cha qua khóa ngoại `revision_id`:
- **`silver.project_location`**: Địa bàn thực hiện dự án (bóc tách từ mảng JSON `plocation`).
- **`silver.package_lot`**: Chi tiết phần/lô thầu thuộc gói thầu trong KHLCNT (bóc tách từ mảng `bidpBidLotList[]`).
- **`silver.package_location`**: Địa bàn thực hiện gói thầu trong KHLCNT (bóc tách từ mảng `bidLocation[]`).
- **`silver.notice_lot`**: Danh sách phần/lô mời thầu trong TBMT (từ `lotDTOList` hoặc `bidpBidLotList`).
- **`silver.notice_location`**: Địa bàn thực hiện gói thầu trong TBMT (từ `bidpBidLocationList`).
- **`silver.notice_reoffer_submission`**: Danh sách các lượt nộp chào giá trực tuyến rút gọn (từ `contractorsResult` và `contractorsResultAll`).
- **`silver.award_lot`**: Quyết định trao thầu theo từng lô thầu (`lot_no`, `lot_name`, `lot_price`, `winning_price`, `winning_code`, `status`).
- **`silver.award_contractor`**: Danh sách nhà thầu trúng thầu & dự thầu (`contractor_code`, `contractor_name`, `tax_code`, `country`, `is_consortium`, `venture_name`, `role`, `awarded_price`, `evaluation_rank`).
- **`silver.opening_submission`**: Danh sách hồ sơ dự thầu nộp tại lễ mở thầu (`contractor_code`, `bid_price`, `final_bid_price`, `tech_result`).

---

## 4. Trọng Tâm KHLCNT: 2 Bảng Bronze $\rightarrow$ 4 Bảng Silver

Tài nguyên **KHLCNT (`khlcnt`)** gồm 2 bảng Bronze (`khlcnt_plan_detail` và `khlcnt_bid_package_detail`), được phân rã thành **đúng 4 bảng Silver**:

```
khlcnt_plan_detail (Bronze)
└── bidPoBidpPlanProjectDetailView
    └── 1. silver.plan_revision (Thực thể Kế hoạch LCNT cha - 18 cột)

khlcnt_bid_package_detail (Bronze)
├── Top-level Package Attributes
│   └── 2. silver.package_revision (Thực thể Gói thầu trong KHLCNT - 24 cột)
├── bidLocation[] (Array)
│   └── 3. silver.package_location (Bảng con Địa bàn thực hiện gói thầu - 8 cột)
└── bidpBidLotList[] (Array)
    └── 4. silver.package_lot (Bảng con Phần / Lô thầu chi tiết - 12 cột)
```

| Tên Bảng Silver | Cấp Độ | Khóa Chính (PK) | Khóa Ngoại 1-Hop (FK) | Nguồn Bronze | Vai Trò Nghiệp Vụ |
| :--- | :---: | :--- | :--- | :--- | :--- |
| **`silver.plan_revision`** | Cha | `revision_id` | `observation_id`, `project_id` | `khlcnt_plan_detail` | Quản lý thông tin phê duyệt kế hoạch, tổng mức đầu tư, chủ đầu tư, quyết định phê duyệt. |
| **`silver.package_revision`** | Cha | `revision_id` | `observation_id`, `plan_id` | `khlcnt_bid_package_detail` | Quản lý từng gói thầu: giá gói, hình thức, phương thức, thời gian bắt đầu, loại hợp đồng. |
| **`silver.package_lot`** | Con | `lot_id` | `revision_id` | `khlcnt_bid_package_detail.bidpBidLotList[]` | Quản lý phần/lô thầu chia nhỏ hoặc danh mục thuốc/vật tư y tế trong gói thầu. |
| **`silver.package_location`**| Con | `location_id` | `revision_id` | `khlcnt_bid_package_detail.bidLocation[]` | Quản lý các tỉnh, huyện, xã nơi triển khai thực hiện gói thầu. |

---

## 5. Chuẩn Lưu Trữ Lakehouse S3 & Tính Idempotency

### 5.1. Định dạng Hive-Partitioning trên S3/MinIO
Toàn bộ 15 bảng Silver được lưu trữ theo cấu trúc thư mục phân vùng Hive trên S3:
```text
s3://procurement-lakehouse/silver/
  ├── <table_name>/
  │     ├── source_date=2025-01-01/
  │     │     └── data.parquet
  │     ├── source_date=2025-01-02/
  │     │     └── data.parquet
  │     └── ...
  └── _reports/<resource>/<run_id>/report.json
```

### 5.2. Nguyên lý Idempotency (Chạy lại an toàn, không trùng lặp)
- **Atomic Partition Overwrite:** Khi xử lý lại ngày $D$, hệ thống ghi đè nguyên tử file `data.parquet` trên phân vùng `source_date=D`. Không bao giờ tạo ra 2 file song song cho cùng một ngày.
- **Deterministic Key Hashing:** Mọi khóa chính (`revision_id`, `child_id`) đều được băm xác định bằng SHA-256 từ nội dung bản ghi, đảm bảo tính bất biến tuyệt đối.
- **Hỗ trợ Apache Iceberg Format v2:** Các bảng Revision lõi được cấu hình phân vùng song song theo `public_date` (DATE) phục vụ tăng tốc độ đọc phân tích OLAP.

---

## 6. Hướng Dẫn Vận Hành CLI

Giao diện dòng lệnh định hướng theo tài nguyên (resource-first):

```powershell
# Xem danh sách các resource sẵn sàng
uv run python -m procurement.cli.silver list

# Chạy Silver cho Thông báo mời thầu (Notice - Standard, VK/ADB, Reoffer)
uv run python -m procurement.cli.silver notice --date 2025-01-01
uv run python -m procurement.cli.silver notice --start-date 2025-01-01 --end-date 2025-01-05
uv run python -m procurement.cli.silver notice --date 2025-01-02 --family reoffer

# Chạy Silver cho Dự án đầu tư (Project)
uv run python -m procurement.cli.silver project --date 2025-08-15
uv run python -m procurement.cli.silver project --start-date 2025-08-01 --end-date 2025-08-31

# Chạy Silver cho Kết quả lựa chọn nhà thầu (Result)
uv run python -m procurement.cli.silver result --date 2022-09-16
uv run python -m procurement.cli.silver result --start-date 2022-09-01 --end-date 2022-09-30

# Kiểm tra báo cáo thực thi gần nhất của resource
uv run python -m procurement.cli.silver report --resource notice
uv run python -m procurement.cli.silver report --resource project
uv run python -m procurement.cli.silver report --resource contractor_result

# Reset dữ liệu Silver trên S3 khi cần rebuild toàn diện
uv run python -m procurement.cli.silver reset --all
```

---

## 7. Truy Vấn Dữ Liệu Với DuckDB

Nhờ cấu trúc Hive-partitioning, người dùng có thể truy vấn trực tiếp Lakehouse bằng DuckDB SQL:

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
    bid_open_date,
    bid_close_date,
    is_current
FROM read_parquet('s3://procurement-lakehouse/silver/notice_revision/**/*.parquet', hive_partitioning=true)
WHERE is_current = true
ORDER BY bid_price DESC
LIMIT 10;

-- 2. Truy vấn liên kết 1-hop: Gói thầu KHLCNT nối với Địa bàn thực hiện
SELECT 
    pkg.package_no,
    pkg.title,
    pkg.bid_price,
    loc.province_name,
    loc.district_name
FROM read_parquet('s3://procurement-lakehouse/silver/package_revision/**/*.parquet') pkg
JOIN read_parquet('s3://procurement-lakehouse/silver/package_location/**/*.parquet') loc
    ON pkg.revision_id = loc.revision_id
LIMIT 10;

-- 3. Tổng hợp các cảnh báo chất lượng dữ liệu
SELECT 
    code, 
    path, 
    severity, 
    count(*) as total_occurrences
FROM read_parquet('s3://procurement-lakehouse/silver/quality_issue/**/*.parquet')
GROUP BY code, path, severity
ORDER BY total_occurrences DESC;
```

---

## 8. Hướng Dẫn Mở Rộng & Phát Triển Transformer Mới

Khi phát triển thêm transformer cho tài nguyên mới, tuân thủ quy trình 6 bước:
1. **Khai báo Schema:** Thêm bảng `<domain>_revision_table` và các bảng con kiểu trong [`src/procurement/silver/domain/schema.py`](file:///home/soul/Dự%20án/procurement/procurement-lakehouse/src/procurement/silver/domain/schema.py).
2. **Viết Transformer:** Kế thừa `BaseResourceTransformer` tại `src/procurement/silver/domain/transformers/<resource>.py`. Sử dụng bộ hàm parser chuẩn (`parse_money`, `parse_date`, `parse_boolean`, `clean_pipe_code`, `clean_text`).
3. **Đăng ký Transformer:** Thêm vào `src/procurement/silver/domain/transformers/__init__.py`.
4. **Khai báo Contract:** Cập nhật bảng đầu ra trong `src/procurement/silver/application/resources.py`.
5. **Thêm CLI Command:** Bổ sung lệnh trong `src/procurement/cli/silver.py`.
6. **Kiểm thử:** Viết unit test trong `tests/unit/silver/` kiểm tra protocol, quarantine và schema typing.
