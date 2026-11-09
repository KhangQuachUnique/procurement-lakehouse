# Đặc Tả Phân Hệ Thử Nghiệm: Contract Performance (Kết Quả Thực Hiện Hợp Đồng)

- **Trạng thái:** PILOT / ISOLATED (Được cô lập thành Job riêng, không làm nghẽn luồng core)
- **Mã nguồn:** [`src/procurement/ingestion/sources/muasamcong/contract_performance/`](file:///home/soul/Dự%20án/procurement/procurement-lakehouse/src/procurement/ingestion/sources/muasamcong/contract_performance)
- **Dagster Job:** `pilot_contract_performance_job` (`group_name="bronze_pilot"` & `"silver_pilot"`)
- **Quyết định liên quan:** [ADR-0001: Khung dữ liệu Thử nghiệm Contract Performance](../decisions/0001-contract-performance.md) · [ADR-0005](../decisions/0005-dagster-daily-partition-lineage.md)

---

## 1. Mục Đích & Phạm Vi Phân Hệ

Phân hệ **Contract Performance** phục vụ mục tiêu theo dõi tình hình thực hiện hợp đồng sau khi có kết quả lựa chọn nhà thầu:
- Hợp đồng nào hoàn thành đúng tiến độ, hợp đồng nào bị chậm trễ và chênh lệch bao nhiêu ngày so với kế hoạch ban đầu?
- Đánh giá chất lượng thực hiện gói thầu của từng chủ đầu tư đối với từng nhà thầu.
- Tỉ lệ giải ngân và giá trị thanh quyết toán thực tế.

> [!WARNING]
> Dữ liệu kết quả thực hiện hợp đồng là nguồn dữ liệu không đầy đủ từ phía Cổng thông tin (chỉ những hợp đồng được chủ đầu tư chủ động đăng tải kết quả mới có dữ liệu). Do đó, phân hệ này được cô lập nghiêm ngặt để không ảnh hưởng đến tính toàn vẹn tài chính của 4 tài nguyên thầu cốt lõi.

---

## 2. Luồng Gọi API Thu Thập (Crawler Pipeline)

Toàn bộ các endpoint đều sử dụng phương thức POST có ngữ nghĩa đọc, cấu hình qua biến môi trường `MUASAMCONG_BASE_URL`:

1. **Tìm kiếm Báo cáo Kết quả:**
   - Endpoint: `/o/egp-portal-database/services/result-perform-contract/search-view-po`
   - Bộ lọc: `publicDate` (ngày công bố kết quả), phân trang `pageSize=50`.
2. **Lấy Danh sách Hợp đồng Liên kết:**
   - Endpoint: `/o/egp-portal-contractor-selection-v2/services/get-linked-list-contract-po`
   - Khớp chính xác `contractCode` và phiên bản `contractVersion`.
3. **Tra cứu ElasticSearch Hợp đồng:**
   - Endpoint: `/o/egp-portal-database/services/smart/search` (`index=es-contractor-selection`).
4. **Chi tiết Hợp đồng:**
   - Endpoint: `/o/egp-portal-contractor-selection-v2/services/econsign/contract-info/detail-by-id-for-po`.

---

## 3. Kiến Trúc Lưu Trữ & Phân Rã Bảng

### 3.1. Tầng Bronze (`raw/contract_performance/`)
- Lưu trữ Parquet thô tại `s3://procurement-lakehouse/raw/contract_performance_detail/source_date=YYYY-MM-DD/`.
- Tài sản Dagster: `bronze_contract_performance` (`group_name="bronze_pilot"`).

### 3.2. Tầng Silver (`lake.silver_v2`)
Dữ liệu thô được chuẩn hóa thành 2 bảng quan hệ nghiệp vụ:
1. **`silver_contract_revision`**:
   - Khóa chính: `contract_id`, `version_id`.
   - Lưu trữ: Mã hợp đồng, giá hợp đồng sau thuế, ngày ký, tiến độ cam kết ban đầu.
2. **`silver_contract_performance_revision`**:
   - Khóa chính: `performance_id`, `contract_id`.
   - Lưu trữ: Ngày hoàn thành thực tế, số ngày chậm trễ, đánh giá chất lượng (Đạt/Không đạt/Khen thưởng), trạng thái vi phạm hợp đồng.

---

## 4. Chính Sách Điều Phối Độc Lập Trong Dagster

Để đảm bảo hiệu năng và tránh rủi ro treo pipeline chính:
1. **Tách biệt Asset Group:** Các assets thuộc phân hệ này được gắn nhãn `group_name="bronze_pilot"` và `group_name="silver_pilot"`.
2. **Job Riêng Biệt (`pilot_contract_performance_job`):**
   - Không nằm trong luồng chạy hàng ngày của `lakehouse_end_to_end_job`.
   - Vận hành viên có thể kích hoạt chạy backfill hoặc chạy thử nghiệm phân hệ này theo nhu cầu mà không làm gián đoạn luồng chính.
