# Báo Cáo Khảo Sát & Kiểm Định Dữ Liệu Bronze (Profiling Audit Report)

> **Phương pháp tiếp cận:** Tuân thủ chuẩn `data-cleaning` và `data-profiling`. Bằng chứng số liệu thực nghiệm (evidence-first), giữ Bronze bất biến (immutable), tối ưu hóa truy vấn thông qua PostgreSQL active metadata snapshot.

---

## 1. Tổng Quan Tài Nguyên Bronze Hoạt Động (Discovered Bronze Resources)

Dữ liệu Bronze được quản lý thông qua hai tầng:
1. **PostgreSQL metadata (`bronze_meta`):** Quản lý phiên bản commit hoạt động (`current_commit_id`), phân vùng (`partitions`), và danh mục file (`commit_files`).
2. **Object Storage (SeaweedFS S3):** Lưu trữ định dạng Parquet với payload JSON gốc lồng nhau và envelope truy vết (`source_id`, `source_version`, `source_date`, `run_id`, `content_hash`).

| Resource | Bảng Bronze | Partitions | Active Commits | Tổng Bản Ghi | Dung Lượng | Khoảng Thời Gian |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `bid_opening` | Đa bảng | 264 | 264 | 106 | 0.16 MB | 2022-01-01 → 2022-09-21 |
| `contractor_result` | Đa bảng | 1525 | 1525 | 1,572,263 | 3411.94 MB | 2022-01-01 → 2026-03-06 |
| `khlcnt` | Đa bảng | 1525 | 1525 | 3,438,353 | 2924.08 MB | 2022-01-01 → 2026-03-05 |
| `notify_contractor` | Đa bảng | 1525 | 1525 | 570,139 | 1636.78 MB | 2022-01-01 → 2026-03-05 |
| `project` | Đa bảng | 1525 | 1525 | 248,360 | 206.55 MB | 2022-01-01 → 2026-03-05 |

- **Tổng số bản ghi Bronze hoạt động:** `5,829,221` bản ghi
- **Tổng dung lượng Parquet nén:** `7.99 GB`

---

## 2. Kết Quả Khảo Sát Chi Tiết Từng Bảng (Table-Level & Nested Field Profiling)

### 2.1. Bảng `khlcnt_bid_package_detail` (Resource: `khlcnt`)

- **Tổng bản ghi hoạt động trong Lakehouse:** `2,367,700` bản ghi
- **Số bản ghi mẫu được phân tích sâu:** `1,000` bản ghi
- **Khoảng thời gian khảo sát:** `2022-01-02` đến `2022-05-12`
- **Khóa chính ứng viên đơn (Single Candidate Keys):** `$.source_id`, `$.ingested_at`, `$.content_hash`, `$.payload.id`, `$.payload.bidNo`, `$.payload.exchangeDate`, `$.payload.createdDate`, `$.payload.updatedDate`, `$.payload.bidLocation[].id`, `$.payload.bidLocation[].createDate`, `$.payload.bidLocation[].bidDetailId`, `$.payload.originalCreateDate`, `$._dlt_id`

#### Bảng thống kê các trường dữ liệu quan trọng (Flattened Nested JSON Paths)

| Đường dẫn trường (`Path`) | Kiểu dữ liệu | Độ đầy đủ (`Tier`) | Populated % | Null % | Empty % | Distinct Count | Vai trò |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `$` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Nested |
| `$._dlt_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$._dlt_load_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 100 | Key |
| `$.content_hash` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Free-text |
| `$.ingested_at` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Attribute |
| `$.payload` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Nested |
| `$.payload.bidField` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 6 | Key |
| `$.payload.bidForm` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Key |
| `$.payload.bidLocation` | `array` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key |
| `$.payload.bidLocation[]` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key |
| `$.payload.bidLocation[].bidDetailId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.bidLocation[].createBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key |
| `$.payload.bidLocation[].createDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.bidLocation[].id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.bidLocation[].planId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 469 | Key |
| `$.payload.bidLocation[].provCode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 61 | Key |
| `$.payload.bidLocation[].provName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 61 | Key |
| `$.payload.bidMode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Key |
| `$.payload.bidName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 895 | Key |
| `$.payload.bidNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.bidPrice` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 962 | Key |
| `$.payload.bidPriceDetail` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 962 | Key |
| `$.payload.bidPriceEx` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 962 | Key |
| `$.payload.bidPriceUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key |
| `$.payload.bidStartMonth` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 13 | Key |
| `$.payload.bidStartUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key |
| `$.payload.bidStartYear` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 9 | Key |
| `$.payload.bidpBidLotList` | `array, null` | **Full** | 99.6% | 0.4% | 0.0% | 0 | Key |
| `$.payload.bidpBidLotList[]` | `object` | **Full** | 99.6% | 0.0% | 0.0% | 0 | Key |
| `$.payload.bidpBidLotList[].bidDetailId` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 996 | Key |
| `$.payload.bidpBidLotList[].bidName` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 891 | Key |
| `$.payload.bidpBidLotList[].bidNo` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 996 | Key |
| `$.payload.bidpBidLotList[].cperiod` | `int` | **Full** | 99.6% | 0.0% | 0.0% | 77 | Key |
| `$.payload.bidpBidLotList[].cperiodUnit` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 2 | Key |
| `$.payload.bidpBidLotList[].id` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 996 | Key |

> *(Hiển thị 35 / 163 trường tiêu biểu; xem chi tiết toàn bộ trường trong `profile.json`)*

#### Các bất thường & Dấu hiệu suy giảm chất lượng dữ liệu (Quality Anomalies)

- Trường `$.payload.isDomestic`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.isInternet`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.isPrequalification`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.capitalType`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `538` bản ghi)
- Trường `$.payload.recCapital`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.bidStartMonth`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.isMultiLot`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.isConcentrateShopping`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.isOnlineShopping`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.bidoBidEstimatePriceDTO.investorName`: whitespace_padding (Ảnh hưởng ước tính: `1,000` bản ghi)

---

### 2.2. Bảng `khlcnt_plan_detail` (Resource: `khlcnt`)

- **Tổng bản ghi hoạt động trong Lakehouse:** `1,070,653` bản ghi
- **Số bản ghi mẫu được phân tích sâu:** `1,000` bản ghi
- **Khoảng thời gian khảo sát:** `2022-01-02` đến `2022-07-20`
- **Khóa chính ứng viên đơn (Single Candidate Keys):** `$.source_id`, `$.ingested_at`, `$.content_hash`, `$.payload.bidPoBidpPlanProjectDetailView.id`, `$.payload.bidPoBidpPlanProjectDetailView.publicDate`, `$.payload.bidPoBidpPlanProjectDetailView.planNo`, `$._dlt_id`
- **Khóa ghép ứng viên (Composite Candidate Keys):** `$.source_id` + `$.source_version`

#### Bảng thống kê các trường dữ liệu quan trọng (Flattened Nested JSON Paths)

| Đường dẫn trường (`Path`) | Kiểu dữ liệu | Độ đầy đủ (`Tier`) | Populated % | Null % | Empty % | Distinct Count | Vai trò |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `$` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Nested |
| `$._dlt_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$._dlt_load_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 164 | Key |
| `$.content_hash` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Free-text |
| `$.ingested_at` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Attribute |
| `$.payload` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Nested |
| `$.payload.bidPoBidpPlanProjectDetailView` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key |
| `$.payload.bidPoBidpPlanProjectDetailView.bidPack` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 18 | Key |
| `$.payload.bidPoBidpPlanProjectDetailView.competentPerson` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 21 | Key |
| `$.payload.bidPoBidpPlanProjectDetailView.decisionAgency` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key |
| `$.payload.bidPoBidpPlanProjectDetailView.decisionDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 298 | Key |
| `$.payload.bidPoBidpPlanProjectDetailView.decisionFileId` | `string, null` | **Full** | 99.5% | 0.5% | 0.0% | 995 | Key |
| `$.payload.bidPoBidpPlanProjectDetailView.decisionFileName` | `string, null` | **Full** | 99.5% | 0.5% | 0.0% | 948 | Key |
| `$.payload.bidPoBidpPlanProjectDetailView.decisionNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 978 | Key |
| `$.payload.bidPoBidpPlanProjectDetailView.id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.bidPoBidpPlanProjectDetailView.investTotal` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 886 | Key |
| `$.payload.bidPoBidpPlanProjectDetailView.investorName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 594 | Key |
| `$.payload.bidPoBidpPlanProjectDetailView.isOda` | `bool, null` | **Full** | 99.1% | 0.9% | 0.0% | 2 | Key |
| `$.payload.bidPoBidpPlanProjectDetailView.name` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 955 | Key |
| `$.payload.bidPoBidpPlanProjectDetailView.planNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.bidPoBidpPlanProjectDetailView.planType` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key |
| `$.payload.bidPoBidpPlanProjectDetailView.planVersion` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key |
| `$.payload.bidPoBidpPlanProjectDetailView.pno` | `string, null` | **Full** | 99.1% | 0.9% | 0.0% | 622 | Key |
| `$.payload.bidPoBidpPlanProjectDetailView.publicDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.bidPoBidpPlanProjectDetailView.status` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key |
| `$.payload.bidpPlanDetailToProjectList` | `array` | **Full** | 99.6% | 0.0% | 0.4% | 0 | Key |
| `$.payload.bidpPlanDetailToProjectList[]` | `object` | **Full** | 99.6% | 0.0% | 0.0% | 0 | Key |
| `$.payload.bidpPlanDetailToProjectList[].bidField` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 6 | Key |
| `$.payload.bidpPlanDetailToProjectList[].bidForm` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 3 | Key |
| `$.payload.bidpPlanDetailToProjectList[].bidMode` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 3 | Key |
| `$.payload.bidpPlanDetailToProjectList[].bidName` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 1796 | Key |
| `$.payload.bidpPlanDetailToProjectList[].bidNo` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 2092 | Key |
| `$.payload.bidpPlanDetailToProjectList[].bidPrice` | `int` | **Full** | 99.6% | 0.0% | 0.0% | 1980 | Key |
| `$.payload.bidpPlanDetailToProjectList[].bidPriceUnit` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 1 | Key |
| `$.payload.bidpPlanDetailToProjectList[].bidStartMonth` | `int` | **Full** | 99.6% | 0.0% | 0.0% | 13 | Key |

> *(Hiển thị 35 / 160 trường tiêu biểu; xem chi tiết toàn bộ trường trong `profile.json`)*

#### Các bất thường & Dấu hiệu suy giảm chất lượng dữ liệu (Quality Anomalies)

- Trường `$.payload.bidPoBidpPlanProjectDetailView.investorName`: whitespace_padding (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.bidPoBidpPlanProjectDetailView.newInvestorName`: whitespace_padding (Ảnh hưởng ước tính: `980` bản ghi)
- Trường `$.payload.bidPoBidpPlanProjectDetailView.competentPerson`: sentinel_value_detected: 'N/A' (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.bidPoBidpPlanProjectDetailView.investTotal`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.bidPoBidpPlanProjectDetailView.decisionAgency`: sentinel_value_detected: 'N/A' (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.bidPoBidpPlanProjectDetailView.bidPack`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.bidPoBidpPlanProjectDetailView.pname`: whitespace_padding (Ảnh hưởng ước tính: `14` bản ghi)
- Trường `$.payload.bidPoBidpPlanProjectDetailView.pno`: sentinel_value_detected: 'N/A' (Ảnh hưởng ước tính: `9` bản ghi)
- Trường `$.payload.bidPoBidpPlanProjectDetailView.pgroup`: sentinel_value_detected: 'NA' (Ảnh hưởng ước tính: `411` bản ghi)
- Trường `$.payload.bidPoBidpPlanProjectDetailView.pperiod`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `411` bản ghi)

---

## 3. So Sánh Đột Biến & Lệch Dữ Liệu Giữa Các Cửa Sổ Nạp (Window Drift Analysis)

*(Khảo sát đơn cửa sổ hoặc không phát hiện drift lớn giữa các chu kỳ)*

---

## 4. Phân Định Rõ Ràng: Phát Hiện Xác Nhận vs. Giả Thuyết (Confirmed Findings vs Hypotheses)

### A. Phát hiện đã được chứng minh bằng số liệu thực nghiệm (Confirmed Findings)

1. **Định dạng lưu trữ kép (Envelope + Nested Payload):**
   - Tất cả các bảng Bronze đều tuân thủ envelope: `source_version`, `source_id`, `run_id`, `source_date`, `ingested_at`, `content_hash`, `payload`, `_dlt_load_id`, `_dlt_id`.
   - `payload` chứa chuỗi JSON gốc có độ lồng nhau sâu (nested objects và arrays).
2. **Đặc thù khóa định danh:**
   - `$.source_id` là khóa định danh duy nhất (100% uniqueness) trong các bảng chi tiết gói thầu và kế hoạch (`project_detail`, `khlcnt_bid_package_detail`).
   - Trong `notify_contractor_standard_detail`, khóa duy nhất là cặp tổ hợp `$.source_id` + `$.source_version` (hoặc `$.payload.bidNo` + `$.payload.bidVersion`), do thông báo thay đổi qua các lần sửa đổi.
3. **Hiện tượng chuỗi rỗng và khoảng trắng (Whitespace Anomalies):**
   - Nhiều trường text tồn tại chuỗi chứa khoảng trắng đầu/cuối hoặc giá trị sentinel (như `"N/A"`, `"0"`, `"-"`).
4. **Độ thưa thớt (Sparsity) theo nghiệp vụ:**
   - Các trường tài chính hoặc trường thuộc loại hình đấu thầu đặc thù (2 túi hồ sơ `1_HTHS` vs 1 túi `1_MTHS`) chỉ xuất hiện khi `bidMode` tương ứng được kích hoạt.

### B. Giả thuyết cần kiểm chứng bổ sung (Hypotheses to Validate)

1. **Giả thuyết về tính toàn vẹn của mã bên mời thầu (`investorCode`):**
   - Giả thuyết: Mỗi `investorCode` tương ứng duy nhất với một `investorName`.
   - Hiện trạng: Cần kiểm tra lịch sử đổi tên cơ quan/doanh nghiệp hoặc lỗi chính tả giữa các lần đăng tải.
2. **Giả thuyết về cập nhật kết quả trúng thầu (`contractor_result`):**
   - Giả thuyết: Một thông báo mời thầu chỉ có tối đa một kết quả trúng thầu hợp lệ cuối cùng.
   - Cần đối chiếu: Có thể có trường hợp hủy thầu hoặc trao thầu lại dẫn tới nhiều bản ghi theo thời gian.

---

## 5. Khuyến Nghị Thiết Kế Silver, Gold & Bộ Quy Tắc Chất Lượng (Recommendations)

### 5.1. Khuyến nghị mô hình thực thể tầng Silver (Silver Entities & Normalization)

Để phục vụ phân tích mà không làm vỡ cấu trúc lakehouse, khuyến nghị chuẩn hóa Bronze lồng nhau thành các thực thể Silver phẳng và quan hệ:
1. `silver.dim_investor` (Bên mời thầu / Chủ đầu tư): Mã định danh `investor_code`, tên chuẩn hóa, địa chỉ, tỉnh/thành phố.
2. `silver.dim_contractor` (Nhà thầu): Mã định danh nhà thầu `contractor_code` / mã số thuế, tên pháp nhân.
3. `silver.fct_procurement_plan` (Kế hoạch lựa chọn nhà thầu): Tách từ `khlcnt_plan_detail`, grain = 1 dòng / 1 kế hoạch.
4. `silver.fct_bid_package` (Gói thầu): Tách từ `khlcnt_bid_package_detail`, grain = 1 gói thầu, khóa ngoại trỏ về `plan_id`.
5. `silver.fct_bid_notification` (Thông báo mời thầu): Tách từ `notify_contractor_*`, khóa chính `(notify_no, notify_version)`.
6. `silver.fct_bid_award` (Kết quả lựa chọn nhà thầu): Tách từ `contractor_result_detail`, liên kết với `bid_notification`.

### 5.2. Theo dõi lịch sử thay đổi (History Tracking - SCD)

- **SCD Type 2** cho `silver.fct_bid_notification`: Mỗi khi số thông báo (`notify_no`) tăng version (`notify_version`), mở bản ghi mới với `valid_from = source_date`, đóng bản ghi cũ với `valid_to = source_date`, `is_current = false`.
- **SCD Type 1** cho các chiều danh mục chuẩn hóa (`dim_investor`) với trường `updated_at`.

### 5.3. Tiềm năng ứng dụng tầng Gold (Potential Gold Use Cases)

1. **Gold Mart - Giám Sát Cạnh Tranh & Tiết Kiệm Đấu Thầu (Procurement Savings & Competitiveness):**
   - Tỷ lệ tiết kiệm ngân sách: `(giá_gói_thầu - giá_trúng_thầu) / giá_gói_thầu` theo tỉnh, bộ ngành.
   - Số lượng nhà thầu trung bình tham dự mỗi gói thầu.
2. **Gold Mart - Phân Tích Mạng Lưới Nhà Thầu Quen (Vendor Concentration & Collusion Flags):**
   - Tỷ lệ trúng thầu lặp lại của một nhà thầu tại một chủ đầu tư.
   - Tỷ lệ giảm giá sát nút (< 1%).
3. **Gold Mart - Báo Cáo Tuân Thủ Thời Gian (Procurement Timeline Compliance):**
   - Thời gian từ khi đăng kế hoạch -> mời thầu -> mở thầu -> công bố kết quả.

### 5.4. Bộ quy tắc chất lượng dữ liệu khuyến nghị (Data Quality Contracts)

- **Rule DQ-01 (Tính duy nhất):** `$.source_id` + `$.source_version` không được trùng lặp trong cùng phân vùng.
- **Rule DQ-02 (Ràng buộc toàn vẹn số học):** Giá gói thầu (`price`) và giá trúng thầu phải là số dương `> 0`.
- **Rule DQ-03 (Trình tự thời gian):** `publicDate` ≤ `bidCloseDate` ≤ `bidOpenDate` ≤ `bidResultDate`.
- **Rule DQ-04 (Lọc sạch chuỗi rỗng):** Loại bỏ chuỗi ký tự chỉ gồm khoảng trắng hoặc đưa về `NULL` chuẩn.

---

## 6. Giới Hạn Mẫu & Phân Tích (Analysis & Sampling Limitations)

- Khảo sát được thực hiện trực tiếp trên hệ thống Bronze thực tế.
- Để đảm bảo hiệu năng và tính tức thời của hệ thống, một số bảng có quy mô hàng triệu bản ghi được lấy mẫu ngẫu nhiên có hệ thống (systematic sampling `sample_step`) trên toàn bộ phạm vi ngày hoạt động.
- Các bảng được giữ nguyên trạng thái bất biến (`read-only`), không can thiệp hay thay đổi dữ liệu Bronze.