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

### 2.2. Bảng `contractor_result_detail` (Resource: `contractor_result`)

- **Tổng bản ghi hoạt động trong Lakehouse:** `1,572,263` bản ghi
- **Số bản ghi mẫu được phân tích sâu:** `1,000` bản ghi
- **Khoảng thời gian khảo sát:** `2022-09-16` đến `2022-09-28`
- **Khóa chính ứng viên đơn (Single Candidate Keys):** `$.source_id`, `$.ingested_at`, `$.content_hash`, `$.payload.bideContractorInputResultDTO.id`, `$.payload.bideContractorInputResultDTO.notifyNo`, `$.payload.bideContractorInputResultDTO.bidId`, `$.payload.bideContractorInputResultDTO.bidNo`, `$.payload.bideContractorInputResultDTO.decisionFileId`, `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorResultId`, `$.payload.bideContractorInputResultDTO.lotResultDTO[].bidId`, `$.payload.bideContractorInputResultDTO.lotResultDTO[].bidNo`, `$.payload.bideContractorInputResultDTO.createdDate`, `$.payload.bideContractorInputResultDTO.lastModifiedDate`, `$.payload.bidpPlanDetailDTO.id`, `$.payload.bidpPlanDetailDTO.bidNo`, `$.payload.bidpPlanDetailDTO.bidpBidLotList[].bidNo`, `$.payload.bidpPlanDetailDTO.bidpBidLotList[].bidDetailId`, `$.payload.bidpPlanDetailDTO.detailLotList[].bidNo`, `$.payload.bidpPlanDetailDTO.detailLotList[].bidDetailId`, `$.payload.bidpPlanDetailDTO.createdDate`, `$.payload.bidpPlanDetailDTO.latestAssignId`, `$._dlt_id`
- **Khóa ghép ứng viên (Composite Candidate Keys):** `$.source_id` + `$.source_version`

#### Bảng thống kê các trường dữ liệu quan trọng (Flattened Nested JSON Paths)

| Đường dẫn trường (`Path`) | Kiểu dữ liệu | Độ đầy đủ (`Tier`) | Populated % | Null % | Empty % | Distinct Count | Vai trò |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `$` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Nested |
| `$._dlt_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$._dlt_load_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 28 | Key |
| `$.content_hash` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Free-text |
| `$.ingested_at` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Attribute |
| `$.payload` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Nested |
| `$.payload.bideContractorInputResultDTO` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key |
| `$.payload.bideContractorInputResultDTO.bidEstimatePriceUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key |
| `$.payload.bideContractorInputResultDTO.bidField` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 5 | Key |
| `$.payload.bideContractorInputResultDTO.bidForm` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 6 | Key |
| `$.payload.bideContractorInputResultDTO.bidId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.bideContractorInputResultDTO.bidMode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key |
| `$.payload.bideContractorInputResultDTO.bidName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 832 | Key |
| `$.payload.bideContractorInputResultDTO.bidNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.bideContractorInputResultDTO.bidPrice` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 885 | Key |
| `$.payload.bideContractorInputResultDTO.bidPriceDetail` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 885 | Key |
| `$.payload.bideContractorInputResultDTO.bidPriceUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key |
| `$.payload.bideContractorInputResultDTO.cperiod` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 42 | Key |
| `$.payload.bideContractorInputResultDTO.cperiodUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key |
| `$.payload.bideContractorInputResultDTO.createdBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 423 | Key |
| `$.payload.bideContractorInputResultDTO.createdDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.bideContractorInputResultDTO.ctype` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 4 | Key |
| `$.payload.bideContractorInputResultDTO.decisionAgency` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 506 | Key |
| `$.payload.bideContractorInputResultDTO.decisionDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 966 | Key |
| `$.payload.bideContractorInputResultDTO.decisionFileId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.bideContractorInputResultDTO.decisionFileName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 984 | Key |
| `$.payload.bideContractorInputResultDTO.decisionNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 990 | Key |
| `$.payload.bideContractorInputResultDTO.id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.bideContractorInputResultDTO.inputMethod` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key |
| `$.payload.bideContractorInputResultDTO.investorCode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 406 | Key |
| `$.payload.bideContractorInputResultDTO.investorName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 406 | Key |
| `$.payload.bideContractorInputResultDTO.isDomestic` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key |
| `$.payload.bideContractorInputResultDTO.isInternet` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key |
| `$.payload.bideContractorInputResultDTO.isMultiLot` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key |
| `$.payload.bideContractorInputResultDTO.lastModifiedBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 423 | Key |

> *(Hiển thị 35 / 317 trường tiêu biểu; xem chi tiết toàn bộ trường trong `profile.json`)*

#### Các bất thường & Dấu hiệu suy giảm chất lượng dữ liệu (Quality Anomalies)

- Trường `$.payload.bideContractorInputResultDTO.procuringEntityName`: whitespace_padding (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.bideContractorInputResultDTO.investorName`: whitespace_padding (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.bideContractorInputResultDTO.isInternet`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].taxNation`: whitespace_padding (Ảnh hưởng ước tính: `4` bản ghi)
- Trường `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].taxCode`: whitespace_padding (Ảnh hưởng ước tính: `14` bản ghi)
- Trường `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].repIdNo`: whitespace_padding (Ảnh hưởng ước tính: `121` bản ghi)
- Trường `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].orgCode`: whitespace_padding (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].orgFullname`: whitespace_padding (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.bideContractorInputResultDTO.lotResultDTO[].winningCode`: whitespace_padding (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.bideContractorInputResultDTO.lotResultDTO[].isInLotOpenDetail`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `1,000` bản ghi)

---

### 2.3. Bảng `khlcnt_plan_detail` (Resource: `khlcnt`)

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

### 2.4. Bảng `notify_contractor_standard_detail` (Resource: `notify_contractor`)

- **Tổng bản ghi hoạt động trong Lakehouse:** `546,426` bản ghi
- **Số bản ghi mẫu được phân tích sâu:** `1,000` bản ghi
- **Khoảng thời gian khảo sát:** `2022-09-17` đến `2022-09-28`
- **Khóa chính ứng viên đơn (Single Candidate Keys):** `$.source_id`, `$.ingested_at`, `$.content_hash`, `$.payload.bidoNotifyContractorM.id`, `$.payload.bidoNotifyContractorM.notifyNo`, `$.payload.bidoNotifyContractorM.publicDate`, `$.payload.bidoNotifyContractorM.originalPublicDate`, `$.payload.bidInvContractorOfflineDTO.id`, `$.payload.bidInvContractorOfflineDTO.notifyId`, `$.payload.bidInvContractorOfflineDTO.notifyNo`, `$.payload.bidInvContractorOfflineDTO.decisionFileId`, `$.payload.bidInvContractorOfflineDTO.createdDate`, `$.payload.bidCancelingResponse.id`, `$.payload.bidCancelingResponse.notifyNo`, `$.payload.bidCancelingResponse.publicDate`, `$.payload.bidNoContractorResponse.bidNotification.id`, `$.payload.bidNoContractorResponse.bidNotification.notifyNo`, `$.payload.bidNoContractorResponse.bidNotification.publicDate`, `$.payload.bidNoContractorResponse.bidNotification.lotDTOList[].notifyNo`, `$.payload.bidNoContractorResponse.bidNotification.lotDTOList[].notifyId`, `$.payload.bidNoContractorResponse.bidNotification.getVersionDTOS[].no`, `$.payload.bidNoContractorResponse.bidNotification.originalPublicDate`, `$._dlt_id`
- **Khóa ghép ứng viên (Composite Candidate Keys):** `$.source_id` + `$.source_version`

#### Bảng thống kê các trường dữ liệu quan trọng (Flattened Nested JSON Paths)

| Đường dẫn trường (`Path`) | Kiểu dữ liệu | Độ đầy đủ (`Tier`) | Populated % | Null % | Empty % | Distinct Count | Vai trò |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `$` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Nested |
| `$._dlt_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$._dlt_load_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 13 | Key |
| `$.content_hash` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Free-text |
| `$.ingested_at` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Attribute |
| `$.payload` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Nested |
| `$.payload.bidCancelingResponse` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key |
| `$.payload.bidCancelingResponse.bidCloseDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 450 | Key |
| `$.payload.bidCancelingResponse.bidName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 862 | Key |
| `$.payload.bidCancelingResponse.bidNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 994 | Key |
| `$.payload.bidCancelingResponse.bidStatus` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 6 | Key |
| `$.payload.bidCancelingResponse.id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.bidCancelingResponse.notifyNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.bidCancelingResponse.procuringEntityCode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 619 | Key |
| `$.payload.bidCancelingResponse.procuringEntityName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 619 | Key |
| `$.payload.bidCancelingResponse.publicDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.bidCancelingResponse.status` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key |
| `$.payload.bidInvContractorOfflineDTO` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key |
| `$.payload.bidInvContractorOfflineDTO.createdBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 651 | Key |
| `$.payload.bidInvContractorOfflineDTO.createdDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.bidInvContractorOfflineDTO.decisionAgency` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 751 | Key |
| `$.payload.bidInvContractorOfflineDTO.decisionDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 903 | Key |
| `$.payload.bidInvContractorOfflineDTO.decisionFileId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.bidInvContractorOfflineDTO.decisionFileName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 890 | Key |
| `$.payload.bidInvContractorOfflineDTO.decisionNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 962 | Key |
| `$.payload.bidInvContractorOfflineDTO.id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.bidInvContractorOfflineDTO.notifyId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.bidInvContractorOfflineDTO.notifyNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.bidInvContractorOfflineDTO.notifyVersion` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 4 | Key |
| `$.payload.bidNoContractorResponse` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key |
| `$.payload.bidNoContractorResponse.bidNotification` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key |
| `$.payload.bidNoContractorResponse.bidNotification.bidCloseDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 450 | Key |
| `$.payload.bidNoContractorResponse.bidNotification.bidForm` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Key |
| `$.payload.bidNoContractorResponse.bidNotification.bidId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 995 | Key |
| `$.payload.bidNoContractorResponse.bidNotification.bidMode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key |

> *(Hiển thị 35 / 424 trường tiêu biểu; xem chi tiết toàn bộ trường trong `profile.json`)*

#### Các bất thường & Dấu hiệu suy giảm chất lượng dữ liệu (Quality Anomalies)

- Trường `$.payload.bidoNotifyContractorM.projectName`: whitespace_padding (Ảnh hưởng ước tính: `5` bản ghi)
- Trường `$.payload.bidoNotifyContractorM.procuringEntityName`: whitespace_padding (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.bidoNotifyContractorM.investorName`: whitespace_padding (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.bidoNotifyContractorM.isDomestic`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.bidoNotifyContractorM.bidOpenLocation`: whitespace_padding (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.bidoNotifyContractorM.receiveLocation`: whitespace_padding (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.bidoNotifyContractorM.projectCode`: sentinel_value_detected: 'N/A' (Ảnh hưởng ước tính: `521` bản ghi)
- Trường `$.payload.bidoNotifyContractorM.isInternet`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.bidoNotifyContractorM.isAgreeFrame`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `994` bản ghi)
- Trường `$.payload.bidoNotifyContractorM.propertySample`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `875` bản ghi)

---

### 2.5. Bảng `project_detail` (Resource: `project`)

- **Tổng bản ghi hoạt động trong Lakehouse:** `248,360` bản ghi
- **Số bản ghi mẫu được phân tích sâu:** `1,000` bản ghi
- **Khoảng thời gian khảo sát:** `2022-01-01` đến `2022-02-21`
- **Khóa chính ứng viên đơn (Single Candidate Keys):** `$.source_id`, `$.ingested_at`, `$.content_hash`, `$.payload.id`, `$.payload.publicDate`, `$.payload.code`, `$.payload.projectDTO.id`, `$.payload.projectDTO.no`, `$.payload.projectDTO.code`, `$.payload.projectDTO.publicDate`, `$.payload.pno`, `$._dlt_id`
- **Khóa ghép ứng viên (Composite Candidate Keys):** `$.source_id` + `$.source_version`

#### Bảng thống kê các trường dữ liệu quan trọng (Flattened Nested JSON Paths)

| Đường dẫn trường (`Path`) | Kiểu dữ liệu | Độ đầy đủ (`Tier`) | Populated % | Null % | Empty % | Distinct Count | Vai trò |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `$` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Nested |
| `$._dlt_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$._dlt_load_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 49 | Key |
| `$.content_hash` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Free-text |
| `$.ingested_at` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Attribute |
| `$.payload` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Nested |
| `$.payload.code` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.competentPersons` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Attribute |
| `$.payload.decisionDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 213 | Timestamp |
| `$.payload.decisionNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 962 | Key |
| `$.payload.districtCode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key |
| `$.payload.id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.investForm` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Attribute |
| `$.payload.investTotal` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 886 | Measure |
| `$.payload.investTotalEx` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 887 | Measure |
| `$.payload.investTotalUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Attribute |
| `$.payload.investorCode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 514 | Key |
| `$.payload.investorName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 510 | Free-text |
| `$.payload.isOda` | `bool` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Flag |
| `$.payload.name` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 972 | Free-text |
| `$.payload.pform` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Attribute |
| `$.payload.pgroup` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Attribute |
| `$.payload.pno` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.pperiod` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Measure |
| `$.payload.pperiodUnit` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Measure |
| `$.payload.projectDTO` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Nested |
| `$.payload.projectDTO.code` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.projectDTO.createdBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Attribute |
| `$.payload.projectDTO.createdDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 999 | Timestamp |
| `$.payload.projectDTO.decisionDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 213 | Timestamp |
| `$.payload.projectDTO.decisionNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 962 | Key |
| `$.payload.projectDTO.form` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Attribute |
| `$.payload.projectDTO.group` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Attribute |
| `$.payload.projectDTO.id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.projectDTO.investDetail` | `array` | **Full** | 100.0% | 0.0% | 0.0% | 889 | Nested |

> *(Hiển thị 35 / 200 trường tiêu biểu; xem chi tiết toàn bộ trường trong `profile.json`)*

#### Các bất thường & Dấu hiệu suy giảm chất lượng dữ liệu (Quality Anomalies)

- Trường `$.payload.investorName`: whitespace_padding (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.name`: whitespace_padding (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.decisionNo`: whitespace_padding (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.decisionAgency`: sentinel_value_detected: 'N/A' (Ảnh hưởng ước tính: `989` bản ghi)
- Trường `$.payload.investTarget`: sentinel_value_detected: 'N/A' (Ảnh hưởng ước tính: `989` bản ghi)
- Trường `$.payload.investScale`: sentinel_value_detected: 'N/A' (Ảnh hưởng ước tính: `989` bản ghi)
- Trường `$.payload.competentPersons`: sentinel_value_detected: 'N/A' (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.bidpProjectProjectPlanList[].planName`: whitespace_padding (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.bidpProjectProjectPlanList[].isOldPlan`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.projectDTO.name`: whitespace_padding (Ảnh hưởng ước tính: `1,000` bản ghi)

---

### 2.6. Bảng `notify_contractor_vk_adb_detail` (Resource: `notify_contractor`)

- **Tổng bản ghi hoạt động trong Lakehouse:** `15,017` bản ghi
- **Số bản ghi mẫu được phân tích sâu:** `1,000` bản ghi
- **Khoảng thời gian khảo sát:** `2022-09-22` đến `2023-02-10`
- **Khóa chính ứng viên đơn (Single Candidate Keys):** `$.source_id`, `$.content_hash`, `$.payload.bidoNotifyContractorP.id`, `$.payload.bidoNotifyContractorP.notifyNo`, `$.payload.bidoNotifyContractorP.publicDate`, `$.payload.bidoNotifyContractorP.createdDate`, `$.payload.bidoNotifyContractorP.lastModifiedDate`, `$.payload.bidoNotifyContractorP.getVersionDTOS[].no`, `$.payload.bidoNotifyContractorP.lotDTOList[].notifyNo`, `$.payload.bidoNotifyContractorP.lotDTOList[].notifyId`, `$.payload.bidoNotifyContractorP.originalPublicDate`, `$.payload.bidInvContractorOfflineDTO.id`, `$.payload.bidInvContractorOfflineDTO.notifyId`, `$.payload.bidInvContractorOfflineDTO.notifyNo`, `$.payload.bidInvContractorOfflineDTO.fileId`, `$.payload.bidInvContractorOfflineDTO.decisionFileId`, `$.payload.bidInvContractorOfflineDTO.createdDate`, `$._dlt_id`
- **Khóa ghép ứng viên (Composite Candidate Keys):** `$.source_id` + `$.source_version`

#### Bảng thống kê các trường dữ liệu quan trọng (Flattened Nested JSON Paths)

| Đường dẫn trường (`Path`) | Kiểu dữ liệu | Độ đầy đủ (`Tier`) | Populated % | Null % | Empty % | Distinct Count | Vai trò |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `$` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Nested |
| `$._dlt_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$._dlt_load_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 110 | Key |
| `$.content_hash` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Free-text |
| `$.ingested_at` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 972 | Attribute |
| `$.payload` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Nested |
| `$.payload.bidInvContractorOfflineDTO` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key |
| `$.payload.bidInvContractorOfflineDTO.createdBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 207 | Key |
| `$.payload.bidInvContractorOfflineDTO.createdDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.bidInvContractorOfflineDTO.decisionAgency` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 370 | Key |
| `$.payload.bidInvContractorOfflineDTO.decisionDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 926 | Key |
| `$.payload.bidInvContractorOfflineDTO.decisionFileId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.bidInvContractorOfflineDTO.decisionFileName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 894 | Key |
| `$.payload.bidInvContractorOfflineDTO.decisionNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 946 | Key |
| `$.payload.bidInvContractorOfflineDTO.fileId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.bidInvContractorOfflineDTO.fileName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 880 | Key |
| `$.payload.bidInvContractorOfflineDTO.id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.bidInvContractorOfflineDTO.notifyId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.bidInvContractorOfflineDTO.notifyNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.bidInvContractorOfflineDTO.notifyVersion` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 5 | Key |
| `$.payload.bidoBidStatusDTO` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key |
| `$.payload.bidoBidStatusDTO.beforeStatus` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 7 | Key |
| `$.payload.bidoBidStatusDTO.bidId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 996 | Key |
| `$.payload.bidoBidStatusDTO.bidName` | `string, null` | **Full** | 99.9% | 0.1% | 0.0% | 980 | Key |
| `$.payload.bidoBidStatusDTO.bidNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 993 | Key |
| `$.payload.bidoBidStatusDTO.id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 996 | Key |
| `$.payload.bidoBidStatusDTO.isMultiLot` | `int, null` | **Full** | 99.9% | 0.1% | 0.0% | 2 | Key |
| `$.payload.bidoBidStatusDTO.status` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 5 | Key |
| `$.payload.bidoNotifyContractorP` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key |
| `$.payload.bidoNotifyContractorP.bidCloseDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 484 | Key |
| `$.payload.bidoNotifyContractorP.bidForm` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Key |
| `$.payload.bidoNotifyContractorP.bidId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 996 | Key |
| `$.payload.bidoNotifyContractorP.bidMode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Key |
| `$.payload.bidoNotifyContractorP.bidName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 979 | Key |
| `$.payload.bidoNotifyContractorP.bidNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 992 | Key |

> *(Hiển thị 35 / 252 trường tiêu biểu; xem chi tiết toàn bộ trường trong `profile.json`)*

#### Các bất thường & Dấu hiệu suy giảm chất lượng dữ liệu (Quality Anomalies)

- Trường `$.payload.bidoNotifyContractorP.procuringEntityName`: whitespace_padding (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.bidoNotifyContractorP.investorName`: whitespace_padding (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.bidoNotifyContractorP.isDomestic`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.bidoNotifyContractorP.bidOpenLocation`: whitespace_padding (Ảnh hưởng ước tính: `75` bản ghi)
- Trường `$.payload.bidoNotifyContractorP.bidGuaranteeValue`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `177` bản ghi)
- Trường `$.payload.bidoNotifyContractorP.bidGuaranteeForm`: sentinel_value_detected: '0', sentinel_value_detected: 'Không', sentinel_value_detected: 'không', whitespace_padding (Ảnh hưởng ước tính: `668` bản ghi)
- Trường `$.payload.bidoNotifyContractorP.issueLocation`: whitespace_padding (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.bidoNotifyContractorP.receiveLocation`: whitespace_padding (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.bidoNotifyContractorP.feeValue`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `577` bản ghi)
- Trường `$.payload.bidoNotifyContractorP.isInternet`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `1,000` bản ghi)

---

### 2.7. Bảng `notify_contractor_reoffer_detail` (Resource: `notify_contractor`)

- **Tổng bản ghi hoạt động trong Lakehouse:** `8,696` bản ghi
- **Số bản ghi mẫu được phân tích sâu:** `1,000` bản ghi
- **Khoảng thời gian khảo sát:** `2024-12-09` đến `2025-05-27`
- **Khóa chính ứng viên đơn (Single Candidate Keys):** `$.source_id`, `$.content_hash`, `$.payload.id`, `$.payload.reofferNo`, `$.payload.notifyId`, `$.payload.notifyNo`, `$.payload.publicDate`, `$.payload.hashContent`, `$.payload.createdDate`, `$.payload.lastModifiedDate`, `$.payload.originalPublicDate`, `$.payload.approvalDetailDTO.id`, `$.payload.approvalDetailDTO.notifyId`, `$.payload.approvalDetailDTO.notifyNo`, `$.payload.approvalDetailDTO.approvalId`, `$.payload.approvalDetailDTO.bmtSendDate`, `$.payload.approvalDetailDTO.cdtApprovalDate`, `$.payload.approvalDetailDTO.createdDate`, `$.payload.approvalDetailDTO.lastModifiedDate`, `$._dlt_id`
- **Khóa ghép ứng viên (Composite Candidate Keys):** `$.source_id` + `$.source_version`

#### Bảng thống kê các trường dữ liệu quan trọng (Flattened Nested JSON Paths)

| Đường dẫn trường (`Path`) | Kiểu dữ liệu | Độ đầy đủ (`Tier`) | Populated % | Null % | Empty % | Distinct Count | Vai trò |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `$` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Nested |
| `$._dlt_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$._dlt_load_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 121 | Key |
| `$.content_hash` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Free-text |
| `$.ingested_at` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 899 | Attribute |
| `$.payload` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Nested |
| `$.payload.approvalDetailDTO` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Nested |
| `$.payload.approvalDetailDTO.approvalContent` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Free-text |
| `$.payload.approvalDetailDTO.approvalDecAgency` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 282 | Attribute |
| `$.payload.approvalDetailDTO.approvalDecDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 933 | Timestamp |
| `$.payload.approvalDetailDTO.approvalId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.approvalDetailDTO.approvalResult` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Measure |
| `$.payload.approvalDetailDTO.approvalStatus` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Attribute |
| `$.payload.approvalDetailDTO.bmtSendDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Timestamp |
| `$.payload.approvalDetailDTO.bmtSender` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 341 | Attribute |
| `$.payload.approvalDetailDTO.bmtSenderName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 282 | Free-text |
| `$.payload.approvalDetailDTO.cdtApprovalDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Timestamp |
| `$.payload.approvalDetailDTO.cdtApprovaler` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 325 | Attribute |
| `$.payload.approvalDetailDTO.createdBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 341 | Attribute |
| `$.payload.approvalDetailDTO.createdDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Timestamp |
| `$.payload.approvalDetailDTO.id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.approvalDetailDTO.lastModifiedBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 325 | Attribute |
| `$.payload.approvalDetailDTO.lastModifiedDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Timestamp |
| `$.payload.approvalDetailDTO.notifyId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.approvalDetailDTO.notifyNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1000 | Key |
| `$.payload.approvalDetailDTO.notifyVersion` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 4 | Key |
| `$.payload.approvalDetailDTO.proposalCount` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Measure |
| `$.payload.bidDetail` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key |
| `$.payload.bidDetail.bidField` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key |
| `$.payload.bidDetail.bidForm` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key |
| `$.payload.bidDetail.bidLocation` | `array` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key |
| `$.payload.bidDetail.bidLocation[]` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key |
| `$.payload.bidDetail.bidLocation[].bidDetailId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 980 | Key |
| `$.payload.bidDetail.bidLocation[].createBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 332 | Key |
| `$.payload.bidDetail.bidLocation[].createDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 999 | Key |

> *(Hiển thị 35 / 421 trường tiêu biểu; xem chi tiết toàn bộ trường trong `profile.json`)*

#### Các bất thường & Dấu hiệu suy giảm chất lượng dữ liệu (Quality Anomalies)

- Trường `$.payload.isPrequalification`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.isMultiLot`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.bidDetail.isPrequalification`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.bidDetail.bidStartMonth`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.bidDetail.bidStartQuarter`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.bidDetail.isMultiLot`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.bidDetail.isConcentrateShopping`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.bidDetail.isOnlineShopping`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `1,000` bản ghi)
- Trường `$.payload.bidPlan.isOda`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `990` bản ghi)
- Trường `$.payload.contractorsResult[].times`: sentinel_value_detected: '-1' (Ảnh hưởng ước tính: `1,000` bản ghi)

---

### 2.8. Bảng `bid_opening_detail` (Resource: `bid_opening`)

- **Tổng bản ghi hoạt động trong Lakehouse:** `106` bản ghi
- **Số bản ghi mẫu được phân tích sâu:** `106` bản ghi
- **Khoảng thời gian khảo sát:** `2022-09-17` đến `2022-09-21`
- **Khóa chính ứng viên đơn (Single Candidate Keys):** `$.payload.notify.bidNoContractorResponse.bidNotification.id`, `$.payload.notify.bidNoContractorResponse.bidNotification.notifyNo`, `$.payload.notify.bidNoContractorResponse.bidNotification.bidNo`, `$.payload.notify.bidNoContractorResponse.bidNotification.publicDate`, `$.payload.notify.bidNoContractorResponse.bidNotification.bidPrice`, `$.payload.notify.bidNoContractorResponse.bidNotification.bidId`, `$.payload.notify.bidNoContractorResponse.bidNotification.originalPublicDate`, `$.payload.roundmng.bidoBidroundMngViewDTO.id`, `$.payload.roundmng.bidoBidroundMngViewDTO.notifyNo`, `$.payload.roundmng.bidoBidroundMngViewDTO.bidNo`, `$.payload.roundmng.bidoBidroundMngViewDTO.publicDate`, `$.payload.roundmng.bidoBidroundMngViewDTO.bidPrice`, `$.payload.roundmng.bidoBidroundMngViewDTO.createdDate`, `$.payload.roundmng.bidoBidroundMngViewDTO.updatedDate`, `$.payload.roundmng.bidoBidroundMngViewDTO.bidId`, `$.payload.roundmng.bidoBidroundMngViewDTO.successBidOpenDate`, `$.source_id`, `$.ingested_at`, `$.content_hash`, `$._dlt_id`
- **Khóa ghép ứng viên (Composite Candidate Keys):** `$.source_id` + `$.source_version`

#### Bảng thống kê các trường dữ liệu quan trọng (Flattened Nested JSON Paths)

| Đường dẫn trường (`Path`) | Kiểu dữ liệu | Độ đầy đủ (`Tier`) | Populated % | Null % | Empty % | Distinct Count | Vai trò |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `$` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Nested |
| `$._dlt_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 106 | Key |
| `$._dlt_load_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 5 | Key |
| `$.content_hash` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 106 | Free-text |
| `$.ingested_at` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 106 | Attribute |
| `$.payload` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Nested |
| `$.payload.notify` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key |
| `$.payload.notify.bidNoContractorResponse` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key |
| `$.payload.notify.bidNoContractorResponse.bidNotification` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key |
| `$.payload.notify.bidNoContractorResponse.bidNotification.bidCloseDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 82 | Key |
| `$.payload.notify.bidNoContractorResponse.bidNotification.bidForm` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Key |
| `$.payload.notify.bidNoContractorResponse.bidNotification.bidId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 106 | Key |
| `$.payload.notify.bidNoContractorResponse.bidNotification.bidMode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key |
| `$.payload.notify.bidNoContractorResponse.bidNotification.bidName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 103 | Key |
| `$.payload.notify.bidNoContractorResponse.bidNotification.bidNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 106 | Key |
| `$.payload.notify.bidNoContractorResponse.bidNotification.bidNoType` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key |
| `$.payload.notify.bidNoContractorResponse.bidNotification.bidOpenDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 83 | Key |
| `$.payload.notify.bidNoContractorResponse.bidNotification.bidOpenLocation` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key |
| `$.payload.notify.bidNoContractorResponse.bidNotification.bidPrice` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 106 | Key |
| `$.payload.notify.bidNoContractorResponse.bidNotification.bidPriceUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key |
| `$.payload.notify.bidNoContractorResponse.bidNotification.bidValidityPeriod` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 9 | Key |
| `$.payload.notify.bidNoContractorResponse.bidNotification.bidValidityPeriodUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key |
| `$.payload.notify.bidNoContractorResponse.bidNotification.capitalDetail` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 83 | Key |
| `$.payload.notify.bidNoContractorResponse.bidNotification.contractPeriod` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 32 | Key |
| `$.payload.notify.bidNoContractorResponse.bidNotification.contractPeriodUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key |
| `$.payload.notify.bidNoContractorResponse.bidNotification.contractType` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 5 | Key |
| `$.payload.notify.bidNoContractorResponse.bidNotification.createdBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 70 | Key |
| `$.payload.notify.bidNoContractorResponse.bidNotification.feeType` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key |
| `$.payload.notify.bidNoContractorResponse.bidNotification.guaranteeForm` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key |
| `$.payload.notify.bidNoContractorResponse.bidNotification.id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 106 | Key |
| `$.payload.notify.bidNoContractorResponse.bidNotification.investField` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 4 | Key |
| `$.payload.notify.bidNoContractorResponse.bidNotification.investorCode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 74 | Key |
| `$.payload.notify.bidNoContractorResponse.bidNotification.investorName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 74 | Key |
| `$.payload.notify.bidNoContractorResponse.bidNotification.isAttachFlowEnabled` | `bool` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key |
| `$.payload.notify.bidNoContractorResponse.bidNotification.isDomestic` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key |

> *(Hiển thị 35 / 261 trường tiêu biểu; xem chi tiết toàn bộ trường trong `profile.json`)*

#### Các bất thường & Dấu hiệu suy giảm chất lượng dữ liệu (Quality Anomalies)

- Trường `$.payload.notify.bidNoContractorResponse.bidNotification.procuringEntityName`: whitespace_padding (Ảnh hưởng ước tính: `106` bản ghi)
- Trường `$.payload.notify.bidNoContractorResponse.bidNotification.investorName`: whitespace_padding (Ảnh hưởng ước tính: `106` bản ghi)
- Trường `$.payload.notify.bidNoContractorResponse.bidNotification.projectCode`: sentinel_value_detected: 'N/A' (Ảnh hưởng ước tính: `75` bản ghi)
- Trường `$.payload.notify.bidNoContractorResponse.bidNotification.isMultiLot`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `106` bản ghi)
- Trường `$.payload.notify.bidNoContractorResponse.bidNotification.medicalType`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `106` bản ghi)
- Trường `$.payload.roundmng.bidoBidroundMngViewDTO.procuringEntityName`: whitespace_padding (Ảnh hưởng ước tính: `106` bản ghi)
- Trường `$.payload.roundmng.bidoBidroundMngViewDTO.investorName`: whitespace_padding (Ảnh hưởng ước tính: `106` bản ghi)
- Trường `$.payload.roundmng.bidoBidroundMngViewDTO.packType`: sentinel_value_detected: '0' (Ảnh hưởng ước tính: `106` bản ghi)
- Trường `$.payload.roundmng.bidoBidroundMngViewDTO.pno`: sentinel_value_detected: 'N/A' (Ảnh hưởng ước tính: `75` bản ghi)
- Trường `$.payload.bid_open.bidSubmissionByContractorViewResponse.bidSubmissionDTOList[].contractorName`: whitespace_padding (Ảnh hưởng ước tính: `106` bản ghi)

---

## 3. So Sánh Đột Biến & Lệch Dữ Liệu Giữa Các Cửa Sổ Nạp (Window Drift Analysis)

### Cửa sổ `2022-01-01..2022-02-21` so với `2025-01-01..2025-01-08`

- **Trường mới xuất hiện:** *(Không có)*
- **Trường bị biến mất:** *(Không có)*
- **Biến động tỷ lệ Null lớn (Null Shift ≥ 10%):**
  - `$.payload.linkedPublishPlan[].decisionNo`: từ `23.5%` sang `92.7%`
  - `$.payload.investTarget`: từ `98.9%` sang `56.4%`
  - `$.payload.projectDTO.investTotalEx`: từ `0.0%` sang `100.0%`
  - `$.payload.plocation`: từ `100.0%` sang `0.0%`
  - `$.payload.projectDTO.decisionFileId`: từ `100.0%` sang `0.0%`
  - `$.payload.projectDTO.investTarget`: từ `98.9%` sang `56.4%`
  - `$.payload.linkedPublishPlan[].expertiseOrg`: từ `23.5%` sang `92.7%`
  - `$.payload.linkedPublishPlan[].projectInvestNm`: từ `23.5%` sang `92.7%`
  - `$.payload.projectDTO.investForm`: từ `0.0%` sang `100.0%`
  - `$.payload.provName`: từ `100.0%` sang `19.9%`
  - `$.payload.decisionFileName`: từ `100.0%` sang `0.0%`
  - `$.payload.linkedPublishPlan[].reciprocalCapital`: từ `23.5%` sang `92.7%`
  - `$.payload.linkedPublishPlan[].numPack`: từ `23.5%` sang `92.7%`
  - `$.payload.projectDTO.performUnit`: từ `0.0%` sang `100.0%`
  - `$.payload.projectDTO.code`: từ `0.0%` sang `100.0%`
  - `$.payload.linkedPublishPlan[].decisionAgency`: từ `23.5%` sang `92.7%`
  - `$.payload.projectDTO.decisionAgency`: từ `98.9%` sang `0.0%`
  - `$.payload.linkedPublishPlan[].missNumPack`: từ `23.5%` sang `92.7%`
  - `$.payload.linkedPublishPlan[].decisionFileName`: từ `23.5%` sang `92.7%`
  - `$.payload.linkedPublishPlan[].pVersion`: từ `23.2%` sang `74.2%`
  - `$.payload.projectDTO.decisionFileName`: từ `100.0%` sang `0.0%`
  - `$.payload.linkedPublishPlan[].pName`: từ `23.5%` sang `92.7%`
  - `$.payload.linkedPublishPlan[].decisionFileId`: từ `23.5%` sang `92.7%`
  - `$.payload.linkedPublishPlan[].investDetail`: từ `23.5%` sang `92.7%`
  - `$.payload.investTotalEx`: từ `0.0%` sang `100.0%`
  - `$.payload.linkedPublishPlan[].pNo`: từ `23.2%` sang `74.2%`
  - `$.payload.linkedPublishPlan[].competentPerson`: từ `23.5%` sang `92.7%`
  - `$.payload.linkedPublishPlan[].isConfirmStep`: từ `23.5%` sang `92.7%`
  - `$.payload.linkedPublishPlan[].expertiseReportDate`: từ `23.5%` sang `92.7%`
  - `$.payload.linkedPublishPlan[].supervisor`: từ `23.5%` sang `92.7%`
  - `$.payload.linkedPublishPlan[].capitalType`: từ `23.5%` sang `92.7%`
  - `$.payload.code`: từ `0.0%` sang `100.0%`
  - `$.payload.investForm`: từ `0.0%` sang `100.0%`
  - `$.payload.decisionAgency`: từ `98.9%` sang `0.0%`
  - `$.payload.projectDTO.form`: từ `0.0%` sang `30.9%`
  - `$.payload.linkedPublishPlan[].versionDTOS`: từ `23.5%` sang `92.7%`
  - `$.payload.linkedPublishPlan[].description`: từ `23.5%` sang `92.7%`
  - `$.payload.decisionFileId`: từ `100.0%` sang `0.0%`
  - `$.payload.districtName`: từ `100.0%` sang `28.3%`
  - `$.payload.pform`: từ `0.0%` sang `30.9%`
  - `$.payload.linkedPublishPlan[].exchangeDate`: từ `23.5%` sang `92.7%`
  - `$.payload.linkedPublishPlan[].process`: từ `23.5%` sang `92.7%`
- **Đột biến kiểu dữ liệu (Type Mutations):**
  - `$.payload.projectDTO.investTotalEx`: `{'int': 1000}` → `{'null': 1000}`
  - `$.payload.projectDTO.projectPlanList[].planBidTotalUnit`: `{'string': 1005}` → `{'null': 24}`
  - `$.payload.otherInfo`: `{'null': 1000}` → `{'null': 999, 'string': 1}`
  - `$.payload.plocation`: `{'null': 1000}` → `{'string': 1000}`
  - `$.payload.projectDTO.decisionFileId`: `{'null': 1000}` → `{'string': 1000}`
  - `$.payload.projectDTO.investForm`: `{'string': 1000}` → `{'null': 1000}`
  - `$.payload.provName`: `{'null': 1000}` → `{'string': 801, 'null': 199}`
  - `$.payload.pgroup`: `{'string': 1000}` → `{'string': 932, 'null': 68}`
  - `$.payload.linkedPublishPlan[].investTotal`: `{'int': 342}` → `{'int': 1193, 'null': 2}`
  - `$.payload.decisionFileName`: `{'null': 1000}` → `{'string': 1000}`
  - `$.payload.investmentFunds`: `{'null': 1000}` → `{'null': 932, 'string': 68}`
  - `$.payload.projectDTO.performUnit`: `{'string': 1000}` → `{'null': 1000}`
  - `$.payload.projectDTO.code`: `{'string': 1000}` → `{'null': 1000}`
  - `$.payload.projectDTO.decisionAgency`: `{'null': 989, 'string': 11}` → `{'string': 1000}`
  - `$.payload.projectDTO.decisionFileName`: `{'null': 1000}` → `{'string': 1000}`
  - `$.payload.competentPersons`: `{'string': 1000}` → `{'string': 932, 'null': 68}`
  - `$.payload.investTotalEx`: `{'int': 1000}` → `{'null': 1000}`
  - `$.payload.linkedPlanOverall[].investmentFunds`: `{'null': 2}` → `{'string': 15, 'null': 9}`
  - `$.payload.projectDTO.investDetail[].exchangeRate`: `{'null': 1000, 'float': 13}` → `{'null': 1000}`
  - `$.payload.bidpProjectProjectPlanList[].planBidTotalUnit`: `{'string': 1005}` → `{'null': 24}`
  - `$.payload.linkedPublishPlan[].isOda`: `{'int': 342}` → `{'int': 1193, 'null': 2}`
  - `$.payload.projectDTO.reciprocalCapital`: `{'null': 994, 'int': 6}` → `{'null': 1000}`
  - `$.payload.linkedPublishPlan[].investTotalUnit`: `{'string': 339, 'null': 3}` → `{'string': 1195}`
  - `$.payload.code`: `{'string': 1000}` → `{'null': 1000}`
  - `$.payload.investForm`: `{'string': 1000}` → `{'null': 1000}`
  - `$.payload.decisionAgency`: `{'null': 989, 'string': 11}` → `{'string': 1000}`
  - `$.payload.projectDTO.form`: `{'string': 1000}` → `{'null': 309, 'string': 691}`
  - `$.payload.decisionFileId`: `{'null': 1000}` → `{'string': 1000}`
  - `$.payload.districtName`: `{'null': 1000}` → `{'string': 717, 'null': 283}`
  - `$.payload.projectDTO.group`: `{'string': 1000}` → `{'string': 932, 'null': 68}`
  - `$.payload.pform`: `{'string': 1000}` → `{'null': 309, 'string': 691}`
  - `$.payload.projectDTO.exChangeDate`: `{'null': 1000}` → `{'null': 998, 'datetime': 2}`

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