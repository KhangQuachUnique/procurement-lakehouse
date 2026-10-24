# Báo Cáo Khảo Sát & Kiểm Định Dữ Liệu Bronze - Ánh Xạ Schema Silver (Profiling & Mapping Audit Report)

> **Phương pháp tiếp cận:** Tuân thủ chuẩn `data-profiling` và `data-cleaning` (evidence-first). Báo cáo gồm 2 phần cốt lõi: **(I) Khảo sát đầy đủ 100% trường dữ liệu của từng bảng Bronze dưới dạng bảng kèm chẩn đoán vấn đề chất lượng cụ thể**, và **(II) Bảng ánh xạ chi tiết từng trường dữ liệu từ Bronze qua các bảng chuẩn hóa tại tầng Silver**.

---

## 1. Tổng Quan Tài Nguyên Bronze Hoạt Động (Discovered Bronze Resources)

Dữ liệu Bronze được quản lý đồng bộ qua 2 tầng:
1. **PostgreSQL Metadata (`bronze_meta`):** Quản lý phiên bản commit hiện hành (`current_commit_id`), phân vùng ngày (`partitions`), và danh mục file vật lý (`commit_files`).
2. **Object Storage (SeaweedFS S3):** Lưu trữ định dạng Parquet bất biến (immutable) chứa envelope truy vết (`source_id`, `source_version`, `source_date`, `run_id`, `ingested_at`, `content_hash`) và `payload` JSON gốc.

| STT | Resource | Các Bảng Bronze Vật Lý | Partitions | Active Commits | Tổng Bản Ghi | Dung Lượng Nén | Khoảng Thời Gian Dữ Liệu |
| :---: | :--- | :--- | ---: | ---: | ---: | ---: | :---: |
| 1 | `project` | `project_detail` | 1,462 | 1,462 | 239,836 | 196.09 MB | `2022-01-01` → `2026-01-01` |
| 2 | `khlcnt` | `khlcnt_plan_detail`, `khlcnt_bid_package_detail` | 1,462 | 1,462 | 3,303,006 | 2,686.21 MB | `2022-01-01` → `2026-01-01` |
| 3 | `notify_contractor` | `notify_contractor_standard_detail`, `notify_contractor_vk_adb_detail`, `notify_contractor_reoffer_detail` | 1,462 | 1,462 | 557,234 | 1,547.62 MB | `2022-01-01` → `2026-01-01` |
| 4 | `contractor_result` | `contractor_result_detail` | 1,461 | 1,461 | 1,500,212 | 3,061.61 MB | `2022-01-01` → `2026-01-01` |

- **Tổng số bản ghi Bronze hoạt động:** `5,600,288` bản ghi
- **Tổng dung lượng Parquet nén:** `7.32 GB`

---

## 2. Khảo Sát Đầy Đủ Trường Dữ Liệu Bronze & Chẩn Đoán Chất Lượng (Full Field-Level Bronze Profiling)

Dưới đây là kết quả bóc tách đệ quy **toàn bộ 100% đường dẫn trường (flattened JSON paths)** của từng bảng vật lý trong tầng Bronze. Mọi trường dữ liệu đều được hiển thị đầy đủ trong bảng kèm theo thông số thống kê và các vấn đề chất lượng dữ liệu cụ thể.

### 2.1. Bảng `project_detail` (Resource: `project`)

- **Tổng bản ghi hoạt động (Active Rows):** `239,836` bản ghi (`1,438` files Parquet)
- **Số bản ghi mẫu phân tích sâu (Sampled Records):** `1,000` bản ghi
- **Khoảng thời gian mẫu khảo sát:** `2022-01-01` → `2022-02-21`
- **Tổng số trường phát hiện (Total Flattened Paths):** `200` trường
- **Khóa chính ứng viên đơn (Single Candidate Keys):** `$.source_id`, `$.ingested_at`, `$.content_hash`, `$.payload.id`, `$.payload.publicDate`, `$.payload.code`, `$.payload.projectDTO.id`, `$.payload.projectDTO.no`, `$.payload.projectDTO.code`, `$.payload.projectDTO.publicDate`, `$.payload.pno`, `$._dlt_id`
- **Khóa ghép ứng viên (Composite Candidate Keys):** `$.source_id` + `$.source_version`

#### 2.1.1. Bảng Thống Kê Đầy Đủ 200 Trường Dữ Liệu Của `project_detail`

| STT | Đường dẫn trường (`Path`) | Kiểu dữ liệu (`Types`) | Độ đầy đủ (`Tier`) | Populated % | Null % | Empty % | Distinct | Vai trò (`Role`) | Giá trị mẫu (`Top Samples`) | Vấn đề cụ thể phát hiện (`Specific Issues`) |
| :---: | :--- | :--- | :---: | ---: | ---: | ---: | ---: | :---: | :--- | :--- |
| 1 | `$` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Nested | *(Nested Object)* | Không (Clean) |
| 2 | `$._dlt_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `mIWaukmABw2EOg` (1), `eJWKgsLyw/DFGg` (1) | Không (Clean) |
| 3 | `$._dlt_load_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 49 | Key | `1789454979.1265752` (50), `1789454989.1295576` (50) | Không (Clean) |
| 4 | `$.content_hash` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Free-text | `08a3c23dab0ed4ee7cc29433f3236...` (1), `f26d33b7334aafc3ef323cb7cb2ef...` (1) | Không (Clean) |
| 5 | `$.ingested_at` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Attribute | `2026-09-15 06:48:15.019301+00:00` (1), `2026-09-15 06:48:16.269699+00:00` (1) | Không (Clean) |
| 6 | `$.payload` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Nested | *(Nested Object)* | Không (Clean) |
| 7 | `$.payload.bidpProjectProjectPlanList` | `array` | **Mostly full** | 98.1% | 0.0% | 1.9% | 0 | Key | *(Nested Array)* | Chuỗi/Mảng rỗng (1.9%) |
| 8 | `$.payload.bidpProjectProjectPlanList[]` | `object` | **Mostly full** | 98.1% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 9 | `$.payload.bidpProjectProjectPlanList[].createdBy` | `string` | **Mostly full** | 98.1% | 0.0% | 0.0% | 2 | Key | `ADMIN` (993), `SYSTEM` (12) | Không (Clean) |
| 10 | `$.payload.bidpProjectProjectPlanList[].createdDate` | `datetime` | **Mostly full** | 98.1% | 0.0% | 0.0% | 994 | Key | `2022-01-25T17:27:53` (12), `2022-09-19T16:58:53.618` (1) | Không (Clean) |
| 11 | `$.payload.bidpProjectProjectPlanList[].id` | `string` | **Mostly full** | 98.1% | 0.0% | 0.0% | 1,005 | Key | `E968C4B9537BB541E053321810ACBA8A` (1), `E968C4B9536BB541E053321810ACBA8A` (1) | Không (Clean) |
| 12 | `$.payload.bidpProjectProjectPlanList[].isOldPlan` | `string` | **Mostly full** | 98.1% | 0.0% | 0.0% | 1 | Key | `0` (1005) | Giá trị khuyết danh/Sentinel ('0') |
| 13 | `$.payload.bidpProjectProjectPlanList[].name` | `string` | **Mostly full** | 98.1% | 0.0% | 0.0% | 953 | Key | `Dự án sản xuất lúa chứng nhận...` (12), `Trạm biến áp 220 kV Gò Công v...` (3) | Không (Clean) |
| 14 | `$.payload.bidpProjectProjectPlanList[].no` | `string` | **Mostly full** | 98.1% | 0.0% | 0.0% | 981 | Key | `20220157737` (12), `20220103570` (2) | Không (Clean) |
| 15 | `$.payload.bidpProjectProjectPlanList[].pId` | `string` | **Mostly full** | 98.1% | 0.0% | 0.0% | 981 | Key | `52882530659347292076` (12), `24996722723466152592` (2) | Không (Clean) |
| 16 | `$.payload.bidpProjectProjectPlanList[].planBidTotal` | `int` | **Mostly full** | 98.1% | 0.0% | 0.0% | 868 | Key | `5577990000` (12), `5000000000` (9) | Không (Clean) |
| 17 | `$.payload.bidpProjectProjectPlanList[].planBidTotalUnit` | `string` | **Mostly full** | 98.1% | 0.0% | 0.0% | 1 | Key | `VND` (1005) | Không (Clean) |
| 18 | `$.payload.bidpProjectProjectPlanList[].planName` | `string` | **Mostly full** | 98.1% | 0.0% | 0.0% | 946 | Key | `Kế hoạch lựa chọn nhà thầu` (12), `Dự án sản xuất lúa chứng nhận...` (12) | Dư khoảng trắng đầu/cuối (1 bản ghi) |
| 19 | `$.payload.bidpProjectProjectPlanList[].planNo` | `string` | **Mostly full** | 98.1% | 0.0% | 0.0% | 1,005 | Key | `20220100278` (1), `20220100496` (1) | Không (Clean) |
| 20 | `$.payload.bidpProjectProjectPlanList[].updateBy` | `string` | **Mostly full** | 98.1% | 0.0% | 0.0% | 17 | Key | `ADMIN` (967), `SYSTEM` (12) | Không (Clean) |
| 21 | `$.payload.bidpProjectProjectPlanList[].updatedDate` | `datetime` | **Mostly full** | 98.1% | 0.0% | 0.0% | 994 | Key | `2022-01-25T17:27:53` (12), `2022-09-19T16:58:53.618` (1) | Không (Clean) |
| 22 | `$.payload.code` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `20220100277` (1), `20220100494` (1) | Không (Clean) |
| 23 | `$.payload.competentPersons` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Attribute | `[""]` (989), `N/A` (11) | Giá trị khuyết danh/Sentinel ('N/A') |
| 24 | `$.payload.decisionAgency` | `null, string` | **Sparse** | 1.1% | 98.9% | 0.0% | 1 | Attribute | `N/A` (11) | Thưa thớt cao (Null 98.9%); Giá trị khuyết danh/Sentinel ('N/A') |
| 25 | `$.payload.decisionDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 213 | Timestamp | `2021-12-31T00:00:00` (54), `2021-10-29T00:00:00` (52) | Không (Clean) |
| 26 | `$.payload.decisionFileId` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 27 | `$.payload.decisionFileName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Free-text | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 28 | `$.payload.decisionNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 962 | Key | `2126/QĐ-EVNNPC` (9), `9681/EVNHANOI-KH` (5) | Dư khoảng trắng đầu/cuối (81 bản ghi) |
| 29 | `$.payload.districtCode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `\|\|` (1000) | Mã địa giới bọc dấu pipe `\|...\|` |
| 30 | `$.payload.districtName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Free-text | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 31 | `$.payload.id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `96868410965331890102` (1), `88339013481120382001` (1) | Không (Clean) |
| 32 | `$.payload.investForm` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Attribute | `KHAC` (1000) | Không (Clean) |
| 33 | `$.payload.investScale` | `null, string` | **Sparse** | 1.1% | 98.9% | 0.0% | 1 | Attribute | `N/A` (11) | Thưa thớt cao (Null 98.9%); Giá trị khuyết danh/Sentinel ('N/A') |
| 34 | `$.payload.investTarget` | `null, string` | **Sparse** | 1.1% | 98.9% | 0.0% | 1 | Attribute | `N/A` (11) | Thưa thớt cao (Null 98.9%); Giá trị khuyết danh/Sentinel ('N/A') |
| 35 | `$.payload.investTotal` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 886 | Measure | `5000000000` (9), `50000000000` (9) | Không (Clean) |
| 36 | `$.payload.investTotalEx` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 887 | Measure | `5000000000` (9), `50000000000` (8) | Không (Clean) |
| 37 | `$.payload.investTotalUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Attribute | `VND` (1000) | Không (Clean) |
| 38 | `$.payload.investmentFunds` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 39 | `$.payload.investorCode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 514 | Key | `vn0104169096` (41), `vn3602000933` (19) | Không (Clean) |
| 40 | `$.payload.investorName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 510 | Free-text | `Ban Duy tu các công trình hạ ...` (41), `Ban Quản lý dự án huyện Nhơn ...` (19) | Dư khoảng trắng đầu/cuối (26 bản ghi) |
| 41 | `$.payload.isOda` | `bool` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Flag | `False` (987), `True` (13) | Không (Clean) |
| 42 | `$.payload.linkedPlanOverall` | `array` | **Sparse** | 0.2% | 0.0% | 99.8% | 0 | Key | *(Nested Array)* | Chuỗi/Mảng rỗng (99.8%) |
| 43 | `$.payload.linkedPlanOverall[]` | `object` | **Sparse** | 0.2% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 44 | `$.payload.linkedPlanOverall[].attachFileId` | `string` | **Sparse** | 0.2% | 0.0% | 0.0% | 2 | Key | `943d47c3-943a-4266-923a-e1c56...` (1), `2b12fe63-7207-44a2-a998-19fbb...` (1) | Không (Clean) |
| 45 | `$.payload.linkedPlanOverall[].attachFileName` | `string` | **Sparse** | 0.2% | 0.0% | 0.0% | 2 | Key | `qd 431.pdf` (1), `QD 1726 phe duyet KHLCNT bo s...` (1) | Không (Clean) |
| 46 | `$.payload.linkedPlanOverall[].bidPack` | `null` | **Sparse** | 0.0% | 0.2% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 47 | `$.payload.linkedPlanOverall[].competentPerson` | `null` | **Sparse** | 0.0% | 0.2% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 48 | `$.payload.linkedPlanOverall[].decisionAgency` | `null` | **Sparse** | 0.0% | 0.2% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 49 | `$.payload.linkedPlanOverall[].decisionDate` | `datetime` | **Sparse** | 0.2% | 0.0% | 0.0% | 2 | Key | `2024-04-12T23:59:59` (1), `2024-10-16T23:59:59` (1) | Không (Clean) |
| 50 | `$.payload.linkedPlanOverall[].decisionFileId` | `null` | **Sparse** | 0.0% | 0.2% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 51 | `$.payload.linkedPlanOverall[].decisionFileName` | `null` | **Sparse** | 0.0% | 0.2% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 52 | `$.payload.linkedPlanOverall[].decisionNo` | `string` | **Sparse** | 0.2% | 0.0% | 0.0% | 2 | Key | `431/QĐ-UBND` (1), `1726/QĐ-LĐTB&XH` (1) | Không (Clean) |
| 53 | `$.payload.linkedPlanOverall[].exChangeDate` | `null` | **Sparse** | 0.0% | 0.2% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 54 | `$.payload.linkedPlanOverall[].id` | `string` | **Sparse** | 0.2% | 0.0% | 0.0% | 2 | Key | `022b5b2b-7516-4850-88b6-add09...` (1), `296a97de-ec73-4d0d-adfb-299a3...` (1) | Không (Clean) |
| 55 | `$.payload.linkedPlanOverall[].investDetail` | `string` | **Sparse** | 0.2% | 0.0% | 0.0% | 2 | Key | `[{"invest":3800000000,"moneyT...` (1), `[{"invest":88380000000,"money...` (1) | Không (Clean) |
| 56 | `$.payload.linkedPlanOverall[].investTotal` | `int` | **Sparse** | 0.2% | 0.0% | 0.0% | 2 | Key | `3800000000` (1), `88380000000` (1) | Không (Clean) |
| 57 | `$.payload.linkedPlanOverall[].investTotalUnit` | `string` | **Sparse** | 0.2% | 0.0% | 0.0% | 1 | Key | `VND` (2) | Không (Clean) |
| 58 | `$.payload.linkedPlanOverall[].investmentFunds` | `null` | **Sparse** | 0.0% | 0.2% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 59 | `$.payload.linkedPlanOverall[].investorCode` | `string` | **Sparse** | 0.2% | 0.0% | 0.0% | 2 | Key | `vn6400185684` (1), `vn1400595635` (1) | Không (Clean) |
| 60 | `$.payload.linkedPlanOverall[].investorName` | `string` | **Sparse** | 0.2% | 0.0% | 0.0% | 2 | Key | `Thanh tra tỉnh Đắk Nông` (1), `Sở Lao động Thương binh và Xã...` (1) | Dư khoảng trắng đầu/cuối (1 bản ghi) |
| 61 | `$.payload.linkedPlanOverall[].isOda` | `int` | **Sparse** | 0.2% | 0.0% | 0.0% | 1 | Key | `0` (2) | Giá trị khuyết danh/Sentinel ('0') |
| 62 | `$.payload.linkedPlanOverall[].name` | `string` | **Sparse** | 0.2% | 0.0% | 0.0% | 2 | Key | `Kế hoặc lựa chọn nhà thầu côn...` (1), `Kế hoạch lựa chọn nhà thầu mộ...` (1) | Không (Clean) |
| 63 | `$.payload.linkedPlanOverall[].otherInformation` | `null` | **Sparse** | 0.0% | 0.2% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 64 | `$.payload.linkedPlanOverall[].period` | `int` | **Sparse** | 0.2% | 0.0% | 0.0% | 1 | Key | `0` (2) | Giá trị khuyết danh/Sentinel ('0') |
| 65 | `$.payload.linkedPlanOverall[].periodUnit` | `int` | **Sparse** | 0.2% | 0.0% | 0.0% | 1 | Key | `1` (2) | Không (Clean) |
| 66 | `$.payload.linkedPlanOverall[].pid` | `string` | **Sparse** | 0.2% | 0.0% | 0.0% | 2 | Key | `81832762584511289128` (1), `57716880812865205795` (1) | Không (Clean) |
| 67 | `$.payload.linkedPlanOverall[].planNo` | `string` | **Sparse** | 0.2% | 0.0% | 0.0% | 2 | Key | `PL2400057048` (1), `PL2400239472` (1) | Không (Clean) |
| 68 | `$.payload.linkedPlanOverall[].planType` | `string` | **Sparse** | 0.2% | 0.0% | 0.0% | 1 | Key | `DTPT` (2) | Không (Clean) |
| 69 | `$.payload.linkedPlanOverall[].planVersion` | `string` | **Sparse** | 0.2% | 0.0% | 0.0% | 2 | Key | `01` (1), `00` (1) | Không (Clean) |
| 70 | `$.payload.linkedPlanOverall[].plocation` | `null` | **Sparse** | 0.0% | 0.2% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 71 | `$.payload.linkedPlanOverall[].pname` | `string` | **Sparse** | 0.2% | 0.0% | 0.0% | 2 | Key | `Sửa chữa Trụ sở làm việc Than...` (1), `Trung tâm Điều dưỡng người có...` (1) | Không (Clean) |
| 72 | `$.payload.linkedPlanOverall[].pno` | `string` | **Sparse** | 0.2% | 0.0% | 0.0% | 2 | Key | `20220201210` (1), `20220218841` (1) | Không (Clean) |
| 73 | `$.payload.linkedPlanOverall[].projectScale` | `null` | **Sparse** | 0.0% | 0.2% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 74 | `$.payload.linkedPlanOverall[].publicDate` | `datetime` | **Sparse** | 0.2% | 0.0% | 0.0% | 2 | Key | `2024-04-16T09:11:47.567` (1), `2024-10-21T14:28:43.91` (1) | Không (Clean) |
| 75 | `$.payload.linkedPlanOverall[].pversion` | `string` | **Sparse** | 0.2% | 0.0% | 0.0% | 1 | Key | `00` (2) | Không (Clean) |
| 76 | `$.payload.linkedPlanOverall[].reciprocalCapital` | `null` | **Sparse** | 0.0% | 0.2% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 77 | `$.payload.linkedPlanOverall[].status` | `string` | **Sparse** | 0.2% | 0.0% | 0.0% | 1 | Key | `01` (2) | Không (Clean) |
| 78 | `$.payload.linkedPlanOverall[].versionDTOList` | `null` | **Sparse** | 0.0% | 0.2% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 79 | `$.payload.linkedPublishPlan` | `array` | **Sparse** | 23.5% | 0.0% | 76.5% | 0 | Nested | *(Nested Array)* | Chuỗi/Mảng rỗng (76.5%) |
| 80 | `$.payload.linkedPublishPlan[]` | `object` | **Sparse** | 23.5% | 0.0% | 0.0% | 0 | Nested | *(Nested Object)* | Không (Clean) |
| 81 | `$.payload.linkedPublishPlan[].approvalStatus` | `string` | **Sparse** | 23.5% | 0.0% | 0.0% | 1 | Measure | `01` (342) | Không (Clean) |
| 82 | `$.payload.linkedPublishPlan[].bidPack` | `int` | **Sparse** | 23.5% | 0.0% | 0.0% | 16 | Key | `1` (108), `2` (79) | Không (Clean) |
| 83 | `$.payload.linkedPublishPlan[].capitalType` | `null` | **Sparse** | 0.0% | 23.5% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Khuyết một phần (Null 23.5%) |
| 84 | `$.payload.linkedPublishPlan[].competentPerson` | `null` | **Sparse** | 0.0% | 23.5% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Khuyết một phần (Null 23.5%) |
| 85 | `$.payload.linkedPublishPlan[].decisionAgency` | `null` | **Sparse** | 0.0% | 23.5% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Khuyết một phần (Null 23.5%) |
| 86 | `$.payload.linkedPublishPlan[].decisionDate` | `datetime` | **Sparse** | 23.5% | 0.0% | 0.0% | 253 | Timestamp | `2022-01-17T00:00:00.000+0000` (7), `2022-01-06T17:00:00.000+0000` (5) | Không (Clean) |
| 87 | `$.payload.linkedPublishPlan[].decisionFileId` | `null` | **Sparse** | 0.0% | 23.5% | 0.0% | 0 | Key | *(Null / Rỗng)* | Khuyết một phần (Null 23.5%) |
| 88 | `$.payload.linkedPublishPlan[].decisionFileName` | `null` | **Sparse** | 0.0% | 23.5% | 0.0% | 0 | Free-text | *(Null / Rỗng)* | Khuyết một phần (Null 23.5%) |
| 89 | `$.payload.linkedPublishPlan[].decisionNo` | `null` | **Sparse** | 0.0% | 23.5% | 0.0% | 0 | Key | *(Null / Rỗng)* | Khuyết một phần (Null 23.5%) |
| 90 | `$.payload.linkedPublishPlan[].description` | `null` | **Sparse** | 0.0% | 23.5% | 0.0% | 0 | Free-text | *(Null / Rỗng)* | Khuyết một phần (Null 23.5%) |
| 91 | `$.payload.linkedPublishPlan[].exchangeDate` | `null` | **Sparse** | 0.0% | 23.5% | 0.0% | 0 | Timestamp | *(Null / Rỗng)* | Khuyết một phần (Null 23.5%) |
| 92 | `$.payload.linkedPublishPlan[].expertiseOrg` | `null` | **Sparse** | 0.0% | 23.5% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Khuyết một phần (Null 23.5%) |
| 93 | `$.payload.linkedPublishPlan[].expertiseReportDate` | `null` | **Sparse** | 0.0% | 23.5% | 0.0% | 0 | Timestamp | *(Null / Rỗng)* | Khuyết một phần (Null 23.5%) |
| 94 | `$.payload.linkedPublishPlan[].id` | `string` | **Sparse** | 23.5% | 0.0% | 0.0% | 342 | Key | `8809228217265904572` (1), `06036749974665434066` (1) | Không (Clean) |
| 95 | `$.payload.linkedPublishPlan[].investDetail` | `null` | **Sparse** | 0.0% | 23.5% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Khuyết một phần (Null 23.5%) |
| 96 | `$.payload.linkedPublishPlan[].investTotal` | `int` | **Sparse** | 23.5% | 0.0% | 0.0% | 214 | Measure | `2728300000000` (27), `50000000000` (9) | Giá trị khuyết danh/Sentinel ('0') |
| 97 | `$.payload.linkedPublishPlan[].investTotalUnit` | `string, null` | **Sparse** | 23.3% | 0.2% | 0.0% | 1 | Attribute | `VND` (339) | Không (Clean) |
| 98 | `$.payload.linkedPublishPlan[].investorCode` | `string` | **Sparse** | 23.5% | 0.0% | 0.0% | 158 | Key | `vn0316506787` (29), `vn0102743068` (15) | Không (Clean) |
| 99 | `$.payload.linkedPublishPlan[].investorName` | `string` | **Sparse** | 23.5% | 0.0% | 0.0% | 164 | Free-text | `Ban Quản lý dự án quốc tế` (29), `TỔNG CÔNG TY TRUYỀN TẢI ĐIỆN ...` (15) | Dư khoảng trắng đầu/cuối (8 bản ghi) |
| 100 | `$.payload.linkedPublishPlan[].isConfirmStep` | `null` | **Sparse** | 0.0% | 23.5% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Khuyết một phần (Null 23.5%) |
| 101 | `$.payload.linkedPublishPlan[].isOda` | `int` | **Sparse** | 23.5% | 0.0% | 0.0% | 2 | Measure | `0` (303), `1` (39) | Giá trị khuyết danh/Sentinel ('0') |
| 102 | `$.payload.linkedPublishPlan[].missNumPack` | `null` | **Sparse** | 0.0% | 23.5% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Khuyết một phần (Null 23.5%) |
| 103 | `$.payload.linkedPublishPlan[].name` | `string` | **Sparse** | 23.5% | 0.0% | 0.0% | 328 | Free-text | `Hạ tầng kỹ thuật khu dân cư k...` (2), `Kế hoạch lựa chọn nhà thầu đợt 2` (2) | Không (Clean) |
| 104 | `$.payload.linkedPublishPlan[].numPack` | `null` | **Sparse** | 0.0% | 23.5% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Khuyết một phần (Null 23.5%) |
| 105 | `$.payload.linkedPublishPlan[].pId` | `string` | **Sparse** | 23.5% | 0.0% | 0.0% | 235 | Key | `25832798313976296741` (29), `24874453513145600172` (8) | Không (Clean) |
| 106 | `$.payload.linkedPublishPlan[].pName` | `null` | **Sparse** | 0.0% | 23.5% | 0.0% | 0 | Free-text | *(Null / Rỗng)* | Khuyết một phần (Null 23.5%) |
| 107 | `$.payload.linkedPublishPlan[].pNo` | `null, string` | **Sparse** | 0.3% | 23.2% | 0.0% | 3 | Key | `20220123218` (1), `20220129605` (1) | Khuyết một phần (Null 23.2%) |
| 108 | `$.payload.linkedPublishPlan[].pVersion` | `null, string` | **Sparse** | 0.3% | 23.2% | 0.0% | 1 | Measure | `00` (3) | Khuyết một phần (Null 23.2%) |
| 109 | `$.payload.linkedPublishPlan[].planNo` | `string` | **Sparse** | 23.5% | 0.0% | 0.0% | 342 | Key | `20220100496` (1), `20220617988` (1) | Không (Clean) |
| 110 | `$.payload.linkedPublishPlan[].planType` | `string` | **Sparse** | 23.5% | 0.0% | 0.0% | 1 | Attribute | `DTPT` (342) | Không (Clean) |
| 111 | `$.payload.linkedPublishPlan[].planVersion` | `string` | **Sparse** | 23.5% | 0.0% | 0.0% | 9 | Measure | `00` (232), `01` (76) | Không (Clean) |
| 112 | `$.payload.linkedPublishPlan[].process` | `null` | **Sparse** | 0.0% | 23.5% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Khuyết một phần (Null 23.5%) |
| 113 | `$.payload.linkedPublishPlan[].projectInvestNm` | `null` | **Sparse** | 0.0% | 23.5% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Khuyết một phần (Null 23.5%) |
| 114 | `$.payload.linkedPublishPlan[].publicDate` | `datetime` | **Sparse** | 23.5% | 0.0% | 0.0% | 342 | Timestamp | `2022-01-02T18:04:50` (1), `2022-06-07T10:58:38` (1) | Không (Clean) |
| 115 | `$.payload.linkedPublishPlan[].reciprocalCapital` | `null` | **Sparse** | 0.0% | 23.5% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Khuyết một phần (Null 23.5%) |
| 116 | `$.payload.linkedPublishPlan[].status` | `string` | **Sparse** | 23.5% | 0.0% | 0.0% | 2 | Measure | `01` (341), `03` (1) | Không (Clean) |
| 117 | `$.payload.linkedPublishPlan[].supervisor` | `null` | **Sparse** | 0.0% | 23.5% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Khuyết một phần (Null 23.5%) |
| 118 | `$.payload.linkedPublishPlan[].totalBidPrice` | `int` | **Sparse** | 23.5% | 0.0% | 0.0% | 341 | Key | `6500000000` (2), `9468825000` (1) | Không (Clean) |
| 119 | `$.payload.linkedPublishPlan[].versionDTOS` | `null` | **Sparse** | 0.0% | 23.5% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Khuyết một phần (Null 23.5%) |
| 120 | `$.payload.name` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 972 | Free-text | `Trạm biến áp 220 kV Gò Công v...` (3), `Hạ tầng Khu tái định cư Phước...` (3) | Dư khoảng trắng đầu/cuối (38 bản ghi) |
| 121 | `$.payload.otherInfo` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 122 | `$.payload.pform` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Attribute | `KHAC` (1000) | Không (Clean) |
| 123 | `$.payload.pgroup` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Attribute | `NC` (797), `NB` (196) | Giá trị khuyết danh/Sentinel ('NA') |
| 124 | `$.payload.plocation` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 125 | `$.payload.pno` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `20220100277` (1), `20220100494` (1) | Không (Clean) |
| 126 | `$.payload.pperiod` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Measure | `0` (1000) | Giá trị khuyết danh/Sentinel ('0') |
| 127 | `$.payload.pperiodUnit` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Measure | `1` (1000) | Không (Clean) |
| 128 | `$.payload.projectDTO` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Nested | *(Nested Object)* | Không (Clean) |
| 129 | `$.payload.projectDTO.budgetRelCode` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 130 | `$.payload.projectDTO.code` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `20220100277` (1), `20220100494` (1) | Không (Clean) |
| 131 | `$.payload.projectDTO.competentPersons` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 132 | `$.payload.projectDTO.createdBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Attribute | `SYSTEM` (1000) | Không (Clean) |
| 133 | `$.payload.projectDTO.createdDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 999 | Timestamp | `2022-02-11T09:38:40` (2), `2022-01-01T23:46:11` (1) | Không (Clean) |
| 134 | `$.payload.projectDTO.decisionAgency` | `null, string` | **Sparse** | 1.1% | 98.9% | 0.0% | 1 | Attribute | `N/A` (11) | Thưa thớt cao (Null 98.9%); Giá trị khuyết danh/Sentinel ('N/A') |
| 135 | `$.payload.projectDTO.decisionDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 213 | Timestamp | `2021-12-31T00:00:00` (54), `2021-10-29T00:00:00` (52) | Không (Clean) |
| 136 | `$.payload.projectDTO.decisionFileId` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 137 | `$.payload.projectDTO.decisionFileName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Free-text | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 138 | `$.payload.projectDTO.decisionNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 962 | Key | `2126/QĐ-EVNNPC` (9), `9681/EVNHANOI-KH` (5) | Dư khoảng trắng đầu/cuối (81 bản ghi) |
| 139 | `$.payload.projectDTO.exChangeDate` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Timestamp | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 140 | `$.payload.projectDTO.form` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Attribute | `KHAC` (1000) | Không (Clean) |
| 141 | `$.payload.projectDTO.group` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Attribute | `NC` (797), `NB` (196) | Giá trị khuyết danh/Sentinel ('NA') |
| 142 | `$.payload.projectDTO.hasLinkedPlan` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 143 | `$.payload.projectDTO.id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `96868410965331890102` (1), `88339013481120382001` (1) | Không (Clean) |
| 144 | `$.payload.projectDTO.investDetail` | `array` | **Full** | 100.0% | 0.0% | 0.0% | 889 | Nested | `[{'invest': 5000000000, 'mone...` (8), `[{'invest': 50000000000, 'mon...` (8) | Không (Clean) |
| 145 | `$.payload.projectDTO.investDetail[]` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 900 | Nested | `{'invest': 5000000000, 'money...` (9), `{'invest': 50000000000, 'mone...` (8) | Không (Clean) |
| 146 | `$.payload.projectDTO.investDetail[].exchangeRate` | `null, float` | **Sparse** | 1.3% | 98.7% | 0.0% | 3 | Measure | `1.0` (11), `23000.0` (1) | Thưa thớt cao (Null 98.7%) |
| 147 | `$.payload.projectDTO.investDetail[].invest` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 893 | Measure | `5000000000` (10), `50000000000` (8) | Không (Clean) |
| 148 | `$.payload.projectDTO.investDetail[].moneyType` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Attribute | `VND` (1011), `USD` (2) | Không (Clean) |
| 149 | `$.payload.projectDTO.investForm` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Attribute | `KHAC` (1000) | Không (Clean) |
| 150 | `$.payload.projectDTO.investScale` | `null, string` | **Sparse** | 1.1% | 98.9% | 0.0% | 1 | Attribute | `N/A` (11) | Thưa thớt cao (Null 98.9%); Giá trị khuyết danh/Sentinel ('N/A') |
| 151 | `$.payload.projectDTO.investTarget` | `null, string` | **Sparse** | 1.1% | 98.9% | 0.0% | 1 | Attribute | `N/A` (11) | Thưa thớt cao (Null 98.9%); Giá trị khuyết danh/Sentinel ('N/A') |
| 152 | `$.payload.projectDTO.investTotal` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 886 | Measure | `5000000000` (9), `50000000000` (9) | Không (Clean) |
| 153 | `$.payload.projectDTO.investTotalEx` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 887 | Measure | `5000000000` (9), `50000000000` (8) | Không (Clean) |
| 154 | `$.payload.projectDTO.investTotalUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Attribute | `VND` (1000) | Không (Clean) |
| 155 | `$.payload.projectDTO.investorCode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 514 | Key | `vn0104169096` (41), `vn3602000933` (19) | Không (Clean) |
| 156 | `$.payload.projectDTO.investorName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 510 | Free-text | `Ban Duy tu các công trình hạ ...` (41), `Ban Quản lý dự án huyện Nhơn ...` (19) | Dư khoảng trắng đầu/cuối (26 bản ghi) |
| 157 | `$.payload.projectDTO.isConfirmStep` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 158 | `$.payload.projectDTO.isOda` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Measure | `0` (987), `1` (13) | Giá trị khuyết danh/Sentinel ('0') |
| 159 | `$.payload.projectDTO.isProjectWorkFirst` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Measure | `0` (1000) | Giá trị khuyết danh/Sentinel ('0') |
| 160 | `$.payload.projectDTO.latestAssignId` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 161 | `$.payload.projectDTO.linkedPublishPlan` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 162 | `$.payload.projectDTO.name` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 972 | Free-text | `Trạm biến áp 220 kV Gò Công v...` (3), `Hạ tầng Khu tái định cư Phước...` (3) | Dư khoảng trắng đầu/cuối (38 bản ghi) |
| 163 | `$.payload.projectDTO.no` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `20220100277` (1), `20220100494` (1) | Không (Clean) |
| 164 | `$.payload.projectDTO.performUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Attribute | `N/A` (1000) | Giá trị khuyết danh/Sentinel ('N/A') |
| 165 | `$.payload.projectDTO.period` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Measure | `0` (1000) | Giá trị khuyết danh/Sentinel ('0') |
| 166 | `$.payload.projectDTO.periodUnit` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Measure | `1` (1000) | Không (Clean) |
| 167 | `$.payload.projectDTO.projectPlanList` | `array` | **Mostly full** | 98.1% | 0.0% | 1.9% | 0 | Nested | *(Nested Array)* | Chuỗi/Mảng rỗng (1.9%) |
| 168 | `$.payload.projectDTO.projectPlanList[]` | `object` | **Mostly full** | 98.1% | 0.0% | 0.0% | 0 | Nested | *(Nested Object)* | Không (Clean) |
| 169 | `$.payload.projectDTO.projectPlanList[].createdBy` | `string` | **Mostly full** | 98.1% | 0.0% | 0.0% | 2 | Attribute | `ADMIN` (993), `SYSTEM` (12) | Không (Clean) |
| 170 | `$.payload.projectDTO.projectPlanList[].createdDate` | `datetime` | **Mostly full** | 98.1% | 0.0% | 0.0% | 994 | Timestamp | `2022-01-25T17:27:53` (12), `2022-09-19T16:58:53.618` (1) | Không (Clean) |
| 171 | `$.payload.projectDTO.projectPlanList[].id` | `string` | **Mostly full** | 98.1% | 0.0% | 0.0% | 1,005 | Key | `E968C4B9537BB541E053321810ACBA8A` (1), `E968C4B9536BB541E053321810ACBA8A` (1) | Không (Clean) |
| 172 | `$.payload.projectDTO.projectPlanList[].isOldPlan` | `string` | **Mostly full** | 98.1% | 0.0% | 0.0% | 1 | Measure | `0` (1005) | Giá trị khuyết danh/Sentinel ('0') |
| 173 | `$.payload.projectDTO.projectPlanList[].name` | `string` | **Mostly full** | 98.1% | 0.0% | 0.0% | 953 | Free-text | `Dự án sản xuất lúa chứng nhận...` (12), `Trạm biến áp 220 kV Gò Công v...` (3) | Không (Clean) |
| 174 | `$.payload.projectDTO.projectPlanList[].no` | `string` | **Mostly full** | 98.1% | 0.0% | 0.0% | 981 | Key | `20220157737` (12), `20220103570` (2) | Không (Clean) |
| 175 | `$.payload.projectDTO.projectPlanList[].pId` | `string` | **Mostly full** | 98.1% | 0.0% | 0.0% | 981 | Key | `52882530659347292076` (12), `24996722723466152592` (2) | Không (Clean) |
| 176 | `$.payload.projectDTO.projectPlanList[].planBidTotal` | `int` | **Mostly full** | 98.1% | 0.0% | 0.0% | 868 | Key | `5577990000` (12), `5000000000` (9) | Không (Clean) |
| 177 | `$.payload.projectDTO.projectPlanList[].planBidTotalUnit` | `string` | **Mostly full** | 98.1% | 0.0% | 0.0% | 1 | Key | `VND` (1005) | Không (Clean) |
| 178 | `$.payload.projectDTO.projectPlanList[].planName` | `string` | **Mostly full** | 98.1% | 0.0% | 0.0% | 946 | Measure | `Kế hoạch lựa chọn nhà thầu` (12), `Dự án sản xuất lúa chứng nhận...` (12) | Dư khoảng trắng đầu/cuối (1 bản ghi) |
| 179 | `$.payload.projectDTO.projectPlanList[].planNo` | `string` | **Mostly full** | 98.1% | 0.0% | 0.0% | 1,005 | Key | `20220100278` (1), `20220100496` (1) | Không (Clean) |
| 180 | `$.payload.projectDTO.projectPlanList[].updateBy` | `string` | **Mostly full** | 98.1% | 0.0% | 0.0% | 17 | Timestamp | `ADMIN` (967), `SYSTEM` (12) | Không (Clean) |
| 181 | `$.payload.projectDTO.projectPlanList[].updatedDate` | `datetime` | **Mostly full** | 98.1% | 0.0% | 0.0% | 994 | Timestamp | `2022-01-25T17:27:53` (12), `2022-09-19T16:58:53.618` (1) | Không (Clean) |
| 182 | `$.payload.projectDTO.publicDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Timestamp | `2022-01-01T23:48:43` (1), `2022-01-02T17:46:23` (1) | Không (Clean) |
| 183 | `$.payload.projectDTO.reciprocalCapital` | `null, int` | **Sparse** | 0.6% | 99.4% | 0.0% | 6 | Measure | `425300000000` (1), `1503010000` (1) | Thưa thớt cao (Null 99.4%) |
| 184 | `$.payload.projectDTO.reciprocalCapitalUnit` | `string, null` | **Full** | 99.9% | 0.1% | 0.0% | 1 | Attribute | `VND` (999) | Không (Clean) |
| 185 | `$.payload.projectDTO.sponsorCode` | `null, string` | **Sparse** | 1.3% | 98.7% | 0.0% | 1 | Key | `KHAC` (13) | Thưa thớt cao (Null 98.7%) |
| 186 | `$.payload.projectDTO.status` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Measure | `01` (1000) | Không (Clean) |
| 187 | `$.payload.projectDTO.updatedBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 130 | Timestamp | `SYSTEM` (804), `vn0102743068` (9) | Không (Clean) |
| 188 | `$.payload.projectDTO.updatedDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 999 | Timestamp | `2022-02-11T09:38:40` (2), `2022-01-01T23:46:11` (1) | Không (Clean) |
| 189 | `$.payload.projectDTO.version` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Measure | `00` (955), `01` (42) | Không (Clean) |
| 190 | `$.payload.projectDTO.versionDTOS` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 191 | `$.payload.provCode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `\|\|` (1000) | Mã địa giới bọc dấu pipe `\|...\|` |
| 192 | `$.payload.provName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Free-text | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 193 | `$.payload.publicDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Timestamp | `2022-01-01T23:48:43` (1), `2022-01-02T17:46:23` (1) | Không (Clean) |
| 194 | `$.payload.pversion` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Measure | `00` (955), `01` (42) | Không (Clean) |
| 195 | `$.payload.status` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Measure | `01` (1000) | Không (Clean) |
| 196 | `$.payload.typeProject` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Attribute | `DTPT` (1000) | Không (Clean) |
| 197 | `$.run_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Key | `890d11441ac849998744be23c5308470` (889), `fd2480153e8044158a080830994c7098` (57) | Không (Clean) |
| 198 | `$.source_date` | `date` | **Full** | 100.0% | 0.0% | 0.0% | 47 | Timestamp | `2022-01-19` (57), `2022-02-11` (54) | Không (Clean) |
| 199 | `$.source_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `96868410965331890102` (1), `88339013481120382001` (1) | Không (Clean) |
| 200 | `$.source_version` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Measure | `00` (955), `01` (42) | Không (Clean) |

#### 2.1.2. Tổng Hợp Các Vấn Đề Chất Lượng Dữ Liệu Cụ Thể Trên Bảng `project_detail`

| Nhóm Vấn Đề Chất Lượng (Issue Category) | Số Trường Bị Ảnh Hưởng | Chi Tiết Trường & Bằng Chứng Số Liệu | Hướng Xử Lý Khi Sang Tầng Silver |
| :--- | :---: | :--- | :--- |
| **1. Trường rỗng hoàn toàn (`100% Null`)** | 17 | `$.payload.decisionFileId`, `$.payload.decisionFileName`, `$.payload.districtName`, `$.payload.investmentFunds`, `$.payload.otherInfo`, `$.payload.plocation`, `$.payload.projectDTO.budgetRelCode`, `$.payload.projectDTO.competentPersons`, `$.payload.projectDTO.decisionFileId`, `$.payload.projectDTO.decisionFileName`, `$.payload.projectDTO.exChangeDate`, `$.payload.projectDTO.hasLinkedPlan`, `$.payload.projectDTO.isConfirmStep`, `$.payload.projectDTO.latestAssignId`, `$.payload.projectDTO.linkedPublishPlan` *(và 2 trường khác)* | Không đưa vào các cột nghiệp vụ chính của Silver; vẫn bảo toàn trong `root_json` / `raw_json`.
| **2. Trường khuyết/thưa thớt (`20% ≤ Null < 100%`)** | 31 | `$.payload.decisionAgency` (98.9%), `$.payload.investScale` (98.9%), `$.payload.investTarget` (98.9%), `$.payload.linkedPublishPlan[].capitalType` (23.5%), `$.payload.linkedPublishPlan[].competentPerson` (23.5%), `$.payload.linkedPublishPlan[].decisionAgency` (23.5%), `$.payload.linkedPublishPlan[].decisionFileId` (23.5%), `$.payload.linkedPublishPlan[].decisionFileName` (23.5%), `$.payload.linkedPublishPlan[].decisionNo` (23.5%), `$.payload.linkedPublishPlan[].description` (23.5%), `$.payload.linkedPublishPlan[].exchangeDate` (23.5%), `$.payload.linkedPublishPlan[].expertiseOrg` (23.5%) *(và 19 trường khác)* | Cho phép `nullable=True` trên schema Silver; thiết lập chuỗi fallback đa tầng (VD: `isMultiLot`, `public_date`, `currency`).
| **3. Chuỗi rỗng hoặc Mảng rỗng (`Empty > 0%`)** | 4 | `$.payload.bidpProjectProjectPlanList` (1.9%), `$.payload.linkedPlanOverall` (99.8%), `$.payload.linkedPublishPlan` (76.5%), `$.payload.projectDTO.projectPlanList` (1.9%) | Hàm `clean_text()` tự động chuẩn hóa chuỗi rỗng `""` hoặc chỉ chứa khoảng trắng về `NULL`; mảng rỗng `[]` không sinh dòng bảng con.
| **4. Đa kiểu dữ liệu (`Mixed Types`)** | 0 | *(Không có)* | Ép kiểu tường minh qua bộ parser (`parse_money`, `parse_boolean`, `parse_date`, `parse_iso_datetime`); ghi nhận `quality_issue` nếu sai định dạng.

**Danh sách chi tiết toàn bộ 30 cảnh báo dị thường (Anomalies) trên các trường có dữ liệu:**

| STT | Trường Dữ Liệu (`Field Path`) | Loại Bất Thường Phát Hiện (`Detected Anomalies`) | Số Bản Ghi Ảnh Hưởng (Trên Mẫu) | Quy Tắc Làm Sạch Tại Silver |
| :---: | :--- | :--- | ---: | :--- |
| 1 | `$.payload.investorName` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 2 | `$.payload.name` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 3 | `$.payload.decisionNo` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 4 | `$.payload.decisionAgency` | `sentinel_value_detected: 'N/A'` | `989` | Lọc các chuỗi khuyết danh (`N/A`, `NULL`, `NONE`, `-`) về `NULL` |
| 5 | `$.payload.investTarget` | `sentinel_value_detected: 'N/A'` | `989` | Lọc các chuỗi khuyết danh (`N/A`, `NULL`, `NONE`, `-`) về `NULL` |
| 6 | `$.payload.investScale` | `sentinel_value_detected: 'N/A'` | `989` | Lọc các chuỗi khuyết danh (`N/A`, `NULL`, `NONE`, `-`) về `NULL` |
| 7 | `$.payload.competentPersons` | `sentinel_value_detected: 'N/A'` | `1,000` | Lọc các chuỗi khuyết danh (`N/A`, `NULL`, `NONE`, `-`) về `NULL` |
| 8 | `$.payload.bidpProjectProjectPlanList[].planName` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 9 | `$.payload.bidpProjectProjectPlanList[].isOldPlan` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 10 | `$.payload.projectDTO.name` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 11 | `$.payload.projectDTO.isOda` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 12 | `$.payload.projectDTO.investorName` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 13 | `$.payload.projectDTO.decisionNo` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 14 | `$.payload.projectDTO.decisionAgency` | `sentinel_value_detected: 'N/A'` | `989` | Lọc các chuỗi khuyết danh (`N/A`, `NULL`, `NONE`, `-`) về `NULL` |
| 15 | `$.payload.projectDTO.isProjectWorkFirst` | `sentinel_value_detected: '0'` | `1,000` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 16 | `$.payload.projectDTO.investTarget` | `sentinel_value_detected: 'N/A'` | `989` | Lọc các chuỗi khuyết danh (`N/A`, `NULL`, `NONE`, `-`) về `NULL` |
| 17 | `$.payload.projectDTO.investScale` | `sentinel_value_detected: 'N/A'` | `989` | Lọc các chuỗi khuyết danh (`N/A`, `NULL`, `NONE`, `-`) về `NULL` |
| 18 | `$.payload.projectDTO.performUnit` | `sentinel_value_detected: 'N/A'` | `1,000` | Lọc các chuỗi khuyết danh (`N/A`, `NULL`, `NONE`, `-`) về `NULL` |
| 19 | `$.payload.projectDTO.projectPlanList[].planName` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 20 | `$.payload.projectDTO.projectPlanList[].isOldPlan` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 21 | `$.payload.projectDTO.period` | `sentinel_value_detected: '0'` | `1,000` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 22 | `$.payload.projectDTO.group` | `sentinel_value_detected: 'NA'` | `1,000` | Lọc các chuỗi khuyết danh (`N/A`, `NULL`, `NONE`, `-`) về `NULL` |
| 23 | `$.payload.pgroup` | `sentinel_value_detected: 'NA'` | `1,000` | Lọc các chuỗi khuyết danh (`N/A`, `NULL`, `NONE`, `-`) về `NULL` |
| 24 | `$.payload.pperiod` | `sentinel_value_detected: '0'` | `1,000` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 25 | `$.payload.linkedPublishPlan[].investorName` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 26 | `$.payload.linkedPublishPlan[].investTotal` | `sentinel_value_detected: '0'` | `1,000` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 27 | `$.payload.linkedPublishPlan[].isOda` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 28 | `$.payload.linkedPlanOverall[].investorName` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 29 | `$.payload.linkedPlanOverall[].isOda` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 30 | `$.payload.linkedPlanOverall[].period` | `sentinel_value_detected: '0'` | `1,000` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |

---

### 2.2. Bảng `khlcnt_plan_detail` (Resource: `khlcnt`)

- **Tổng bản ghi hoạt động (Active Rows):** `1,024,254` bản ghi (`1,421` files Parquet)
- **Số bản ghi mẫu phân tích sâu (Sampled Records):** `1,000` bản ghi
- **Khoảng thời gian mẫu khảo sát:** `2022-01-02` → `2022-07-20`
- **Tổng số trường phát hiện (Total Flattened Paths):** `160` trường
- **Khóa chính ứng viên đơn (Single Candidate Keys):** `$.source_id`, `$.ingested_at`, `$.content_hash`, `$.payload.bidPoBidpPlanProjectDetailView.id`, `$.payload.bidPoBidpPlanProjectDetailView.publicDate`, `$.payload.bidPoBidpPlanProjectDetailView.planNo`, `$._dlt_id`
- **Khóa ghép ứng viên (Composite Candidate Keys):** `$.source_id` + `$.source_version`

#### 2.2.1. Bảng Thống Kê Đầy Đủ 160 Trường Dữ Liệu Của `khlcnt_plan_detail`

| STT | Đường dẫn trường (`Path`) | Kiểu dữ liệu (`Types`) | Độ đầy đủ (`Tier`) | Populated % | Null % | Empty % | Distinct | Vai trò (`Role`) | Giá trị mẫu (`Top Samples`) | Vấn đề cụ thể phát hiện (`Specific Issues`) |
| :---: | :--- | :--- | :---: | ---: | ---: | ---: | ---: | :---: | :--- | :--- |
| 1 | `$` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Nested | *(Nested Object)* | Không (Clean) |
| 2 | `$._dlt_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `bKiD9xrTZIXxdg` (1), `cKOIaPF54j7OOg` (1) | Không (Clean) |
| 3 | `$._dlt_load_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 164 | Key | `1789464889.3990676` (30), `1789465063.0729125` (29) | Không (Clean) |
| 4 | `$.content_hash` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Free-text | `381266ea74b987b17528a13dfde74...` (1), `34f857d7c5d32eb9ceaad79fdb437...` (1) | Không (Clean) |
| 5 | `$.ingested_at` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Attribute | `2026-09-15 09:08:09.868171+00:00` (1), `2026-09-15 09:08:12.015703+00:00` (1) | Không (Clean) |
| 6 | `$.payload` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Nested | *(Nested Object)* | Không (Clean) |
| 7 | `$.payload.bidPoBidpPlanProjectDetailView` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 8 | `$.payload.bidPoBidpPlanProjectDetailView.bidPack` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 18 | Key | `1` (617), `2` (163) | Giá trị khuyết danh/Sentinel ('0') |
| 9 | `$.payload.bidPoBidpPlanProjectDetailView.cancelFileId` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 10 | `$.payload.bidPoBidpPlanProjectDetailView.cancelFileName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 11 | `$.payload.bidPoBidpPlanProjectDetailView.cancelReason` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 12 | `$.payload.bidPoBidpPlanProjectDetailView.competentPerson` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 21 | Key | `N/A` (974), `Ông Nguyễn Duy Bình - Phó Giá...` (6) | Giá trị khuyết danh/Sentinel ('N/A') |
| 13 | `$.payload.bidPoBidpPlanProjectDetailView.decisionAgency` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `N/A` (1000) | Giá trị khuyết danh/Sentinel ('N/A') |
| 14 | `$.payload.bidPoBidpPlanProjectDetailView.decisionDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 298 | Key | `2022-06-03T00:00:00` (18), `2022-06-06T00:00:00` (18) | Không (Clean) |
| 15 | `$.payload.bidPoBidpPlanProjectDetailView.decisionFileId` | `string, null` | **Full** | 99.5% | 0.5% | 0.0% | 995 | Key | `a7467425-4657-433b-b5f1-5965f...` (1), `e7a51b91-97c3-4ff6-95fa-5111a...` (1) | Không (Clean) |
| 16 | `$.payload.bidPoBidpPlanProjectDetailView.decisionFileName` | `string, null` | **Full** | 99.5% | 0.5% | 0.0% | 948 | Key | `QD phe duyet KHLCNT.pdf` (10), `KHLCNT.pdf` (6) | Không (Clean) |
| 17 | `$.payload.bidPoBidpPlanProjectDetailView.decisionNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 978 | Key | `34/QĐ-CCDS` (4), `168/QĐ-EVNNPT` (3) | Không (Clean) |
| 18 | `$.payload.bidPoBidpPlanProjectDetailView.id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `8809228217265904572` (1), `70665437143638444254` (1) | Không (Clean) |
| 19 | `$.payload.bidPoBidpPlanProjectDetailView.investForm` | `string, null` | **Sparse** | 57.7% | 42.3% | 0.0% | 1 | Key | `KHAC` (577) | Khuyết một phần (Null 42.3%) |
| 20 | `$.payload.bidPoBidpPlanProjectDetailView.investScale` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 21 | `$.payload.bidPoBidpPlanProjectDetailView.investTarget` | `null, string` | **Sparse** | 1.4% | 98.6% | 0.0% | 9 | Key | `Đáp ứng nhu cầu gaio thông, v...` (3), `Cung cấp điện` (1) | Thưa thớt cao (Null 98.6%) |
| 22 | `$.payload.bidPoBidpPlanProjectDetailView.investTotal` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 886 | Key | `0` (28), `30000000000` (7) | Giá trị khuyết danh/Sentinel ('0') |
| 23 | `$.payload.bidPoBidpPlanProjectDetailView.investTotalEx` | `int, null` | **Sparse** | 57.8% | 42.2% | 0.0% | 513 | Key | `30000000000` (6), `461817906` (6) | Khuyết một phần (Null 42.2%) |
| 24 | `$.payload.bidPoBidpPlanProjectDetailView.investTotalUnit` | `string, null` | **Mostly full** | 95.3% | 4.7% | 0.0% | 1 | Key | `VND` (953) | Không (Clean) |
| 25 | `$.payload.bidPoBidpPlanProjectDetailView.investmentFunds` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 26 | `$.payload.bidPoBidpPlanProjectDetailView.investorName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 594 | Key | `TỔNG CÔNG TY TRUYỀN TẢI ĐIỆN ...` (51), `Ban QLDA các công trình thành...` (16) | Dư khoảng trắng đầu/cuối (34 bản ghi) |
| 27 | `$.payload.bidPoBidpPlanProjectDetailView.isMultiProject` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 28 | `$.payload.bidPoBidpPlanProjectDetailView.isOda` | `bool, null` | **Full** | 99.1% | 0.9% | 0.0% | 2 | Key | `False` (978), `True` (13) | Không (Clean) |
| 29 | `$.payload.bidPoBidpPlanProjectDetailView.location` | `null, string` | **Sparse** | 1.5% | 98.5% | 0.0% | 11 | Key | `Tỉnh Đồng Nai` (3), `Tỉnh Bạc Liêu` (2) | Thưa thớt cao (Null 98.5%) |
| 30 | `$.payload.bidPoBidpPlanProjectDetailView.name` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 955 | Key | `Kế hoạch lựa chọn nhà thầu` (20), `cung cấp giải khát giữa giờ p...` (4) | Không (Clean) |
| 31 | `$.payload.bidPoBidpPlanProjectDetailView.newInvestorCode` | `null, string` | **Sparse** | 2.0% | 98.0% | 0.0% | 19 | Key | `vn6101266134` (2), `vnz000006672` (1) | Thưa thớt cao (Null 98.0%) |
| 32 | `$.payload.bidPoBidpPlanProjectDetailView.newInvestorName` | `null, string` | **Sparse** | 2.0% | 98.0% | 0.0% | 19 | Key | `Công ty TNHH MTV hữu cơ vi si...` (2), `CÔNG TY TƯ VẤN ĐIỆN MIỀN NAM ...` (1) | Thưa thớt cao (Null 98.0%); Dư khoảng trắng đầu/cuối (1 bản ghi) |
| 33 | `$.payload.bidPoBidpPlanProjectDetailView.pform` | `string, null` | **Sparse** | 58.8% | 41.2% | 0.0% | 4 | Key | `KHAC` (573), `TTQLDA` (6) | Khuyết một phần (Null 41.2%) |
| 34 | `$.payload.bidPoBidpPlanProjectDetailView.pgroup` | `string, null` | **Sparse** | 58.9% | 41.1% | 0.0% | 5 | Key | `NC` (368), `NB` (209) | Khuyết một phần (Null 41.1%); Giá trị khuyết danh/Sentinel ('NA') |
| 35 | `$.payload.bidPoBidpPlanProjectDetailView.pid` | `string, null` | **Sparse** | 62.9% | 37.1% | 0.0% | 621 | Key | `63778791392117779178` (2), `43298022566758431409` (2) | Khuyết một phần (Null 37.1%) |
| 36 | `$.payload.bidPoBidpPlanProjectDetailView.planNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `20220100496` (1), `20220102815` (1) | Không (Clean) |
| 37 | `$.payload.bidPoBidpPlanProjectDetailView.planType` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `DTPT` (643), `TX` (357) | Không (Clean) |
| 38 | `$.payload.bidPoBidpPlanProjectDetailView.planVersion` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `00` (1000) | Không (Clean) |
| 39 | `$.payload.bidPoBidpPlanProjectDetailView.pname` | `string, null` | **Mostly full** | 98.6% | 1.4% | 0.0% | 952 | Key | `cung cấp giải khát giữa giờ p...` (4), `Trạm biến áp 220 kV Gò Công v...` (3) | Dư khoảng trắng đầu/cuối (1 bản ghi) |
| 40 | `$.payload.bidPoBidpPlanProjectDetailView.pno` | `string, null` | **Full** | 99.1% | 0.9% | 0.0% | 622 | Key | `N/A` (362), `20220104809` (2) | Giá trị khuyết danh/Sentinel ('N/A') |
| 41 | `$.payload.bidPoBidpPlanProjectDetailView.pperiod` | `int, null` | **Sparse** | 58.9% | 41.1% | 0.0% | 10 | Key | `0` (569), `5` (5) | Khuyết một phần (Null 41.1%); Giá trị khuyết danh/Sentinel ('0') |
| 42 | `$.payload.bidPoBidpPlanProjectDetailView.pperiodUnit` | `int, null` | **Sparse** | 58.9% | 41.1% | 0.0% | 1 | Key | `1` (589) | Khuyết một phần (Null 41.1%) |
| 43 | `$.payload.bidPoBidpPlanProjectDetailView.publicDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `2022-01-02T18:04:50` (1), `2022-01-04T16:37:47` (1) | Không (Clean) |
| 44 | `$.payload.bidPoBidpPlanProjectDetailView.recCapital` | `null, int` | **Sparse** | 0.6% | 99.4% | 0.0% | 5 | Key | `425300000000` (2), `327345500000` (1) | Thưa thớt cao (Null 99.4%) |
| 45 | `$.payload.bidPoBidpPlanProjectDetailView.recCapitalUnit` | `string, null` | **Sparse** | 58.9% | 41.1% | 0.0% | 1 | Key | `VND` (589) | Khuyết một phần (Null 41.1%) |
| 46 | `$.payload.bidPoBidpPlanProjectDetailView.status` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `01` (979), `03` (21) | Không (Clean) |
| 47 | `$.payload.bidpPlanDetailToProjectList` | `array` | **Full** | 99.6% | 0.0% | 0.4% | 0 | Key | *(Nested Array)* | Chuỗi/Mảng rỗng (0.4%) |
| 48 | `$.payload.bidpPlanDetailToProjectList[]` | `object` | **Full** | 99.6% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 49 | `$.payload.bidpPlanDetailToProjectList[].additionalChoise` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 50 | `$.payload.bidpPlanDetailToProjectList[].additionalChoiseValue` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 51 | `$.payload.bidpPlanDetailToProjectList[].bidEstimatePrice` | `null, int` | **Sparse** | 12.7% | 86.9% | 0.0% | 202 | Key | `2000000000` (2), `82253042102` (1) | Thưa thớt cao (Null 86.9%) |
| 52 | `$.payload.bidpPlanDetailToProjectList[].bidField` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 6 | Key | `HH` (687), `XL` (600) | Không (Clean) |
| 53 | `$.payload.bidpPlanDetailToProjectList[].bidForm` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 3 | Key | `DTRR` (1528), `CHCT` (427) | Không (Clean) |
| 54 | `$.payload.bidpPlanDetailToProjectList[].bidMode` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 3 | Key | `1_MTHS` (1208), `1_HTHS` (884) | Không (Clean) |
| 55 | `$.payload.bidpPlanDetailToProjectList[].bidName` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 1,796 | Key | `Thi công xây dựng` (18), `Tư vấn giám sát thi công xây ...` (13) | Không (Clean) |
| 56 | `$.payload.bidpPlanDetailToProjectList[].bidNo` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 2,092 | Key | `BP2200089512` (2), `BP2200029655` (1) | Không (Clean) |
| 57 | `$.payload.bidpPlanDetailToProjectList[].bidPrice` | `int` | **Full** | 99.6% | 0.0% | 0.0% | 1,980 | Key | `200000000` (9), `150000000` (9) | Không (Clean) |
| 58 | `$.payload.bidpPlanDetailToProjectList[].bidPriceUnit` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 1 | Key | `VND` (2093) | Không (Clean) |
| 59 | `$.payload.bidpPlanDetailToProjectList[].bidRealityOpenDate` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 60 | `$.payload.bidpPlanDetailToProjectList[].bidStartMonth` | `int` | **Full** | 99.6% | 0.0% | 0.0% | 13 | Key | `0` (1799), `6` (63) | Giá trị khuyết danh/Sentinel ('0') |
| 61 | `$.payload.bidpPlanDetailToProjectList[].bidStartQuarter` | `string, null` | **Gaps present** | 85.6% | 14.0% | 0.0% | 4 | Key | `II` (798), `III` (529) | Không (Clean) |
| 62 | `$.payload.bidpPlanDetailToProjectList[].bidStartUnit` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 2 | Key | `Q` (1799), `M` (294) | Không (Clean) |
| 63 | `$.payload.bidpPlanDetailToProjectList[].bidStartYear` | `int` | **Full** | 99.6% | 0.0% | 0.0% | 11 | Key | `2022` (1791), `2023` (199) | Không (Clean) |
| 64 | `$.payload.bidpPlanDetailToProjectList[].bidTime` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 65 | `$.payload.bidpPlanDetailToProjectList[].capitalDetail` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 606 | Key | `EVNNPT` (200), `Ngân sách nhà nước` (116) | Không (Clean) |
| 66 | `$.payload.bidpPlanDetailToProjectList[].commodityLeft` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 67 | `$.payload.bidpPlanDetailToProjectList[].cperiod` | `int` | **Full** | 99.6% | 0.0% | 0.0% | 118 | Key | `30` (188), `60` (173) | Không (Clean) |
| 68 | `$.payload.bidpPlanDetailToProjectList[].cperiodUnit` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 2 | Key | `D` (1675), `M` (418) | Không (Clean) |
| 69 | `$.payload.bidpPlanDetailToProjectList[].createdBy` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 607 | Key | `vn0102743068` (228), `vn0100100417-03` (47) | Không (Clean) |
| 70 | `$.payload.bidpPlanDetailToProjectList[].createdDate` | `datetime` | **Full** | 99.6% | 0.0% | 0.0% | 2,093 | Key | `2022-10-19T15:40:53.781` (1), `2022-10-03T10:02:34.731` (1) | Không (Clean) |
| 71 | `$.payload.bidpPlanDetailToProjectList[].ctype` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 4 | Key | `TG` (1909), `DGCD` (178) | Không (Clean) |
| 72 | `$.payload.bidpPlanDetailToProjectList[].decisionFileId` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 73 | `$.payload.bidpPlanDetailToProjectList[].decisionFileName` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 74 | `$.payload.bidpPlanDetailToProjectList[].estimateCreatedBy` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 75 | `$.payload.bidpPlanDetailToProjectList[].estimateCreatedDate` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 76 | `$.payload.bidpPlanDetailToProjectList[].generalTasks` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 77 | `$.payload.bidpPlanDetailToProjectList[].id` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 2,093 | Key | `mi_40e6e536-d26b-4cec-a61c-da...` (1), `mi_f1d33527-c423-4422-9bc7-ce...` (1) | Không (Clean) |
| 78 | `$.payload.bidpPlanDetailToProjectList[].idDetail` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 2,093 | Key | `mi_40e6e536-d26b-4cec-a61c-da...` (1), `mi_f1d33527-c423-4422-9bc7-ce...` (1) | Không (Clean) |
| 79 | `$.payload.bidpPlanDetailToProjectList[].idPlan` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 996 | Key | `3932169743072311197` (32), `57940526142159654092` (22) | Không (Clean) |
| 80 | `$.payload.bidpPlanDetailToProjectList[].investorCode` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 81 | `$.payload.bidpPlanDetailToProjectList[].investorName` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 82 | `$.payload.bidpPlanDetailToProjectList[].isConcentrateShopping` | `int` | **Full** | 99.6% | 0.0% | 0.0% | 1 | Key | `0` (2093) | Giá trị khuyết danh/Sentinel ('0') |
| 83 | `$.payload.bidpPlanDetailToProjectList[].isDomestic` | `int` | **Full** | 99.6% | 0.0% | 0.0% | 2 | Key | `1` (2073), `0` (20) | Giá trị khuyết danh/Sentinel ('0') |
| 84 | `$.payload.bidpPlanDetailToProjectList[].isInternet` | `int` | **Full** | 99.6% | 0.0% | 0.0% | 2 | Key | `1` (1952), `0` (141) | Giá trị khuyết danh/Sentinel ('0') |
| 85 | `$.payload.bidpPlanDetailToProjectList[].isMultiLot` | `int` | **Full** | 99.6% | 0.0% | 0.0% | 1 | Key | `0` (2093) | Giá trị khuyết danh/Sentinel ('0') |
| 86 | `$.payload.bidpPlanDetailToProjectList[].isOda` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 87 | `$.payload.bidpPlanDetailToProjectList[].isPrequalification` | `int` | **Full** | 99.6% | 0.0% | 0.0% | 2 | Key | `0` (2090), `1` (3) | Giá trị khuyết danh/Sentinel ('0') |
| 88 | `$.payload.bidpPlanDetailToProjectList[].linkNotifyInfo` | `null, string` | **Sparse** | 44.6% | 55.0% | 0.0% | 708 | Key | `{"notifyNo":"IB2200027554","n...` (2), `{"notifyNo":"IB2300052806","n...` (1) | Khuyết một phần (Null 55.0%) |
| 89 | `$.payload.bidpPlanDetailToProjectList[].name` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 90 | `$.payload.bidpPlanDetailToProjectList[].notifyId` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 91 | `$.payload.bidpPlanDetailToProjectList[].notifyNo` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 92 | `$.payload.bidpPlanDetailToProjectList[].notifyVersion` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 93 | `$.payload.bidpPlanDetailToProjectList[].pgroup` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 94 | `$.payload.bidpPlanDetailToProjectList[].planDecisionDate` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 95 | `$.payload.bidpPlanDetailToProjectList[].planNo` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 996 | Key | `20220580394` (32), `20220558175` (22) | Không (Clean) |
| 96 | `$.payload.bidpPlanDetailToProjectList[].planType` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 97 | `$.payload.bidpPlanDetailToProjectList[].processApply` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 3 | Key | `LDT` (2086), `KHAC` (6) | Không (Clean) |
| 98 | `$.payload.bidpPlanDetailToProjectList[].procuringEntityCode` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 99 | `$.payload.bidpPlanDetailToProjectList[].procuringEntityName` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 100 | `$.payload.bidpPlanDetailToProjectList[].projectName` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 101 | `$.payload.bidpPlanDetailToProjectList[].projectsLinked` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 102 | `$.payload.bidpPlanDetailToProjectList[].publicDate` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 103 | `$.payload.bidpPlanDetailToProjectList[].serviceLeft` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 104 | `$.payload.bidpPlanDetailToProjectList[].status` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 105 | `$.payload.bidpPlanDetailToProjectList[].supervisor` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 106 | `$.payload.correctPacks` | `array` | **Sparse** | 0.0% | 0.0% | 100.0% | 0 | Nested | *(Nested Array)* | Chuỗi/Mảng rỗng (100.0%) |
| 107 | `$.payload.lsBidpPlanDetailDTO` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 108 | `$.payload.project` | `object, null` | **Sparse** | 58.9% | 41.1% | 0.0% | 0 | Nested | *(Nested Object)* | Khuyết một phần (Null 41.1%) |
| 109 | `$.payload.project.budgetRelCode` | `null` | **Sparse** | 0.0% | 58.9% | 0.0% | 0 | Key | *(Null / Rỗng)* | Khuyết một phần (Null 58.9%) |
| 110 | `$.payload.project.code` | `string, null` | **Sparse** | 57.8% | 1.1% | 0.0% | 570 | Key | `20220104809` (2), `20210842142` (2) | Không (Clean) |
| 111 | `$.payload.project.competentPersons` | `null` | **Sparse** | 0.0% | 58.9% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Khuyết một phần (Null 58.9%) |
| 112 | `$.payload.project.createdBy` | `string` | **Sparse** | 58.9% | 0.0% | 0.0% | 12 | Attribute | `SYSTEM` (578), `vnz000022483` (1) | Không (Clean) |
| 113 | `$.payload.project.createdDate` | `datetime` | **Sparse** | 58.9% | 0.0% | 0.0% | 581 | Timestamp | `2022-01-05T11:09:36` (2), `2021-08-17T09:48:57` (2) | Không (Clean) |
| 114 | `$.payload.project.decisionAgency` | `null, string` | **Sparse** | 2.8% | 56.1% | 0.0% | 22 | Attribute | `Ủy ban nhân dân tỉnh Đắk Nông` (4), `Bộ Công an` (3) | Khuyết một phần (Null 56.1%) |
| 115 | `$.payload.project.decisionDate` | `datetime` | **Sparse** | 58.9% | 0.0% | 0.0% | 281 | Timestamp | `2021-12-14T00:00:00` (18), `2021-10-29T00:00:00` (12) | Không (Clean) |
| 116 | `$.payload.project.decisionFileId` | `null, string` | **Sparse** | 2.8% | 56.1% | 0.0% | 28 | Key | `7858c9b1-a204-478b-b6bf-07a52...` (1), `cbeadb3f-8a60-4b5f-bce2-a6531...` (1) | Khuyết một phần (Null 56.1%) |
| 117 | `$.payload.project.decisionFileName` | `null, string` | **Sparse** | 2.8% | 56.1% | 0.0% | 28 | Free-text | `6.2 QĐ phê duyệt báo cáo KTKT...` (1), `190325.652.QĐ-UBND Báo cáo ng...` (1) | Khuyết một phần (Null 56.1%) |
| 118 | `$.payload.project.decisionNo` | `string` | **Sparse** | 58.9% | 0.0% | 0.0% | 571 | Key | `766/QĐ-EVNNPT` (3), `1648/QĐ-EVNNPT` (2) | Dư khoảng trắng đầu/cuối (46 bản ghi) |
| 119 | `$.payload.project.exChangeDate` | `null` | **Sparse** | 0.0% | 58.9% | 0.0% | 0 | Timestamp | *(Null / Rỗng)* | Khuyết một phần (Null 58.9%) |
| 120 | `$.payload.project.form` | `string, null` | **Sparse** | 58.8% | 0.1% | 0.0% | 4 | Attribute | `KHAC` (573), `TTQLDA` (6) | Không (Clean) |
| 121 | `$.payload.project.group` | `string` | **Sparse** | 58.9% | 0.0% | 0.0% | 5 | Attribute | `NC` (368), `NB` (209) | Giá trị khuyết danh/Sentinel ('NA') |
| 122 | `$.payload.project.hasLinkedPlan` | `null` | **Sparse** | 0.0% | 58.9% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Khuyết một phần (Null 58.9%) |
| 123 | `$.payload.project.id` | `string` | **Sparse** | 58.9% | 0.0% | 0.0% | 581 | Key | `63778791392117779178` (2), `43298022566758431409` (2) | Không (Clean) |
| 124 | `$.payload.project.investDetail` | `array` | **Sparse** | 58.9% | 0.0% | 0.0% | 524 | Nested | `[{'invest': 30000000000, 'mon...` (6), `[{'invest': 461817906, 'money...` (6) | Không (Clean) |
| 125 | `$.payload.project.investDetail[]` | `object` | **Sparse** | 58.9% | 0.0% | 0.0% | 535 | Nested | `{'invest': 30000000000, 'mone...` (6), `{'invest': 461817906, 'moneyT...` (6) | Không (Clean) |
| 126 | `$.payload.project.investDetail[].exchangeRate` | `null, float` | **Sparse** | 1.2% | 57.7% | 0.0% | 3 | Measure | `1.0` (10), `22280.0` (1) | Khuyết một phần (Null 57.7%) |
| 127 | `$.payload.project.investDetail[].invest` | `int` | **Sparse** | 58.9% | 0.0% | 0.0% | 529 | Measure | `30000000000` (6), `461817906` (6) | Không (Clean) |
| 128 | `$.payload.project.investDetail[].moneyType` | `string` | **Sparse** | 58.9% | 0.0% | 0.0% | 2 | Attribute | `VND` (599), `USD` (2) | Không (Clean) |
| 129 | `$.payload.project.investForm` | `string, null` | **Sparse** | 57.7% | 1.2% | 0.0% | 1 | Attribute | `KHAC` (577) | Không (Clean) |
| 130 | `$.payload.project.investScale` | `null` | **Sparse** | 0.0% | 58.9% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Khuyết một phần (Null 58.9%) |
| 131 | `$.payload.project.investTarget` | `null, string` | **Sparse** | 1.4% | 57.5% | 0.0% | 9 | Attribute | `Đáp ứng nhu cầu gaio thông, v...` (3), `Cung cấp điện` (1) | Khuyết một phần (Null 57.5%) |
| 132 | `$.payload.project.investTotal` | `int` | **Sparse** | 58.9% | 0.0% | 0.0% | 523 | Measure | `30000000000` (6), `461817906` (6) | Không (Clean) |
| 133 | `$.payload.project.investTotalEx` | `int, null` | **Sparse** | 57.8% | 1.1% | 0.0% | 513 | Measure | `30000000000` (6), `461817906` (6) | Không (Clean) |
| 134 | `$.payload.project.investTotalUnit` | `string` | **Sparse** | 58.9% | 0.0% | 0.0% | 1 | Attribute | `VND` (589) | Không (Clean) |
| 135 | `$.payload.project.investorCode` | `string` | **Sparse** | 58.9% | 0.0% | 0.0% | 314 | Key | `vn0102743068` (47), `vnz000008610` (16) | Không (Clean) |
| 136 | `$.payload.project.investorName` | `string` | **Sparse** | 58.9% | 0.0% | 0.0% | 320 | Free-text | `TỔNG CÔNG TY TRUYỀN TẢI ĐIỆN ...` (47), `Ban quản lý dự án các công tr...` (16) | Dư khoảng trắng đầu/cuối (9 bản ghi) |
| 137 | `$.payload.project.isConfirmStep` | `null` | **Sparse** | 0.0% | 58.9% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Khuyết một phần (Null 58.9%) |
| 138 | `$.payload.project.isOda` | `int` | **Sparse** | 58.9% | 0.0% | 0.0% | 2 | Measure | `0` (577), `1` (12) | Giá trị khuyết danh/Sentinel ('0') |
| 139 | `$.payload.project.isProjectWorkFirst` | `int` | **Sparse** | 58.9% | 0.0% | 0.0% | 2 | Measure | `0` (586), `1` (3) | Giá trị khuyết danh/Sentinel ('0') |
| 140 | `$.payload.project.latestAssignId` | `null` | **Sparse** | 0.0% | 58.9% | 0.0% | 0 | Key | *(Null / Rỗng)* | Khuyết một phần (Null 58.9%) |
| 141 | `$.payload.project.linkedPublishPlan` | `null` | **Sparse** | 0.0% | 58.9% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Khuyết một phần (Null 58.9%) |
| 142 | `$.payload.project.name` | `string` | **Sparse** | 58.9% | 0.0% | 0.0% | 570 | Free-text | `Trạm biến áp 220 kV Gò Công v...` (3), `Trạm biến áp 220 kV Nam Hòa` (2) | Dư khoảng trắng đầu/cuối (23 bản ghi) |
| 143 | `$.payload.project.no` | `string` | **Sparse** | 58.9% | 0.0% | 0.0% | 581 | Key | `20220104809` (2), `20210842142` (2) | Không (Clean) |
| 144 | `$.payload.project.performUnit` | `string, null` | **Sparse** | 57.8% | 1.1% | 0.0% | 1 | Attribute | `N/A` (578) | Giá trị khuyết danh/Sentinel ('N/A') |
| 145 | `$.payload.project.period` | `int` | **Sparse** | 58.9% | 0.0% | 0.0% | 10 | Measure | `0` (569), `5` (5) | Giá trị khuyết danh/Sentinel ('0') |
| 146 | `$.payload.project.periodUnit` | `int` | **Sparse** | 58.9% | 0.0% | 0.0% | 1 | Measure | `1` (589) | Không (Clean) |
| 147 | `$.payload.project.projectPlanList` | `null` | **Sparse** | 0.0% | 58.9% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Khuyết một phần (Null 58.9%) |
| 148 | `$.payload.project.publicDate` | `datetime` | **Sparse** | 58.9% | 0.0% | 0.0% | 581 | Timestamp | `2022-01-05T11:09:42` (2), `2021-08-17T09:54:33` (2) | Không (Clean) |
| 149 | `$.payload.project.reciprocalCapital` | `null, int` | **Sparse** | 0.6% | 58.3% | 0.0% | 5 | Measure | `425300000000` (2), `327345500000` (1) | Khuyết một phần (Null 58.3%) |
| 150 | `$.payload.project.reciprocalCapitalUnit` | `string` | **Sparse** | 58.9% | 0.0% | 0.0% | 1 | Attribute | `VND` (589) | Không (Clean) |
| 151 | `$.payload.project.sponsorCode` | `null, string` | **Sparse** | 1.2% | 57.7% | 0.0% | 1 | Key | `KHAC` (12) | Khuyết một phần (Null 57.7%) |
| 152 | `$.payload.project.status` | `string` | **Sparse** | 58.9% | 0.0% | 0.0% | 4 | Measure | `01` (581), `03` (4) | Không (Clean) |
| 153 | `$.payload.project.updatedBy` | `string` | **Sparse** | 58.9% | 0.0% | 0.0% | 334 | Timestamp | `vn0102743068` (47), `vnz000008610` (16) | Không (Clean) |
| 154 | `$.payload.project.updatedDate` | `datetime` | **Sparse** | 58.9% | 0.0% | 0.0% | 581 | Timestamp | `2022-10-03T10:01:58.578` (2), `2022-10-26T08:27:21.961` (2) | Không (Clean) |
| 155 | `$.payload.project.version` | `string` | **Sparse** | 58.9% | 0.0% | 0.0% | 4 | Measure | `00` (543), `01` (37) | Không (Clean) |
| 156 | `$.payload.project.versionDTOS` | `null` | **Sparse** | 0.0% | 58.9% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Khuyết một phần (Null 58.9%) |
| 157 | `$.run_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `33cab08bff5c4b84a9011c28f53f69f5` (1000) | Không (Clean) |
| 158 | `$.source_date` | `date` | **Full** | 100.0% | 0.0% | 0.0% | 164 | Timestamp | `2022-06-07` (30), `2022-06-14` (29) | Không (Clean) |
| 159 | `$.source_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `8809228217265904572` (1), `70665437143638444254` (1) | Không (Clean) |
| 160 | `$.source_version` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Measure | `00` (1000) | Không (Clean) |

#### 2.2.2. Tổng Hợp Các Vấn Đề Chất Lượng Dữ Liệu Cụ Thể Trên Bảng `khlcnt_plan_detail`

| Nhóm Vấn Đề Chất Lượng (Issue Category) | Số Trường Bị Ảnh Hưởng | Chi Tiết Trường & Bằng Chứng Số Liệu | Hướng Xử Lý Khi Sang Tầng Silver |
| :--- | :---: | :--- | :--- |
| **1. Trường rỗng hoàn toàn (`100% Null`)** | 7 | `$.payload.bidPoBidpPlanProjectDetailView.cancelFileId`, `$.payload.bidPoBidpPlanProjectDetailView.cancelFileName`, `$.payload.bidPoBidpPlanProjectDetailView.cancelReason`, `$.payload.bidPoBidpPlanProjectDetailView.investScale`, `$.payload.bidPoBidpPlanProjectDetailView.investmentFunds`, `$.payload.bidPoBidpPlanProjectDetailView.isMultiProject`, `$.payload.lsBidpPlanDetailDTO` | Không đưa vào các cột nghiệp vụ chính của Silver; vẫn bảo toàn trong `root_json` / `raw_json`.
| **2. Trường khuyết/thưa thớt (`20% ≤ Null < 100%`)** | 61 | `$.payload.bidPoBidpPlanProjectDetailView.investForm` (42.3%), `$.payload.bidPoBidpPlanProjectDetailView.investTarget` (98.6%), `$.payload.bidPoBidpPlanProjectDetailView.investTotalEx` (42.2%), `$.payload.bidPoBidpPlanProjectDetailView.location` (98.5%), `$.payload.bidPoBidpPlanProjectDetailView.newInvestorCode` (98.0%), `$.payload.bidPoBidpPlanProjectDetailView.newInvestorName` (98.0%), `$.payload.bidPoBidpPlanProjectDetailView.pform` (41.2%), `$.payload.bidPoBidpPlanProjectDetailView.pgroup` (41.1%), `$.payload.bidPoBidpPlanProjectDetailView.pid` (37.1%), `$.payload.bidPoBidpPlanProjectDetailView.pperiod` (41.1%), `$.payload.bidPoBidpPlanProjectDetailView.pperiodUnit` (41.1%), `$.payload.bidPoBidpPlanProjectDetailView.recCapital` (99.4%) *(và 49 trường khác)* | Cho phép `nullable=True` trên schema Silver; thiết lập chuỗi fallback đa tầng (VD: `isMultiLot`, `public_date`, `currency`).
| **3. Chuỗi rỗng hoặc Mảng rỗng (`Empty > 0%`)** | 2 | `$.payload.bidpPlanDetailToProjectList` (0.4%), `$.payload.correctPacks` (100.0%) | Hàm `clean_text()` tự động chuẩn hóa chuỗi rỗng `""` hoặc chỉ chứa khoảng trắng về `NULL`; mảng rỗng `[]` không sinh dòng bảng con.
| **4. Đa kiểu dữ liệu (`Mixed Types`)** | 0 | *(Không có)* | Ép kiểu tường minh qua bộ parser (`parse_money`, `parse_boolean`, `parse_date`, `parse_iso_datetime`); ghi nhận `quality_issue` nếu sai định dạng.

**Danh sách chi tiết toàn bộ 24 cảnh báo dị thường (Anomalies) trên các trường có dữ liệu:**

| STT | Trường Dữ Liệu (`Field Path`) | Loại Bất Thường Phát Hiện (`Detected Anomalies`) | Số Bản Ghi Ảnh Hưởng (Trên Mẫu) | Quy Tắc Làm Sạch Tại Silver |
| :---: | :--- | :--- | ---: | :--- |
| 1 | `$.payload.bidPoBidpPlanProjectDetailView.investorName` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 2 | `$.payload.bidPoBidpPlanProjectDetailView.newInvestorName` | `whitespace_padding` | `980` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 3 | `$.payload.bidPoBidpPlanProjectDetailView.competentPerson` | `sentinel_value_detected: 'N/A'` | `1,000` | Lọc các chuỗi khuyết danh (`N/A`, `NULL`, `NONE`, `-`) về `NULL` |
| 4 | `$.payload.bidPoBidpPlanProjectDetailView.investTotal` | `sentinel_value_detected: '0'` | `1,000` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 5 | `$.payload.bidPoBidpPlanProjectDetailView.decisionAgency` | `sentinel_value_detected: 'N/A'` | `1,000` | Lọc các chuỗi khuyết danh (`N/A`, `NULL`, `NONE`, `-`) về `NULL` |
| 6 | `$.payload.bidPoBidpPlanProjectDetailView.bidPack` | `sentinel_value_detected: '0'` | `1,000` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 7 | `$.payload.bidPoBidpPlanProjectDetailView.pname` | `whitespace_padding` | `14` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 8 | `$.payload.bidPoBidpPlanProjectDetailView.pno` | `sentinel_value_detected: 'N/A'` | `9` | Lọc các chuỗi khuyết danh (`N/A`, `NULL`, `NONE`, `-`) về `NULL` |
| 9 | `$.payload.bidPoBidpPlanProjectDetailView.pgroup` | `sentinel_value_detected: 'NA'` | `411` | Lọc các chuỗi khuyết danh (`N/A`, `NULL`, `NONE`, `-`) về `NULL` |
| 10 | `$.payload.bidPoBidpPlanProjectDetailView.pperiod` | `sentinel_value_detected: '0'` | `411` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 11 | `$.payload.bidpPlanDetailToProjectList[].isInternet` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 12 | `$.payload.bidpPlanDetailToProjectList[].isMultiLot` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 13 | `$.payload.bidpPlanDetailToProjectList[].isDomestic` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 14 | `$.payload.bidpPlanDetailToProjectList[].isPrequalification` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 15 | `$.payload.bidpPlanDetailToProjectList[].isConcentrateShopping` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 16 | `$.payload.bidpPlanDetailToProjectList[].bidStartMonth` | `sentinel_value_detected: '0'` | `1,000` | Giá trị sentinel `0` khi tính theo Quý -> chuyển thành `NULL` qua `_month()` / `_positive_year()` |
| 17 | `$.payload.project.name` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 18 | `$.payload.project.isOda` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 19 | `$.payload.project.investorName` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 20 | `$.payload.project.decisionNo` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 21 | `$.payload.project.isProjectWorkFirst` | `sentinel_value_detected: '0'` | `1,000` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 22 | `$.payload.project.performUnit` | `sentinel_value_detected: 'N/A'` | `422` | Lọc các chuỗi khuyết danh (`N/A`, `NULL`, `NONE`, `-`) về `NULL` |
| 23 | `$.payload.project.period` | `sentinel_value_detected: '0'` | `1,000` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 24 | `$.payload.project.group` | `sentinel_value_detected: 'NA'` | `1,000` | Lọc các chuỗi khuyết danh (`N/A`, `NULL`, `NONE`, `-`) về `NULL` |

---

### 2.3. Bảng `khlcnt_bid_package_detail` (Resource: `khlcnt`)

- **Tổng bản ghi hoạt động (Active Rows):** `2,278,752` bản ghi (`1,421` files Parquet)
- **Số bản ghi mẫu phân tích sâu (Sampled Records):** `1,000` bản ghi
- **Khoảng thời gian mẫu khảo sát:** `2022-01-02` → `2022-05-12`
- **Tổng số trường phát hiện (Total Flattened Paths):** `163` trường
- **Khóa chính ứng viên đơn (Single Candidate Keys):** `$.source_id`, `$.ingested_at`, `$.content_hash`, `$.payload.id`, `$.payload.bidNo`, `$.payload.exchangeDate`, `$.payload.createdDate`, `$.payload.updatedDate`, `$.payload.bidLocation[].id`, `$.payload.bidLocation[].createDate`, `$.payload.bidLocation[].bidDetailId`, `$.payload.originalCreateDate`, `$._dlt_id`

#### 2.3.1. Bảng Thống Kê Đầy Đủ 163 Trường Dữ Liệu Của `khlcnt_bid_package_detail`

| STT | Đường dẫn trường (`Path`) | Kiểu dữ liệu (`Types`) | Độ đầy đủ (`Tier`) | Populated % | Null % | Empty % | Distinct | Vai trò (`Role`) | Giá trị mẫu (`Top Samples`) | Vấn đề cụ thể phát hiện (`Specific Issues`) |
| :---: | :--- | :--- | :---: | ---: | ---: | ---: | ---: | :---: | :--- | :--- |
| 1 | `$` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Nested | *(Nested Object)* | Không (Clean) |
| 2 | `$._dlt_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `KX/rIsAC7f3KzA` (1), `DyrWryBc03Jy9A` (1) | Không (Clean) |
| 3 | `$._dlt_load_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 100 | Key | `1789463925.933984` (39), `1789464016.8091342` (37) | Không (Clean) |
| 4 | `$.content_hash` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Free-text | `06ad1c448f0fea08fa0b2f9730dac...` (1), `dd4095647d479969c5101dd3828dc...` (1) | Không (Clean) |
| 5 | `$.ingested_at` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Attribute | `2026-09-15 09:08:10.688633+00:00` (1), `2026-09-15 09:08:12.880177+00:00` (1) | Không (Clean) |
| 6 | `$.payload` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Nested | *(Nested Object)* | Không (Clean) |
| 7 | `$.payload.additionalChoise` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 8 | `$.payload.additionalChoiseValue` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 9 | `$.payload.agreeFrame` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 10 | `$.payload.appraisalFileId` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 11 | `$.payload.appraisalFileName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Free-text | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 12 | `$.payload.attachFileId` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 13 | `$.payload.attachFileName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Free-text | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 14 | `$.payload.baseMedicine` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 15 | `$.payload.bidBefore` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 16 | `$.payload.bidEstimatePrice` | `null, int` | **Sparse** | 14.6% | 85.4% | 0.0% | 146 | Key | `82253042102` (1), `57834176` (1) | Thưa thớt cao (Null 85.4%) |
| 17 | `$.payload.bidField` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 6 | Key | `HH` (296), `XL` (282) | Không (Clean) |
| 18 | `$.payload.bidForm` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Key | `DTRR` (774), `CHCT` (162) | Không (Clean) |
| 19 | `$.payload.bidFormDetail` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 20 | `$.payload.bidLocation` | `array` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key | *(Nested Array)* | Không (Clean) |
| 21 | `$.payload.bidLocation[]` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 22 | `$.payload.bidLocation[].action` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 23 | `$.payload.bidLocation[].bidDetailId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `mi_40e6e536-d26b-4cec-a61c-da...` (1), `mi_f1d33527-c423-4422-9bc7-ce...` (1) | Không (Clean) |
| 24 | `$.payload.bidLocation[].createBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `ADMIN` (1000) | Không (Clean) |
| 25 | `$.payload.bidLocation[].createDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `2022-09-16T23:33:59.563` (1), `2022-09-16T19:14:20.631` (1) | Không (Clean) |
| 26 | `$.payload.bidLocation[].districtCode` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 27 | `$.payload.bidLocation[].districtName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 28 | `$.payload.bidLocation[].id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `9d2613bc-f72e-40fa-ac60-d873c...` (1), `fe0cdafa-c557-4d4a-904d-06ad5...` (1) | Không (Clean) |
| 29 | `$.payload.bidLocation[].planId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 469 | Key | `71225350078452702813` (16), `07048613740496313121` (16) | Không (Clean) |
| 30 | `$.payload.bidLocation[].provCode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 61 | Key | `101` (267), `713` (64) | Không (Clean) |
| 31 | `$.payload.bidLocation[].provName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 61 | Key | `Thành phố Hà Nội` (267), `Tỉnh Đồng Nai` (64) | Không (Clean) |
| 32 | `$.payload.bidLocation[].wardCode` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 33 | `$.payload.bidLocation[].wardName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 34 | `$.payload.bidMode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Key | `1_MTHS` (536), `1_HTHS` (463) | Không (Clean) |
| 35 | `$.payload.bidName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 895 | Key | `Thi công xây dựng` (7), `Kiểm toán` (6) | Không (Clean) |
| 36 | `$.payload.bidNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `BP2200029655` (1), `BP2200007206` (1) | Không (Clean) |
| 37 | `$.payload.bidNoImg` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 38 | `$.payload.bidPackageSymbol` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 39 | `$.payload.bidPrice` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 962 | Key | `422464484` (6), `485000000` (5) | Không (Clean) |
| 40 | `$.payload.bidPriceDetail` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 962 | Key | `[{"invest":422464484,"exchang...` (6), `[{"invest":485000000,"exchang...` (5) | Không (Clean) |
| 41 | `$.payload.bidPriceEx` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 962 | Key | `422464484` (6), `485000000` (5) | Không (Clean) |
| 42 | `$.payload.bidPriceUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `VND` (1000) | Không (Clean) |
| 43 | `$.payload.bidStartMonth` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 13 | Key | `0` (870), `5` (30) | Giá trị khuyết danh/Sentinel ('0') |
| 44 | `$.payload.bidStartQuarter` | `string, null` | **Gaps present** | 87.0% | 13.0% | 0.0% | 4 | Key | `II` (387), `III` (181) | Không (Clean) |
| 45 | `$.payload.bidStartUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `Q` (870), `M` (130) | Không (Clean) |
| 46 | `$.payload.bidStartYear` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 9 | Key | `2022` (834), `2023` (102) | Không (Clean) |
| 47 | `$.payload.bidTime` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 48 | `$.payload.bidoBidEstimatePriceDTO` | `null, object` | **Sparse** | 14.6% | 85.4% | 0.0% | 0 | Key | *(Nested Object)* | Thưa thớt cao (Null 85.4%) |
| 49 | `$.payload.bidoBidEstimatePriceDTO.bidCloseDate` | `null, datetime` | **Sparse** | 13.9% | 0.7% | 0.0% | 130 | Key | `2023-03-10T10:00:00` (2), `2023-08-03T10:00:00` (2) | Không (Clean) |
| 50 | `$.payload.bidoBidEstimatePriceDTO.bidEstimatePrice` | `int` | **Sparse** | 14.6% | 0.0% | 0.0% | 146 | Key | `82253042102` (1), `57834176` (1) | Không (Clean) |
| 51 | `$.payload.bidoBidEstimatePriceDTO.bidId` | `string` | **Sparse** | 14.6% | 0.0% | 0.0% | 146 | Key | `mi_a847b863-64a0-443d-9f32-8d...` (1), `mi_ae6d26ee-0c50-410d-a54d-cf...` (1) | Không (Clean) |
| 52 | `$.payload.bidoBidEstimatePriceDTO.bidName` | `string` | **Sparse** | 14.6% | 0.0% | 0.0% | 133 | Key | `Kiểm toán dự án` (3), `Gói thầu số 8: Cung cấp và vậ...` (2) | Không (Clean) |
| 53 | `$.payload.bidoBidEstimatePriceDTO.bidNo` | `string` | **Sparse** | 14.6% | 0.0% | 0.0% | 146 | Key | `BP2200033675` (1), `BP2200010788` (1) | Không (Clean) |
| 54 | `$.payload.bidoBidEstimatePriceDTO.bidOpenDate` | `null, datetime` | **Sparse** | 13.9% | 0.7% | 0.0% | 131 | Key | `2023-03-10T10:00:00` (2), `2023-05-29T09:00:00` (2) | Không (Clean) |
| 55 | `$.payload.bidoBidEstimatePriceDTO.bidPrice` | `int` | **Sparse** | 14.6% | 0.0% | 0.0% | 145 | Key | `1500000000` (2), `80807580000` (1) | Không (Clean) |
| 56 | `$.payload.bidoBidEstimatePriceDTO.bidPriceUnit` | `string` | **Sparse** | 14.6% | 0.0% | 0.0% | 1 | Key | `VND` (146) | Không (Clean) |
| 57 | `$.payload.bidoBidEstimatePriceDTO.changeNum` | `int` | **Sparse** | 14.6% | 0.0% | 0.0% | 3 | Key | `1` (124), `2` (17) | Không (Clean) |
| 58 | `$.payload.bidoBidEstimatePriceDTO.createdBy` | `string` | **Sparse** | 14.6% | 0.0% | 0.0% | 86 | Key | `vnz000013163` (6), `vn0800356171-01` (5) | Không (Clean) |
| 59 | `$.payload.bidoBidEstimatePriceDTO.createdDate` | `datetime` | **Sparse** | 14.6% | 0.0% | 0.0% | 146 | Key | `2023-10-02T09:45:04.82` (1), `2023-03-29T10:28:23.596` (1) | Không (Clean) |
| 60 | `$.payload.bidoBidEstimatePriceDTO.decisionFileId` | `string` | **Sparse** | 14.6% | 0.0% | 0.0% | 146 | Key | `bd6be1d8-92df-418f-b1f8-8a952...` (1), `b3be0f8d-fab8-4918-9fdd-604b9...` (1) | Không (Clean) |
| 61 | `$.payload.bidoBidEstimatePriceDTO.decisionFileName` | `string` | **Sparse** | 14.6% | 0.0% | 0.0% | 140 | Key | `QD PD THIET KE.pdf` (2), `878_Quyet dinh phe duyet cap ...` (2) | Không (Clean) |
| 62 | `$.payload.bidoBidEstimatePriceDTO.id` | `string` | **Sparse** | 14.6% | 0.0% | 0.0% | 146 | Key | `cc4e91cb-69fe-4b3b-b3d0-c9cd4...` (1), `d2a13e41-c241-4472-96bb-b4aae...` (1) | Không (Clean) |
| 63 | `$.payload.bidoBidEstimatePriceDTO.investField` | `string` | **Sparse** | 14.6% | 0.0% | 0.0% | 4 | Key | `HH` (53), `TV` (35) | Không (Clean) |
| 64 | `$.payload.bidoBidEstimatePriceDTO.investorCode` | `string` | **Sparse** | 14.6% | 0.0% | 0.0% | 58 | Key | `vn0102743068` (32), `vn0100100417` (12) | Không (Clean) |
| 65 | `$.payload.bidoBidEstimatePriceDTO.investorName` | `string` | **Sparse** | 14.6% | 0.0% | 0.0% | 60 | Key | `TỔNG CÔNG TY TRUYỀN TẢI ĐIỆN ...` (32), `TỔNG CÔNG TY ĐIỆN LỰC MIỀN BẮC` (12) | Dư khoảng trắng đầu/cuối (5 bản ghi) |
| 66 | `$.payload.bidoBidEstimatePriceDTO.lastModifiedBy` | `string` | **Sparse** | 14.6% | 0.0% | 0.0% | 86 | Key | `vnz000013163` (6), `vn0800356171-01` (5) | Không (Clean) |
| 67 | `$.payload.bidoBidEstimatePriceDTO.lastModifiedDate` | `datetime` | **Sparse** | 14.6% | 0.0% | 0.0% | 146 | Key | `2023-10-02T10:29:48.596` (1), `2023-03-29T10:31:57.047` (1) | Không (Clean) |
| 68 | `$.payload.bidoBidEstimatePriceDTO.notifyId` | `null, string` | **Sparse** | 13.9% | 0.7% | 0.0% | 139 | Key | `54031222-fbb8-4625-aa03-a8b2d...` (1), `642258f5-351b-48cf-b560-79eff...` (1) | Không (Clean) |
| 69 | `$.payload.bidoBidEstimatePriceDTO.notifyNo` | `null, string` | **Sparse** | 13.9% | 0.7% | 0.0% | 139 | Key | `IB2300052806` (1), `IB2200079914` (1) | Không (Clean) |
| 70 | `$.payload.bidoBidEstimatePriceDTO.procuringEntityCode` | `string` | **Sparse** | 14.6% | 0.0% | 0.0% | 63 | Key | `vnz000012981` (16), `vnz000022540` (10) | Không (Clean) |
| 71 | `$.payload.bidoBidEstimatePriceDTO.procuringEntityName` | `string` | **Sparse** | 14.6% | 0.0% | 0.0% | 63 | Key | `BAN QUẢN LÝ DỰ ÁN TRUYỀN TẢI ...` (16), `Ban Quản lý dự án Lưới điện -...` (10) | Dư khoảng trắng đầu/cuối (3 bản ghi) |
| 72 | `$.payload.bidoBidEstimatePriceDTO.publicDate` | `null, datetime` | **Sparse** | 3.6% | 11.0% | 0.0% | 36 | Key | `2022-12-08T14:35:18.102` (1), `2022-11-29T11:14:54.806` (1) | Không (Clean) |
| 73 | `$.payload.bidpBidLotList` | `array, null` | **Full** | 99.6% | 0.4% | 0.0% | 0 | Key | *(Nested Array)* | Không (Clean) |
| 74 | `$.payload.bidpBidLotList[]` | `object` | **Full** | 99.6% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 75 | `$.payload.bidpBidLotList[].action` | `string` | **Sparse** | 0.0% | 0.0% | 99.6% | 0 | Key | *(Null / Rỗng)* | Chuỗi/Mảng rỗng (99.6%) |
| 76 | `$.payload.bidpBidLotList[].bidDetailId` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 996 | Key | `mi_40e6e536-d26b-4cec-a61c-da...` (1), `mi_f1d33527-c423-4422-9bc7-ce...` (1) | Không (Clean) |
| 77 | `$.payload.bidpBidLotList[].bidName` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 891 | Key | `Thi công xây dựng` (7), `Kiểm toán` (6) | Không (Clean) |
| 78 | `$.payload.bidpBidLotList[].bidNo` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 996 | Key | `BP2200029655` (1), `BP2200007206` (1) | Không (Clean) |
| 79 | `$.payload.bidpBidLotList[].contractType` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 80 | `$.payload.bidpBidLotList[].cperiod` | `int` | **Full** | 99.6% | 0.0% | 0.0% | 77 | Key | `60` (220), `30` (94) | Không (Clean) |
| 81 | `$.payload.bidpBidLotList[].cperiodUnit` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 2 | Key | `D` (802), `M` (194) | Không (Clean) |
| 82 | `$.payload.bidpBidLotList[].dangBaoChe` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 83 | `$.payload.bidpBidLotList[].duongDung` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 84 | `$.payload.bidpBidLotList[].groupMedicine` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 85 | `$.payload.bidpBidLotList[].id` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 996 | Key | `EA1DED10A7DEBBA2E053321810ACBE5B` (1), `EA1C9EC36201DB11E053331810ACB292` (1) | Không (Clean) |
| 86 | `$.payload.bidpBidLotList[].lotEstimatePrice` | `null, int` | **Sparse** | 12.4% | 87.2% | 0.0% | 124 | Key | `82253042102` (1), `57834176` (1) | Thưa thớt cao (Null 87.2%) |
| 87 | `$.payload.bidpBidLotList[].lotName` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 891 | Key | `Thi công xây dựng` (7), `Kiểm toán` (6) | Không (Clean) |
| 88 | `$.payload.bidpBidLotList[].lotNo` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 996 | Key | `BP2200029655` (1), `BP2200007206` (1) | Không (Clean) |
| 89 | `$.payload.bidpBidLotList[].lotPrice` | `int` | **Full** | 99.6% | 0.0% | 0.0% | 958 | Key | `422464484` (6), `485000000` (5) | Không (Clean) |
| 90 | `$.payload.bidpBidLotList[].medicineCode` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 91 | `$.payload.bidpBidLotList[].medicines` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 92 | `$.payload.bidpBidLotList[].nongDo` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 93 | `$.payload.bidpBidLotList[].percentageAdd` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 94 | `$.payload.bidpBidLotList[].planId` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 467 | Key | `71225350078452702813` (16), `07048613740496313121` (16) | Không (Clean) |
| 95 | `$.payload.bidpBidLotList[].planNo` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 467 | Key | `20220107310` (16), `20220376599` (16) | Không (Clean) |
| 96 | `$.payload.bidpBidLotList[].planVersion` | `string` | **Full** | 99.6% | 0.0% | 0.0% | 7 | Key | `00` (854), `01` (109) | Không (Clean) |
| 97 | `$.payload.bidpBidLotList[].pricePlan` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 98 | `$.payload.bidpBidLotList[].qualityStandards` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 99 | `$.payload.bidpBidLotList[].quantity` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 100 | `$.payload.bidpBidLotList[].quantityAdd` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 101 | `$.payload.bidpBidLotList[].tenThuoc` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 102 | `$.payload.bidpBidLotList[].uom` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 103 | `$.payload.bidpBidLotList[].valueAdd` | `null` | **Sparse** | 0.0% | 99.6% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 99.6%) |
| 104 | `$.payload.cancelBy` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 105 | `$.payload.cancelDate` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Timestamp | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 106 | `$.payload.cancelDecisionAgency` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 107 | `$.payload.cancelDecisionDate` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Timestamp | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 108 | `$.payload.cancelDecisionNo` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 109 | `$.payload.cancelFileAttachId` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 110 | `$.payload.cancelFileAttachName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Free-text | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 111 | `$.payload.cancelReason` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Free-text | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 112 | `$.payload.capitalClassify` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 113 | `$.payload.capitalDetail` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 287 | Attribute | `EVNNPT` (132), `Ngân sách nhà nước` (45) | Không (Clean) |
| 114 | `$.payload.capitalType` | `null, string` | **Sparse** | 46.2% | 53.8% | 0.0% | 2 | Measure | `0` (460), `1` (2) | Khuyết một phần (Null 53.8%); Giá trị khuyết danh/Sentinel ('0') |
| 115 | `$.payload.constructionInsurance` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 116 | `$.payload.correctingPacks` | `array` | **Sparse** | 0.0% | 0.0% | 100.0% | 0 | Nested | *(Nested Array)* | Chuỗi/Mảng rỗng (100.0%) |
| 117 | `$.payload.cperiod` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 85 | Measure | `30` (108), `60` (84) | Không (Clean) |
| 118 | `$.payload.cperiodUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Attribute | `D` (806), `M` (194) | Không (Clean) |
| 119 | `$.payload.createdBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 303 | Attribute | `vn0102743068` (149), `vn0100100417-03` (20) | Không (Clean) |
| 120 | `$.payload.createdDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Timestamp | `2022-10-19T15:40:53.781` (1), `2022-10-03T10:02:34.731` (1) | Không (Clean) |
| 121 | `$.payload.ctype` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Attribute | `TG` (915), `DGCD` (85) | Không (Clean) |
| 122 | `$.payload.exchangeDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Timestamp | `2022-09-16T23:33:59.563` (1), `2022-09-16T19:14:20.631` (1) | Không (Clean) |
| 123 | `$.payload.exclusiveBuy` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 124 | `$.payload.formValue` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 125 | `$.payload.generalTasks` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 126 | `$.payload.hospitalRetailGoods` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 127 | `$.payload.id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `mi_40e6e536-d26b-4cec-a61c-da...` (1), `mi_f1d33527-c423-4422-9bc7-ce...` (1) | Không (Clean) |
| 128 | `$.payload.investorName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Free-text | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 129 | `$.payload.isConcentrateShopping` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Measure | `0` (1000) | Giá trị khuyết danh/Sentinel ('0') |
| 130 | `$.payload.isDomestic` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Measure | `1` (988), `0` (12) | Giá trị khuyết danh/Sentinel ('0') |
| 131 | `$.payload.isHighTechRequired` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 132 | `$.payload.isInterest` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 133 | `$.payload.isInternet` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Measure | `1` (919), `0` (81) | Giá trị khuyết danh/Sentinel ('0') |
| 134 | `$.payload.isMedicine` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 135 | `$.payload.isMultiLot` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Measure | `0` (1000) | Giá trị khuyết danh/Sentinel ('0') |
| 136 | `$.payload.isOnlineShopping` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Measure | `0` (1000) | Giá trị khuyết danh/Sentinel ('0') |
| 137 | `$.payload.isPrequalification` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Measure | `0` (997), `1` (3) | Giá trị khuyết danh/Sentinel ('0') |
| 138 | `$.payload.isRented` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 139 | `$.payload.latestAssignId` | `string, null` | **Mostly full** | 98.7% | 1.3% | 0.0% | 987 | Key | `f1a7068c-4989-4598-8d7a-ea6b8...` (1), `b4a4cc64-ea85-444c-8f1b-74cc7...` (1) | Không (Clean) |
| 140 | `$.payload.linkNotifyInfo` | `null, string` | **Sparse** | 34.2% | 65.8% | 0.0% | 342 | Key | `{"notifyNo":"IB2300052806","n...` (1), `{"notifyNo":"IB2300000208","n...` (1) | Khuyết một phần (Null 65.8%) |
| 141 | `$.payload.location` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 142 | `$.payload.mixBidField` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 143 | `$.payload.originalCreateDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Timestamp | `2022-10-19T15:40:53.781` (1), `2022-10-03T10:02:34.731` (1) | Không (Clean) |
| 144 | `$.payload.planDecisionDate` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Timestamp | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 145 | `$.payload.planId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 469 | Key | `71225350078452702813` (16), `07048613740496313121` (16) | Không (Clean) |
| 146 | `$.payload.planName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Free-text | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 147 | `$.payload.planNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 469 | Key | `20220107310` (16), `20220376599` (16) | Không (Clean) |
| 148 | `$.payload.planVersion` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 4 | Measure | `00` (983), `01` (11) | Không (Clean) |
| 149 | `$.payload.processApply` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Attribute | `LDT` (995), `KHAC` (4) | Không (Clean) |
| 150 | `$.payload.projectsLinked` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 151 | `$.payload.recCapital` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Measure | `0` (1000) | Giá trị khuyết danh/Sentinel ('0') |
| 152 | `$.payload.recCapitalUnit` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 153 | `$.payload.resultDTO` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 154 | `$.payload.shoppingAgencyCode` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 155 | `$.payload.shortlists` | `array` | **Sparse** | 0.0% | 0.0% | 100.0% | 0 | Nested | *(Nested Array)* | Chuỗi/Mảng rỗng (100.0%) |
| 156 | `$.payload.supervisor` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 157 | `$.payload.underUnitCode` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 158 | `$.payload.updatedBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 303 | Timestamp | `vn0102743068` (149), `vn0100100417-03` (20) | Không (Clean) |
| 159 | `$.payload.updatedDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Timestamp | `2022-10-19T15:40:53.796` (1), `2022-10-05T14:37:51.424` (1) | Không (Clean) |
| 160 | `$.run_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `33cab08bff5c4b84a9011c28f53f69f5` (1000) | Không (Clean) |
| 161 | `$.source_date` | `date` | **Full** | 100.0% | 0.0% | 0.0% | 100 | Timestamp | `2022-03-30` (39), `2022-04-06` (37) | Không (Clean) |
| 162 | `$.source_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `mi_40e6e536-d26b-4cec-a61c-da...` (1), `mi_f1d33527-c423-4422-9bc7-ce...` (1) | Không (Clean) |
| 163 | `$.source_version` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |

#### 2.3.2. Tổng Hợp Các Vấn Đề Chất Lượng Dữ Liệu Cụ Thể Trên Bảng `khlcnt_bid_package_detail`

| Nhóm Vấn Đề Chất Lượng (Issue Category) | Số Trường Bị Ảnh Hưởng | Chi Tiết Trường & Bằng Chứng Số Liệu | Hướng Xử Lý Khi Sang Tầng Silver |
| :--- | :---: | :--- | :--- |
| **1. Trường rỗng hoàn toàn (`100% Null`)** | 48 | `$.payload.additionalChoise`, `$.payload.additionalChoiseValue`, `$.payload.agreeFrame`, `$.payload.appraisalFileId`, `$.payload.appraisalFileName`, `$.payload.attachFileId`, `$.payload.attachFileName`, `$.payload.baseMedicine`, `$.payload.bidBefore`, `$.payload.bidFormDetail`, `$.payload.bidLocation[].action`, `$.payload.bidLocation[].districtCode`, `$.payload.bidLocation[].districtName`, `$.payload.bidLocation[].wardCode`, `$.payload.bidLocation[].wardName` *(và 33 trường khác)* | Không đưa vào các cột nghiệp vụ chính của Silver; vẫn bảo toàn trong `root_json` / `raw_json`.
| **2. Trường khuyết/thưa thớt (`20% ≤ Null < 100%`)** | 20 | `$.payload.bidEstimatePrice` (85.4%), `$.payload.bidoBidEstimatePriceDTO` (85.4%), `$.payload.bidpBidLotList[].contractType` (99.6%), `$.payload.bidpBidLotList[].dangBaoChe` (99.6%), `$.payload.bidpBidLotList[].duongDung` (99.6%), `$.payload.bidpBidLotList[].groupMedicine` (99.6%), `$.payload.bidpBidLotList[].lotEstimatePrice` (87.2%), `$.payload.bidpBidLotList[].medicineCode` (99.6%), `$.payload.bidpBidLotList[].medicines` (99.6%), `$.payload.bidpBidLotList[].nongDo` (99.6%), `$.payload.bidpBidLotList[].percentageAdd` (99.6%), `$.payload.bidpBidLotList[].pricePlan` (99.6%) *(và 8 trường khác)* | Cho phép `nullable=True` trên schema Silver; thiết lập chuỗi fallback đa tầng (VD: `isMultiLot`, `public_date`, `currency`).
| **3. Chuỗi rỗng hoặc Mảng rỗng (`Empty > 0%`)** | 3 | `$.payload.bidpBidLotList[].action` (99.6%), `$.payload.correctingPacks` (100.0%), `$.payload.shortlists` (100.0%) | Hàm `clean_text()` tự động chuẩn hóa chuỗi rỗng `""` hoặc chỉ chứa khoảng trắng về `NULL`; mảng rỗng `[]` không sinh dòng bảng con.
| **4. Đa kiểu dữ liệu (`Mixed Types`)** | 0 | *(Không có)* | Ép kiểu tường minh qua bộ parser (`parse_money`, `parse_boolean`, `parse_date`, `parse_iso_datetime`); ghi nhận `quality_issue` nếu sai định dạng.

**Danh sách chi tiết toàn bộ 11 cảnh báo dị thường (Anomalies) trên các trường có dữ liệu:**

| STT | Trường Dữ Liệu (`Field Path`) | Loại Bất Thường Phát Hiện (`Detected Anomalies`) | Số Bản Ghi Ảnh Hưởng (Trên Mẫu) | Quy Tắc Làm Sạch Tại Silver |
| :---: | :--- | :--- | ---: | :--- |
| 1 | `$.payload.isDomestic` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 2 | `$.payload.isInternet` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 3 | `$.payload.isPrequalification` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 4 | `$.payload.capitalType` | `sentinel_value_detected: '0'` | `538` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 5 | `$.payload.recCapital` | `sentinel_value_detected: '0'` | `1,000` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 6 | `$.payload.bidStartMonth` | `sentinel_value_detected: '0'` | `1,000` | Giá trị sentinel `0` khi tính theo Quý -> chuyển thành `NULL` qua `_month()` / `_positive_year()` |
| 7 | `$.payload.isMultiLot` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 8 | `$.payload.isConcentrateShopping` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 9 | `$.payload.isOnlineShopping` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 10 | `$.payload.bidoBidEstimatePriceDTO.investorName` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 11 | `$.payload.bidoBidEstimatePriceDTO.procuringEntityName` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |

---

### 2.4. Bảng `notify_contractor_standard_detail` (Resource: `notify_contractor`)

- **Tổng bản ghi hoạt động (Active Rows):** `536,201` bản ghi (`1,181` files Parquet)
- **Số bản ghi mẫu phân tích sâu (Sampled Records):** `1,000` bản ghi
- **Khoảng thời gian mẫu khảo sát:** `2022-09-17` → `2022-09-28`
- **Tổng số trường phát hiện (Total Flattened Paths):** `424` trường
- **Khóa chính ứng viên đơn (Single Candidate Keys):** `$.source_id`, `$.ingested_at`, `$.content_hash`, `$.payload.bidoNotifyContractorM.id`, `$.payload.bidoNotifyContractorM.notifyNo`, `$.payload.bidoNotifyContractorM.publicDate`, `$.payload.bidoNotifyContractorM.originalPublicDate`, `$.payload.bidInvContractorOfflineDTO.id`, `$.payload.bidInvContractorOfflineDTO.notifyId`, `$.payload.bidInvContractorOfflineDTO.notifyNo`, `$.payload.bidInvContractorOfflineDTO.decisionFileId`, `$.payload.bidInvContractorOfflineDTO.createdDate`, `$.payload.bidCancelingResponse.id`, `$.payload.bidCancelingResponse.notifyNo`, `$.payload.bidCancelingResponse.publicDate`, `$.payload.bidNoContractorResponse.bidNotification.id`, `$.payload.bidNoContractorResponse.bidNotification.notifyNo`, `$.payload.bidNoContractorResponse.bidNotification.publicDate`, `$.payload.bidNoContractorResponse.bidNotification.lotDTOList[].notifyNo`, `$.payload.bidNoContractorResponse.bidNotification.lotDTOList[].notifyId`, `$.payload.bidNoContractorResponse.bidNotification.getVersionDTOS[].no`, `$.payload.bidNoContractorResponse.bidNotification.originalPublicDate`, `$._dlt_id`
- **Khóa ghép ứng viên (Composite Candidate Keys):** `$.source_id` + `$.source_version`

#### 2.4.1. Bảng Thống Kê Đầy Đủ 424 Trường Dữ Liệu Của `notify_contractor_standard_detail`

| STT | Đường dẫn trường (`Path`) | Kiểu dữ liệu (`Types`) | Độ đầy đủ (`Tier`) | Populated % | Null % | Empty % | Distinct | Vai trò (`Role`) | Giá trị mẫu (`Top Samples`) | Vấn đề cụ thể phát hiện (`Specific Issues`) |
| :---: | :--- | :--- | :---: | ---: | ---: | ---: | ---: | :---: | :--- | :--- |
| 1 | `$` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Nested | *(Nested Object)* | Không (Clean) |
| 2 | `$._dlt_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `xe1gn6UD9G2/3A` (1), `6WXDjxp6xQXbDw` (1) | Không (Clean) |
| 3 | `$._dlt_load_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 13 | Key | `1790962841.397079` (237), `1790962823.7155643` (187) | Không (Clean) |
| 4 | `$.content_hash` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Free-text | `2f39bef201fa524f112d86fc91836...` (1), `c75d5a7d82a6c8a84b0d33cc40069...` (1) | Không (Clean) |
| 5 | `$.ingested_at` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Attribute | `2026-09-15 13:06:35.106734+00:00` (1), `2026-09-15 13:06:35.244799+00:00` (1) | Không (Clean) |
| 6 | `$.payload` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Nested | *(Nested Object)* | Không (Clean) |
| 7 | `$.payload.bidCancelingResponse` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 8 | `$.payload.bidCancelingResponse.appraisalFileId` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 9 | `$.payload.bidCancelingResponse.appraisalFileName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 10 | `$.payload.bidCancelingResponse.bidCloseDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 450 | Key | `2022-10-07T09:00:00` (26), `2022-10-04T09:00:00` (19) | Không (Clean) |
| 11 | `$.payload.bidCancelingResponse.bidName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 862 | Key | `Xây lắp` (27), `Thi công xây dựng công trình` (19) | Không (Clean) |
| 12 | `$.payload.bidCancelingResponse.bidNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 994 | Key | `BP2200108663` (2), `BP2200112370` (2) | Không (Clean) |
| 13 | `$.payload.bidCancelingResponse.bidStatus` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 6 | Key | `PUB_KQLCNT` (900), `CANCEL_BID` (55) | Không (Clean) |
| 14 | `$.payload.bidCancelingResponse.cancelBy` | `null, string` | **Sparse** | 12.2% | 87.8% | 0.0% | 98 | Key | `vn0700794284` (5), `vn0900265739` (3) | Thưa thớt cao (Null 87.8%) |
| 15 | `$.payload.bidCancelingResponse.cancelDate` | `null, datetime` | **Sparse** | 12.2% | 87.8% | 0.0% | 122 | Key | `2022-09-17T10:57:28` (1), `2022-10-04T14:18:19` (1) | Thưa thớt cao (Null 87.8%) |
| 16 | `$.payload.bidCancelingResponse.cancelDecisionAgency` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 17 | `$.payload.bidCancelingResponse.cancelDecisionDate` | `null, datetime` | **Sparse** | 12.2% | 87.8% | 0.0% | 44 | Key | `2022-10-10T00:00:00` (8), `2022-10-19T00:00:00` (7) | Thưa thớt cao (Null 87.8%) |
| 18 | `$.payload.bidCancelingResponse.cancelDecisionNo` | `null, string` | **Sparse** | 12.2% | 87.8% | 0.0% | 119 | Key | `1621/QĐ-ĐHSPKTHY` (3), `242/QĐ-UBND` (2) | Thưa thớt cao (Null 87.8%) |
| 19 | `$.payload.bidCancelingResponse.cancelFileAttachId` | `null, string` | **Sparse** | 12.2% | 87.8% | 0.0% | 122 | Key | `b1e66e3d-cc17-4512-b0c1-6d3e8...` (1), `d00709c6-761c-4156-bfed-7a915...` (1) | Thưa thớt cao (Null 87.8%) |
| 20 | `$.payload.bidCancelingResponse.cancelFileAttachName` | `null, string` | **Sparse** | 12.2% | 87.8% | 0.0% | 115 | Key | `QĐ1621 UTEHY.pdf` (3), `QD huy thau.pdf` (3) | Thưa thớt cao (Null 87.8%) |
| 21 | `$.payload.bidCancelingResponse.cancelReason` | `null, string` | **Sparse** | 12.2% | 87.8% | 0.0% | 20 | Key | `TTQDPL` (73), `SPTDT` (26) | Thưa thớt cao (Null 87.8%) |
| 22 | `$.payload.bidCancelingResponse.cancelType` | `null, string` | **Sparse** | 12.2% | 87.8% | 0.0% | 2 | Key | `1` (92), `0` (30) | Thưa thớt cao (Null 87.8%); Giá trị khuyết danh/Sentinel ('0') |
| 23 | `$.payload.bidCancelingResponse.id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `2b353261-e958-4263-b9d3-4b339...` (1), `4bcce44d-8451-428d-bc4f-4ca86...` (1) | Không (Clean) |
| 24 | `$.payload.bidCancelingResponse.notifyNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `IB2200000066` (1), `IB2200000048` (1) | Không (Clean) |
| 25 | `$.payload.bidCancelingResponse.procuringEntityCode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 619 | Key | `vnz000013160` (14), `vn0307114021` (12) | Không (Clean) |
| 26 | `$.payload.bidCancelingResponse.procuringEntityName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 619 | Key | `Ban quản lý dự án đầu tư xây ...` (14), `Ban Quản lý dự án đầu tư xây ...` (12) | Dư khoảng trắng đầu/cuối (36 bản ghi) |
| 27 | `$.payload.bidCancelingResponse.publicDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `2022-09-17T11:00:02.034` (1), `2022-09-17T10:48:23.226` (1) | Không (Clean) |
| 28 | `$.payload.bidCancelingResponse.reportFileId` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 29 | `$.payload.bidCancelingResponse.reportFileName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 30 | `$.payload.bidCancelingResponse.status` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `01` (878), `03` (122) | Không (Clean) |
| 31 | `$.payload.bidContractorShortlistMDTO` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 32 | `$.payload.bidInvContractorOfflineDTO` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 33 | `$.payload.bidInvContractorOfflineDTO.briefFileId` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 34 | `$.payload.bidInvContractorOfflineDTO.briefFileIdEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 35 | `$.payload.bidInvContractorOfflineDTO.briefFileName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 36 | `$.payload.bidInvContractorOfflineDTO.briefFileNameEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 37 | `$.payload.bidInvContractorOfflineDTO.createdBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 651 | Key | `vn0307114021` (12), `vn2300109040` (11) | Không (Clean) |
| 38 | `$.payload.bidInvContractorOfflineDTO.createdDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `2022-09-17T00:49:39.965` (1), `2022-09-17T08:58:18.37` (1) | Không (Clean) |
| 39 | `$.payload.bidInvContractorOfflineDTO.decisionAgency` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 751 | Key | `Công ty trách nhiệm hữu hạn m...` (10), `Ban quản lý dự án đầu tư xây ...` (9) | Không (Clean) |
| 40 | `$.payload.bidInvContractorOfflineDTO.decisionAgencyEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 41 | `$.payload.bidInvContractorOfflineDTO.decisionDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 903 | Key | `2022-09-25T17:00:00.000+0000` (22), `2022-09-21T17:00:00.000+0000` (18) | Không (Clean) |
| 42 | `$.payload.bidInvContractorOfflineDTO.decisionFileId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `3dad1871-d032-488e-817a-a9a75...` (1), `0649a5aa-38ca-4cd5-aa65-9ccdd...` (1) | Không (Clean) |
| 43 | `$.payload.bidInvContractorOfflineDTO.decisionFileIdEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 44 | `$.payload.bidInvContractorOfflineDTO.decisionFileName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 890 | Key | `QĐ phê duyệt HSMT.pdf` (15), `Quyết định phê duyệt HSMT.pdf` (10) | Không (Clean) |
| 45 | `$.payload.bidInvContractorOfflineDTO.decisionFileNameEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 46 | `$.payload.bidInvContractorOfflineDTO.decisionNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 962 | Key | `10/2022/QĐ-PD/E-HSMT` (8), `5340/QĐ-BCCB` (8) | Không (Clean) |
| 47 | `$.payload.bidInvContractorOfflineDTO.decisionNoEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 48 | `$.payload.bidInvContractorOfflineDTO.fileId` | `null, string` | **Sparse** | 4.7% | 95.3% | 0.0% | 47 | Key | `2bbc54a6-b678-4db8-a2f9-8a53a...` (1), `1c7f2bae-5f29-44f2-9227-8be24...` (1) | Thưa thớt cao (Null 95.3%) |
| 49 | `$.payload.bidInvContractorOfflineDTO.fileIdEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 50 | `$.payload.bidInvContractorOfflineDTO.fileName` | `null, string` | **Sparse** | 4.7% | 95.3% | 0.0% | 44 | Key | `HSMT.pdf` (3), `E-HSMT tieng Viêt truc ngang ...` (2) | Thưa thớt cao (Null 95.3%) |
| 51 | `$.payload.bidInvContractorOfflineDTO.fileNameEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 52 | `$.payload.bidInvContractorOfflineDTO.id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `2b353261-e958-4263-b9d3-4b339...` (1), `4bcce44d-8451-428d-bc4f-4ca86...` (1) | Không (Clean) |
| 53 | `$.payload.bidInvContractorOfflineDTO.listOtherFile` | `null, string` | **Sparse** | 4.7% | 95.3% | 0.0% | 13 | Key | `{"otherFileId1":null,"otherFi...` (32), `{"otherFileId1":"4139f733-6a2...` (1) | Thưa thớt cao (Null 95.3%) |
| 54 | `$.payload.bidInvContractorOfflineDTO.listOtherFileEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 55 | `$.payload.bidInvContractorOfflineDTO.notifyId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `2b353261-e958-4263-b9d3-4b339...` (1), `4bcce44d-8451-428d-bc4f-4ca86...` (1) | Không (Clean) |
| 56 | `$.payload.bidInvContractorOfflineDTO.notifyNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `IB2200000066` (1), `IB2200000048` (1) | Không (Clean) |
| 57 | `$.payload.bidInvContractorOfflineDTO.notifyVersion` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 4 | Key | `00` (918), `01` (71) | Không (Clean) |
| 58 | `$.payload.bidInvContractorOfflineDTO.otherFileId` | `null, string` | **Sparse** | 0.7% | 99.3% | 0.0% | 7 | Key | `7ed5a1b2-6b67-4c54-8c2f-fde70...` (1), `d7314036-686b-441f-90ad-c8a5d...` (1) | Thưa thớt cao (Null 99.3%) |
| 59 | `$.payload.bidInvContractorOfflineDTO.otherFileIdEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 60 | `$.payload.bidInvContractorOfflineDTO.otherFileName` | `null, string` | **Sparse** | 0.7% | 99.3% | 0.0% | 5 | Key | `4. QĐ phê duyệt KH thuốc 1365...` (3), `4. QĐ phê duyệt KH VX 1366 ng...` (1) | Thưa thớt cao (Null 99.3%) |
| 61 | `$.payload.bidInvContractorOfflineDTO.otherFileNameEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 62 | `$.payload.bidInvContractorOfflineDTO.updatedBy` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 63 | `$.payload.bidInvContractorOfflineDTO.updatedDate` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 64 | `$.payload.bidNoContractorResponse` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 65 | `$.payload.bidNoContractorResponse.bidNotification` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 66 | `$.payload.bidNoContractorResponse.bidNotification.additionalChoise` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 67 | `$.payload.bidNoContractorResponse.bidNotification.additionalPrice` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 68 | `$.payload.bidNoContractorResponse.bidNotification.baseMedicine` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 69 | `$.payload.bidNoContractorResponse.bidNotification.bidBefore` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 70 | `$.payload.bidNoContractorResponse.bidNotification.bidCloseDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 450 | Key | `2022-10-07T09:00:00` (26), `2022-10-04T09:00:00` (19) | Không (Clean) |
| 71 | `$.payload.bidNoContractorResponse.bidNotification.bidEstimatePrice` | `null, int` | **Sparse** | 50.4% | 49.6% | 0.0% | 491 | Key | `770200000` (2), `4200000000` (2) | Khuyết một phần (Null 49.6%) |
| 72 | `$.payload.bidNoContractorResponse.bidNotification.bidForm` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Key | `DTRR` (568), `CHCT` (305) | Không (Clean) |
| 73 | `$.payload.bidNoContractorResponse.bidNotification.bidGuaranteeFormEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 74 | `$.payload.bidNoContractorResponse.bidNotification.bidId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 995 | Key | `b82103dc-d049-4b36-936b-a27d5...` (2), `1b92563f-347e-409f-a644-9ca16...` (2) | Không (Clean) |
| 75 | `$.payload.bidNoContractorResponse.bidNotification.bidMode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `1_MTHS` (798), `1_HTHS` (202) | Không (Clean) |
| 76 | `$.payload.bidNoContractorResponse.bidNotification.bidName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 862 | Key | `Xây lắp` (27), `Thi công xây dựng công trình` (19) | Không (Clean) |
| 77 | `$.payload.bidNoContractorResponse.bidNotification.bidNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 994 | Key | `BP2200108663` (2), `BP2200112370` (2) | Không (Clean) |
| 78 | `$.payload.bidNoContractorResponse.bidNotification.bidNoType` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `TBMT` (1000) | Không (Clean) |
| 79 | `$.payload.bidNoContractorResponse.bidNotification.bidOpenDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 479 | Key | `2022-10-07T09:00:00` (24), `2022-10-06T15:00:00` (18) | Không (Clean) |
| 80 | `$.payload.bidNoContractorResponse.bidNotification.bidOpenLocation` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 40 | Key | `https://muasamcong.mpi.gov.vn` (953), `Trung tâm Y tế huyện Hồng Dân` (4) | Dư khoảng trắng đầu/cuối (4 bản ghi) |
| 81 | `$.payload.bidNoContractorResponse.bidNotification.bidOpenLocationEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 82 | `$.payload.bidNoContractorResponse.bidNotification.bidPrice` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 976 | Key | `180000000` (3), `125701000` (2) | Không (Clean) |
| 83 | `$.payload.bidNoContractorResponse.bidNotification.bidPriceUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `VND` (1000) | Không (Clean) |
| 84 | `$.payload.bidNoContractorResponse.bidNotification.bidValidityPeriod` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 18 | Key | `90` (336), `60` (229) | Không (Clean) |
| 85 | `$.payload.bidNoContractorResponse.bidNotification.bidValidityPeriodUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `D` (1000) | Không (Clean) |
| 86 | `$.payload.bidNoContractorResponse.bidNotification.capitalDetail` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 640 | Key | `Ngân sách nhà nước` (40), `Ngân sách Nhà nước` (27) | Không (Clean) |
| 87 | `$.payload.bidNoContractorResponse.bidNotification.cdtType` | `int, null` | **Sparse** | 53.9% | 46.1% | 0.0% | 2 | Key | `0` (529), `1` (10) | Khuyết một phần (Null 46.1%); Giá trị khuyết danh/Sentinel ('0') |
| 88 | `$.payload.bidNoContractorResponse.bidNotification.contractPeriod` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 77 | Key | `30` (129), `90` (109) | Không (Clean) |
| 89 | `$.payload.bidNoContractorResponse.bidNotification.contractPeriodUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `D` (833), `M` (167) | Không (Clean) |
| 90 | `$.payload.bidNoContractorResponse.bidNotification.contractType` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 5 | Key | `TG` (944), `DGCD` (45) | Không (Clean) |
| 91 | `$.payload.bidNoContractorResponse.bidNotification.createdBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 650 | Key | `vn0307114021` (12), `vn2300109040` (11) | Không (Clean) |
| 92 | `$.payload.bidNoContractorResponse.bidNotification.delayDTOList` | `null, array` | **Sparse** | 10.4% | 89.6% | 0.0% | 0 | Key | *(Nested Array)* | Thưa thớt cao (Null 89.6%) |
| 93 | `$.payload.bidNoContractorResponse.bidNotification.delayDTOList[]` | `object` | **Sparse** | 10.4% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 94 | `$.payload.bidNoContractorResponse.bidNotification.delayDTOList[].bidCloseDate` | `datetime` | **Sparse** | 10.4% | 0.0% | 0.0% | 101 | Key | `2022-10-06T09:00:00` (7), `2022-10-06T15:00:00` (6) | Không (Clean) |
| 95 | `$.payload.bidNoContractorResponse.bidNotification.delayDTOList[].bidCloseDelayDate` | `datetime` | **Sparse** | 10.4% | 0.0% | 0.0% | 84 | Key | `2022-10-06T15:00:00` (21), `2022-10-14T09:00:00` (5) | Không (Clean) |
| 96 | `$.payload.bidNoContractorResponse.bidNotification.delayDTOList[].bidName` | `string` | **Sparse** | 10.4% | 0.0% | 0.0% | 99 | Key | `Công tác thu gom, vận chuyển ...` (8), `gói thầu: Mua xe phục vụ công...` (7) | Không (Clean) |
| 97 | `$.payload.bidNoContractorResponse.bidNotification.delayDTOList[].bidNo` | `string` | **Sparse** | 10.4% | 0.0% | 0.0% | 104 | Key | `BP2200114434` (7), `BP2200112694` (4) | Không (Clean) |
| 98 | `$.payload.bidNoContractorResponse.bidNotification.delayDTOList[].bidOpenDate` | `datetime` | **Sparse** | 10.4% | 0.0% | 0.0% | 105 | Key | `2022-10-06T09:00:00` (7), `2022-10-06T15:00:00` (6) | Không (Clean) |
| 99 | `$.payload.bidNoContractorResponse.bidNotification.delayDTOList[].bidOpenDelayDate` | `datetime` | **Sparse** | 10.4% | 0.0% | 0.0% | 90 | Key | `2022-10-06T15:00:00` (21), `2022-10-03T09:00:00` (4) | Không (Clean) |
| 100 | `$.payload.bidNoContractorResponse.bidNotification.delayDTOList[].createdBy` | `string` | **Sparse** | 10.4% | 0.0% | 0.0% | 77 | Key | `root` (25), `vn0314643354` (6) | Không (Clean) |
| 101 | `$.payload.bidNoContractorResponse.bidNotification.delayDTOList[].createdDate` | `datetime` | **Sparse** | 10.4% | 0.0% | 0.0% | 136 | Key | `2022-09-27T15:37:06.982` (1), `2022-09-27T15:38:06.57` (1) | Không (Clean) |
| 102 | `$.payload.bidNoContractorResponse.bidNotification.delayDTOList[].id` | `string` | **Sparse** | 10.4% | 0.0% | 0.0% | 136 | Key | `81e976bf-cdf7-4f12-ba31-4b494...` (1), `3a5008e8-b54e-4243-9a0d-a338f...` (1) | Không (Clean) |
| 103 | `$.payload.bidNoContractorResponse.bidNotification.delayDTOList[].notifyList` | `null` | **Sparse** | 0.0% | 10.4% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 104 | `$.payload.bidNoContractorResponse.bidNotification.delayDTOList[].notifyNo` | `string` | **Sparse** | 10.4% | 0.0% | 0.0% | 104 | Key | `IB2200002045` (7), `IB2200003720` (4) | Không (Clean) |
| 105 | `$.payload.bidNoContractorResponse.bidNotification.delayDTOList[].notifyType` | `string` | **Sparse** | 10.4% | 0.0% | 0.0% | 1 | Key | `TBMT` (136) | Không (Clean) |
| 106 | `$.payload.bidNoContractorResponse.bidNotification.delayDTOList[].notifyVersion` | `string` | **Sparse** | 10.4% | 0.0% | 0.0% | 2 | Key | `00` (129), `01` (7) | Không (Clean) |
| 107 | `$.payload.bidNoContractorResponse.bidNotification.delayDTOList[].reason` | `string` | **Sparse** | 10.4% | 0.0% | 0.0% | 78 | Key | `Gia hạn tự động các gói thầu ...` (25), `Không có nhà thầu tham dự` (13) | Dư khoảng trắng đầu/cuối (1 bản ghi) |
| 108 | `$.payload.bidNoContractorResponse.bidNotification.delayDTOList[].sendEmailInfos` | `null` | **Sparse** | 0.0% | 10.4% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 109 | `$.payload.bidNoContractorResponse.bidNotification.deliveryCapability` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 110 | `$.payload.bidNoContractorResponse.bidNotification.executionLocation` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 111 | `$.payload.bidNoContractorResponse.bidNotification.feeType` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `F` (965), `C` (35) | Không (Clean) |
| 112 | `$.payload.bidNoContractorResponse.bidNotification.feeValue` | `null, int` | **Sparse** | 3.5% | 96.5% | 0.0% | 3 | Key | `2000000` (18), `1000000` (12) | Thưa thớt cao (Null 96.5%) |
| 113 | `$.payload.bidNoContractorResponse.bidNotification.getVersionDTOS` | `array` | **Full** | 100.0% | 0.0% | 0.0% | 912 | Key | `[{'id': '2b353261-e958-4263-b...` (1), `[{'id': '4bcce44d-8451-428d-b...` (1) | Không (Clean) |
| 114 | `$.payload.bidNoContractorResponse.bidNotification.getVersionDTOS[]` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 1,102 | Key | `{'id': '2b353261-e958-4263-b9...` (1), `{'id': '4bcce44d-8451-428d-bc...` (1) | Không (Clean) |
| 115 | `$.payload.bidNoContractorResponse.bidNotification.getVersionDTOS[].id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,102 | Key | `2b353261-e958-4263-b9d3-4b339...` (1), `4bcce44d-8451-428d-bc4f-4ca86...` (1) | Không (Clean) |
| 116 | `$.payload.bidNoContractorResponse.bidNotification.getVersionDTOS[].no` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `IB2200001962` (4), `IB2200003145` (4) | Không (Clean) |
| 117 | `$.payload.bidNoContractorResponse.bidNotification.getVersionDTOS[].publicBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 644 | Key | `vn0307114021` (13), `vn2300109040` (12) | Không (Clean) |
| 118 | `$.payload.bidNoContractorResponse.bidNotification.getVersionDTOS[].publicDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,096 | Key | `2022-09-23T17:06:26.815` (2), `2022-09-26T15:38:42.133` (2) | Không (Clean) |
| 119 | `$.payload.bidNoContractorResponse.bidNotification.getVersionDTOS[].status` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Key | `01` (974), `03` (122) | Không (Clean) |
| 120 | `$.payload.bidNoContractorResponse.bidNotification.getVersionDTOS[].version` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 4 | Key | `00` (1000), `01` (88) | Không (Clean) |
| 121 | `$.payload.bidNoContractorResponse.bidNotification.guaranteeForm` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Key | `Thư bảo lãnh` (533), `Cam kết` (434) | Không (Clean) |
| 122 | `$.payload.bidNoContractorResponse.bidNotification.guaranteeValue` | `int, null` | **Sparse** | 74.9% | 25.1% | 0.0% | 267 | Key | `30000000` (29), `5000000` (29) | Khuyết một phần (Null 25.1%) |
| 123 | `$.payload.bidNoContractorResponse.bidNotification.id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `2b353261-e958-4263-b9d3-4b339...` (1), `4bcce44d-8451-428d-bc4f-4ca86...` (1) | Không (Clean) |
| 124 | `$.payload.bidNoContractorResponse.bidNotification.incentives` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 125 | `$.payload.bidNoContractorResponse.bidNotification.investField` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 5 | Key | `HH` (378), `XL` (354) | Không (Clean) |
| 126 | `$.payload.bidNoContractorResponse.bidNotification.investorCode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 637 | Key | `vnz000013160` (14), `vn0307114021` (12) | Không (Clean) |
| 127 | `$.payload.bidNoContractorResponse.bidNotification.investorName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 643 | Key | `Ban quản lý dự án đầu tư xây ...` (14), `Ban Quản lý dự án đầu tư xây ...` (12) | Dư khoảng trắng đầu/cuối (30 bản ghi) |
| 128 | `$.payload.bidNoContractorResponse.bidNotification.isAgreeFrame` | `null, int` | **Sparse** | 0.6% | 99.4% | 0.0% | 2 | Key | `1` (4), `0` (2) | Thưa thớt cao (Null 99.4%); Giá trị khuyết danh/Sentinel ('0') |
| 129 | `$.payload.bidNoContractorResponse.bidNotification.isAttachFlowEnabled` | `bool` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `False` (1000) | Không (Clean) |
| 130 | `$.payload.bidNoContractorResponse.bidNotification.isDomestic` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `1` (997), `0` (3) | Giá trị khuyết danh/Sentinel ('0') |
| 131 | `$.payload.bidNoContractorResponse.bidNotification.isEarly` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 132 | `$.payload.bidNoContractorResponse.bidNotification.isElig29B` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 133 | `$.payload.bidNoContractorResponse.bidNotification.isGoods` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 134 | `$.payload.bidNoContractorResponse.bidNotification.isInternet` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `1` (953), `0` (47) | Giá trị khuyết danh/Sentinel ('0') |
| 135 | `$.payload.bidNoContractorResponse.bidNotification.isMedicine` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 136 | `$.payload.bidNoContractorResponse.bidNotification.isMultiLot` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 137 | `$.payload.bidNoContractorResponse.bidNotification.isPrequalification` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 138 | `$.payload.bidNoContractorResponse.bidNotification.isPriorityBidForm` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 139 | `$.payload.bidNoContractorResponse.bidNotification.isRented` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 140 | `$.payload.bidNoContractorResponse.bidNotification.isSustainably` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 141 | `$.payload.bidNoContractorResponse.bidNotification.issueLocation` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `https://muasamcong.mpi.gov.vn` (1000) | Không (Clean) |
| 142 | `$.payload.bidNoContractorResponse.bidNotification.language` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 143 | `$.payload.bidNoContractorResponse.bidNotification.lotDTOList` | `array` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key | *(Nested Array)* | Không (Clean) |
| 144 | `$.payload.bidNoContractorResponse.bidNotification.lotDTOList[]` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 145 | `$.payload.bidNoContractorResponse.bidNotification.lotDTOList[].actions` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 146 | `$.payload.bidNoContractorResponse.bidNotification.lotDTOList[].bidNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 994 | Key | `BP2200111699` (1462), `BP2200109937` (293) | Không (Clean) |
| 147 | `$.payload.bidNoContractorResponse.bidNotification.lotDTOList[].cperiod` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 77 | Key | `12` (2117), `24` (261) | Không (Clean) |
| 148 | `$.payload.bidNoContractorResponse.bidNotification.lotDTOList[].cperiodUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `M` (2497), `D` (870) | Không (Clean) |
| 149 | `$.payload.bidNoContractorResponse.bidNotification.lotDTOList[].id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3,367 | Key | `a27e39ff-0a60-474c-86ba-de1b0...` (1), `45c2a93d-e854-4e1f-a879-829b4...` (1) | Không (Clean) |
| 150 | `$.payload.bidNoContractorResponse.bidNotification.lotDTOList[].lotEstimatePrice` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 3,039 | Key | `25200000` (7), `21000000` (7) | Không (Clean) |
| 151 | `$.payload.bidNoContractorResponse.bidNotification.lotDTOList[].lotGuaranteeValue` | `int, null, float` | **Sparse** | 74.9% | 25.1% | 0.0% | 2,324 | Key | `5000000` (32), `30000000` (30) | Khuyết một phần (Null 25.1%); Đa kiểu dữ liệu (int, float) |
| 152 | `$.payload.bidNoContractorResponse.bidNotification.lotDTOList[].lotName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3,042 | Key | `Xây lắp` (27), `Thi công xây dựng công trình` (19) | Không (Clean) |
| 153 | `$.payload.bidNoContractorResponse.bidNotification.lotDTOList[].lotNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3,361 | Key | `BP2200108663` (2), `BP2200112370` (2) | Không (Clean) |
| 154 | `$.payload.bidNoContractorResponse.bidNotification.lotDTOList[].lotPrice` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 3,040 | Key | `25200000` (7), `21000000` (7) | Không (Clean) |
| 155 | `$.payload.bidNoContractorResponse.bidNotification.lotDTOList[].notifyId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `b126ac29-9161-425c-af56-a16e3...` (1462), `107d7a51-66a5-4e23-ac10-9b0cf...` (293) | Không (Clean) |
| 156 | `$.payload.bidNoContractorResponse.bidNotification.lotDTOList[].notifyNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `IB2200002525` (1462), `IB2200001070` (293) | Không (Clean) |
| 157 | `$.payload.bidNoContractorResponse.bidNotification.lotDTOList[].planNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 893 | Key | `PL2200005170` (1462), `PL2200004643` (378) | Không (Clean) |
| 158 | `$.payload.bidNoContractorResponse.bidNotification.medicalType` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `0` (1000) | Giá trị khuyết danh/Sentinel ('0') |
| 159 | `$.payload.bidNoContractorResponse.bidNotification.mergeInvestorDate` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 160 | `$.payload.bidNoContractorResponse.bidNotification.mergerDate` | `null, datetime` | **Sparse** | 7.7% | 92.3% | 0.0% | 46 | Key | `2025-07-07T17:29:33.704` (7), `2025-07-10T17:50:03.014` (5) | Thưa thớt cao (Null 92.3%) |
| 161 | `$.payload.bidNoContractorResponse.bidNotification.mergerInvestorName` | `null, string` | **Sparse** | 7.7% | 92.3% | 0.0% | 48 | Key | `Ban Quản lý dự án đầu tư xây ...` (7), `Ban quản lý dự án đầu tư - hạ...` (5) | Thưa thớt cao (Null 92.3%) |
| 162 | `$.payload.bidNoContractorResponse.bidNotification.mergerProName` | `null, string` | **Sparse** | 7.1% | 92.9% | 0.0% | 44 | Key | `Ban Quản lý dự án đầu tư xây ...` (7), `Ban quản lý dự án đầu tư - hạ...` (5) | Thưa thớt cao (Null 92.9%) |
| 163 | `$.payload.bidNoContractorResponse.bidNotification.mixBidField` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 164 | `$.payload.bidNoContractorResponse.bidNotification.note` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 165 | `$.payload.bidNoContractorResponse.bidNotification.notifyNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `IB2200000066` (1), `IB2200000048` (1) | Không (Clean) |
| 166 | `$.payload.bidNoContractorResponse.bidNotification.notifyType` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `TBMT` (1000) | Không (Clean) |
| 167 | `$.payload.bidNoContractorResponse.bidNotification.notifyVersion` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 4 | Key | `00` (918), `01` (71) | Không (Clean) |
| 168 | `$.payload.bidNoContractorResponse.bidNotification.oldInvestorName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 169 | `$.payload.bidNoContractorResponse.bidNotification.originalPublicDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `2022-09-17T11:00:02.034` (1), `2022-09-17T10:48:23.226` (1) | Không (Clean) |
| 170 | `$.payload.bidNoContractorResponse.bidNotification.planDecisionDate` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 171 | `$.payload.bidNoContractorResponse.bidNotification.planName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 859 | Key | `Kế hoạch lựa chọn nhà thầu đợ...` (10), `Đầu tư Cải tạo và nâng cấp Nh...` (9) | Không (Clean) |
| 172 | `$.payload.bidNoContractorResponse.bidNotification.planNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 893 | Key | `PL2200005036` (10), `PL2200001719` (9) | Không (Clean) |
| 173 | `$.payload.bidNoContractorResponse.bidNotification.planType` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Key | `TX` (541), `DTPT` (362) | Không (Clean) |
| 174 | `$.payload.bidNoContractorResponse.bidNotification.preNotifyId` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 175 | `$.payload.bidNoContractorResponse.bidNotification.preNotifyNo` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 176 | `$.payload.bidNoContractorResponse.bidNotification.preNotifyVersion` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 177 | `$.payload.bidNoContractorResponse.bidNotification.processApply` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `LDT` (1000) | Không (Clean) |
| 178 | `$.payload.bidNoContractorResponse.bidNotification.procuringEntityCode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 619 | Key | `vnz000013160` (14), `vn0307114021` (12) | Không (Clean) |
| 179 | `$.payload.bidNoContractorResponse.bidNotification.procuringEntityName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 619 | Key | `Ban quản lý dự án đầu tư xây ...` (14), `Ban Quản lý dự án đầu tư xây ...` (12) | Dư khoảng trắng đầu/cuối (36 bản ghi) |
| 180 | `$.payload.bidNoContractorResponse.bidNotification.projectCode` | `null, string` | **Sparse** | 47.9% | 52.1% | 0.0% | 304 | Key | `N/A` (130), `PR2200004771` (7) | Khuyết một phần (Null 52.1%); Giá trị khuyết danh/Sentinel ('N/A') |
| 181 | `$.payload.bidNoContractorResponse.bidNotification.projectName` | `string, null` | **Full** | 99.5% | 0.5% | 0.0% | 869 | Key | `Mua sắm trang bị công binh` (10), `Đầu tư Cải tạo và nâng cấp Nh...` (9) | Dư khoảng trắng đầu/cuối (2 bản ghi) |
| 182 | `$.payload.bidNoContractorResponse.bidNotification.propertySample` | `null, int` | **Sparse** | 12.5% | 87.5% | 0.0% | 2 | Key | `0` (81), `1` (44) | Thưa thớt cao (Null 87.5%); Giá trị khuyết danh/Sentinel ('0') |
| 183 | `$.payload.bidNoContractorResponse.bidNotification.publicDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `2022-09-17T11:00:02.034` (1), `2022-09-17T10:48:23.226` (1) | Không (Clean) |
| 184 | `$.payload.bidNoContractorResponse.bidNotification.reOfferNotifyId` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 185 | `$.payload.bidNoContractorResponse.bidNotification.reOfferNotifyNo` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 186 | `$.payload.bidNoContractorResponse.bidNotification.reOfferNotifyVersion` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 187 | `$.payload.bidNoContractorResponse.bidNotification.receiveLocation` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 44 | Key | `https://muasamcong.mpi.gov.vn` (953), `Ban Công nghệ - Tổng Công ty ...` (2) | Dư khoảng trắng đầu/cuối (5 bản ghi) |
| 188 | `$.payload.bidNoContractorResponse.bidNotification.receiveLocationEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 189 | `$.payload.bidNoContractorResponse.bidNotification.shoppingAgencyCode` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 190 | `$.payload.bidNoContractorResponse.bidNotification.shortListNo` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 191 | `$.payload.bidNoContractorResponse.bidNotification.status` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `01` (878), `03` (122) | Không (Clean) |
| 192 | `$.payload.bidNoContractorResponse.bidNotification.underUnitCode` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 193 | `$.payload.bidNoContractorResponse.bidNotification.workType` | `string, null` | **Sparse** | 35.4% | 64.6% | 0.0% | 1 | Key | `KHAC` (354) | Khuyết một phần (Null 64.6%) |
| 194 | `$.payload.bidaInvChapterConfList` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 195 | `$.payload.bidoBidStatus` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 196 | `$.payload.bidoBidStatus.beforeStatus` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 9 | Key | `OPEN_BID` (651), `OPEN_DXTC` (146) | Không (Clean) |
| 197 | `$.payload.bidoBidStatus.bidCloseDate` | `null, datetime` | **Sparse** | 21.2% | 78.8% | 0.0% | 144 | Key | `2022-10-07T09:00:00` (7), `2022-10-14T09:00:00` (5) | Khuyết một phần (Null 78.8%) |
| 198 | `$.payload.bidoBidStatus.bidId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 995 | Key | `b82103dc-d049-4b36-936b-a27d5...` (2), `1b92563f-347e-409f-a644-9ca16...` (2) | Không (Clean) |
| 199 | `$.payload.bidoBidStatus.bidName` | `string, null` | **Gaps present** | 81.7% | 18.3% | 0.0% | 709 | Key | `Xây lắp` (16), `Thi công xây dựng công trình` (15) | Không (Clean) |
| 200 | `$.payload.bidoBidStatus.bidNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 994 | Key | `BP2200108663` (2), `BP2200112370` (2) | Không (Clean) |
| 201 | `$.payload.bidoBidStatus.bidOpenDate` | `datetime, null` | **Sparse** | 77.8% | 22.2% | 0.0% | 408 | Key | `2022-10-04T09:00:00` (14), `2022-10-07T09:00:00` (14) | Khuyết một phần (Null 22.2%) |
| 202 | `$.payload.bidoBidStatus.bidReOfferOpenDate` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 203 | `$.payload.bidoBidStatus.id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 995 | Key | `5054136e-2887-4da9-974f-f4f11...` (2), `489684d1-8000-4309-b612-8682e...` (2) | Không (Clean) |
| 204 | `$.payload.bidoBidStatus.isMultiLot` | `int, null` | **Gaps present** | 81.7% | 18.3% | 0.0% | 2 | Key | `0` (803), `1` (14) | Giá trị khuyết danh/Sentinel ('0') |
| 205 | `$.payload.bidoBidStatus.notifyId` | `string, null` | **Gaps present** | 80.7% | 19.3% | 0.0% | 804 | Key | `2644d788-f4b8-417f-b117-3113d...` (2), `8dd1941c-9b3c-4083-9445-e96cb...` (2) | Không (Clean) |
| 206 | `$.payload.bidoBidStatus.notifyNo` | `string, null` | **Gaps present** | 80.7% | 19.3% | 0.0% | 804 | Key | `IB2200022352` (2), `IB2200020194` (2) | Không (Clean) |
| 207 | `$.payload.bidoBidStatus.notifyType` | `string, null` | **Sparse** | 77.8% | 22.2% | 0.0% | 1 | Key | `TBMT` (778) | Khuyết một phần (Null 22.2%) |
| 208 | `$.payload.bidoBidStatus.notifyVersion` | `string, null` | **Gaps present** | 80.7% | 19.3% | 0.0% | 4 | Key | `00` (739), `01` (59) | Không (Clean) |
| 209 | `$.payload.bidoBidStatus.packType` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 210 | `$.payload.bidoBidStatus.procuringEntityCode` | `string, null` | **Gaps present** | 80.7% | 19.3% | 0.0% | 521 | Key | `vnz000013160` (14), `vn2300109040` (11) | Không (Clean) |
| 211 | `$.payload.bidoBidStatus.procuringEntityName` | `string, null` | **Sparse** | 77.8% | 22.2% | 0.0% | 502 | Key | `Ban quản lý dự án đầu tư xây ...` (14), `CÔNG TY TNHH MỘT THÀNH VIÊN K...` (11) | Khuyết một phần (Null 22.2%); Dư khoảng trắng đầu/cuối (25 bản ghi) |
| 212 | `$.payload.bidoBidStatus.published` | `int, null` | **Gaps present** | 90.1% | 9.9% | 0.0% | 1 | Key | `1` (901) | Không (Clean) |
| 213 | `$.payload.bidoBidStatus.quantity` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 214 | `$.payload.bidoBidStatus.status` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 6 | Key | `PUB_KQLCNT` (900), `CANCEL_BID` (55) | Không (Clean) |
| 215 | `$.payload.bidoBidStatus.successBidOpenDate` | `datetime, null` | **Gaps present** | 89.4% | 10.6% | 0.0% | 894 | Key | `2022-09-27T21:02:34.666` (1), `2022-09-27T11:04:09.241` (1) | Không (Clean) |
| 216 | `$.payload.bidoBidStatus.successBidOpenDateTc` | `null, datetime` | **Sparse** | 17.2% | 82.8% | 0.0% | 172 | Key | `2022-10-03T14:16:11.342` (1), `2022-10-13T09:03:35.949` (1) | Thưa thớt cao (Null 82.8%) |
| 217 | `$.payload.bidoInvBiddingDTO` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 218 | `$.payload.bidoInvFileDTO` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 219 | `$.payload.bidoInvReqApprovalAndDetailView` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 220 | `$.payload.bidoNotifyContractorM` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 221 | `$.payload.bidoNotifyContractorM.additionalChoise` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 222 | `$.payload.bidoNotifyContractorM.additionalPrice` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 223 | `$.payload.bidoNotifyContractorM.baseMedicine` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 224 | `$.payload.bidoNotifyContractorM.bidBefore` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 225 | `$.payload.bidoNotifyContractorM.bidCloseDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 450 | Key | `2022-10-07T09:00:00` (26), `2022-10-04T09:00:00` (19) | Không (Clean) |
| 226 | `$.payload.bidoNotifyContractorM.bidEstimatePrice` | `null, int` | **Sparse** | 50.4% | 49.6% | 0.0% | 491 | Key | `770200000` (2), `4200000000` (2) | Khuyết một phần (Null 49.6%) |
| 227 | `$.payload.bidoNotifyContractorM.bidForm` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Key | `DTRR` (568), `CHCT` (305) | Không (Clean) |
| 228 | `$.payload.bidoNotifyContractorM.bidGuaranteeFormEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 229 | `$.payload.bidoNotifyContractorM.bidId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 995 | Key | `b82103dc-d049-4b36-936b-a27d5...` (2), `1b92563f-347e-409f-a644-9ca16...` (2) | Không (Clean) |
| 230 | `$.payload.bidoNotifyContractorM.bidMode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `1_MTHS` (798), `1_HTHS` (202) | Không (Clean) |
| 231 | `$.payload.bidoNotifyContractorM.bidName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 862 | Key | `Xây lắp` (27), `Thi công xây dựng công trình` (19) | Không (Clean) |
| 232 | `$.payload.bidoNotifyContractorM.bidNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 994 | Key | `BP2200108663` (2), `BP2200112370` (2) | Không (Clean) |
| 233 | `$.payload.bidoNotifyContractorM.bidNoType` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 234 | `$.payload.bidoNotifyContractorM.bidOpenDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 479 | Key | `2022-10-07T09:00:00` (24), `2022-10-06T15:00:00` (18) | Không (Clean) |
| 235 | `$.payload.bidoNotifyContractorM.bidOpenLocation` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 40 | Key | `https://muasamcong.mpi.gov.vn` (953), `Trung tâm Y tế huyện Hồng Dân` (4) | Dư khoảng trắng đầu/cuối (4 bản ghi) |
| 236 | `$.payload.bidoNotifyContractorM.bidOpenLocationEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 237 | `$.payload.bidoNotifyContractorM.bidPrice` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 976 | Key | `180000000` (3), `125701000` (2) | Không (Clean) |
| 238 | `$.payload.bidoNotifyContractorM.bidPriceUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `VND` (1000) | Không (Clean) |
| 239 | `$.payload.bidoNotifyContractorM.bidValidityPeriod` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 18 | Key | `90` (336), `60` (229) | Không (Clean) |
| 240 | `$.payload.bidoNotifyContractorM.bidValidityPeriodUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `D` (1000) | Không (Clean) |
| 241 | `$.payload.bidoNotifyContractorM.capitalDetail` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 640 | Key | `Ngân sách nhà nước` (40), `Ngân sách Nhà nước` (27) | Không (Clean) |
| 242 | `$.payload.bidoNotifyContractorM.cdtType` | `int, null` | **Sparse** | 53.9% | 46.1% | 0.0% | 2 | Key | `0` (529), `1` (10) | Khuyết một phần (Null 46.1%); Giá trị khuyết danh/Sentinel ('0') |
| 243 | `$.payload.bidoNotifyContractorM.contractPeriod` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 77 | Key | `30` (129), `90` (109) | Không (Clean) |
| 244 | `$.payload.bidoNotifyContractorM.contractPeriodUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `D` (833), `M` (167) | Không (Clean) |
| 245 | `$.payload.bidoNotifyContractorM.contractType` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 5 | Key | `TG` (944), `DGCD` (45) | Không (Clean) |
| 246 | `$.payload.bidoNotifyContractorM.createdBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 650 | Key | `vn0307114021` (12), `vn2300109040` (11) | Không (Clean) |
| 247 | `$.payload.bidoNotifyContractorM.delayDTOList` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 248 | `$.payload.bidoNotifyContractorM.deliveryCapability` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 249 | `$.payload.bidoNotifyContractorM.executionLocation` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 250 | `$.payload.bidoNotifyContractorM.feeType` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `F` (965), `C` (35) | Không (Clean) |
| 251 | `$.payload.bidoNotifyContractorM.feeValue` | `null, int` | **Sparse** | 3.5% | 96.5% | 0.0% | 3 | Key | `2000000` (18), `1000000` (12) | Thưa thớt cao (Null 96.5%) |
| 252 | `$.payload.bidoNotifyContractorM.getVersionDTOS` | `array` | **Full** | 100.0% | 0.0% | 0.0% | 918 | Key | `[{'id': '2b353261-e958-4263-b...` (1), `[{'id': '4bcce44d-8451-428d-b...` (1) | Không (Clean) |
| 253 | `$.payload.bidoNotifyContractorM.getVersionDTOS[]` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 1,096 | Key | `{'id': '2b353261-e958-4263-b9...` (1), `{'id': '4bcce44d-8451-428d-bc...` (1) | Không (Clean) |
| 254 | `$.payload.bidoNotifyContractorM.getVersionDTOS[].id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,096 | Key | `2b353261-e958-4263-b9d3-4b339...` (1), `4bcce44d-8451-428d-bc4f-4ca86...` (1) | Không (Clean) |
| 255 | `$.payload.bidoNotifyContractorM.getVersionDTOS[].no` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 256 | `$.payload.bidoNotifyContractorM.getVersionDTOS[].publicBy` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 257 | `$.payload.bidoNotifyContractorM.getVersionDTOS[].publicDate` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 258 | `$.payload.bidoNotifyContractorM.getVersionDTOS[].status` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 259 | `$.payload.bidoNotifyContractorM.getVersionDTOS[].version` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 4 | Key | `00` (1000), `01` (82) | Không (Clean) |
| 260 | `$.payload.bidoNotifyContractorM.guaranteeForm` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Key | `Thư bảo lãnh` (533), `Cam kết` (434) | Không (Clean) |
| 261 | `$.payload.bidoNotifyContractorM.guaranteeValue` | `int, null` | **Sparse** | 74.9% | 25.1% | 0.0% | 267 | Key | `30000000` (29), `5000000` (29) | Khuyết một phần (Null 25.1%) |
| 262 | `$.payload.bidoNotifyContractorM.id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `2b353261-e958-4263-b9d3-4b339...` (1), `4bcce44d-8451-428d-bc4f-4ca86...` (1) | Không (Clean) |
| 263 | `$.payload.bidoNotifyContractorM.incentives` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 264 | `$.payload.bidoNotifyContractorM.investField` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 5 | Key | `HH` (378), `XL` (354) | Không (Clean) |
| 265 | `$.payload.bidoNotifyContractorM.investorCode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 637 | Key | `vnz000013160` (14), `vn0307114021` (12) | Không (Clean) |
| 266 | `$.payload.bidoNotifyContractorM.investorName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 643 | Key | `Ban quản lý dự án đầu tư xây ...` (14), `Ban Quản lý dự án đầu tư xây ...` (12) | Dư khoảng trắng đầu/cuối (30 bản ghi) |
| 267 | `$.payload.bidoNotifyContractorM.isAgreeFrame` | `null, int` | **Sparse** | 0.6% | 99.4% | 0.0% | 2 | Key | `1` (4), `0` (2) | Thưa thớt cao (Null 99.4%); Giá trị khuyết danh/Sentinel ('0') |
| 268 | `$.payload.bidoNotifyContractorM.isAttachFlowEnabled` | `bool` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `False` (1000) | Không (Clean) |
| 269 | `$.payload.bidoNotifyContractorM.isDomestic` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `1` (997), `0` (3) | Giá trị khuyết danh/Sentinel ('0') |
| 270 | `$.payload.bidoNotifyContractorM.isEarly` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 271 | `$.payload.bidoNotifyContractorM.isElig29B` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 272 | `$.payload.bidoNotifyContractorM.isGoods` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 273 | `$.payload.bidoNotifyContractorM.isInternet` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `1` (953), `0` (47) | Giá trị khuyết danh/Sentinel ('0') |
| 274 | `$.payload.bidoNotifyContractorM.isMedicine` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 275 | `$.payload.bidoNotifyContractorM.isMultiLot` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 276 | `$.payload.bidoNotifyContractorM.isPrequalification` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 277 | `$.payload.bidoNotifyContractorM.isPriorityBidForm` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 278 | `$.payload.bidoNotifyContractorM.isRented` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 279 | `$.payload.bidoNotifyContractorM.isSustainably` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 280 | `$.payload.bidoNotifyContractorM.issueLocation` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `https://muasamcong.mpi.gov.vn` (1000) | Không (Clean) |
| 281 | `$.payload.bidoNotifyContractorM.language` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 282 | `$.payload.bidoNotifyContractorM.lotDTOList` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 283 | `$.payload.bidoNotifyContractorM.medicalType` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `0` (1000) | Giá trị khuyết danh/Sentinel ('0') |
| 284 | `$.payload.bidoNotifyContractorM.mergeInvestorDate` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 285 | `$.payload.bidoNotifyContractorM.mergerDate` | `null, datetime` | **Sparse** | 7.7% | 92.3% | 0.0% | 46 | Key | `2025-07-07T17:29:33.704` (7), `2025-07-10T17:50:03.014` (5) | Thưa thớt cao (Null 92.3%) |
| 286 | `$.payload.bidoNotifyContractorM.mergerInvestorName` | `null, string` | **Sparse** | 7.7% | 92.3% | 0.0% | 48 | Key | `Ban Quản lý dự án đầu tư xây ...` (7), `Ban quản lý dự án đầu tư - hạ...` (5) | Thưa thớt cao (Null 92.3%) |
| 287 | `$.payload.bidoNotifyContractorM.mergerProName` | `null, string` | **Sparse** | 7.1% | 92.9% | 0.0% | 44 | Key | `Ban Quản lý dự án đầu tư xây ...` (7), `Ban quản lý dự án đầu tư - hạ...` (5) | Thưa thớt cao (Null 92.9%) |
| 288 | `$.payload.bidoNotifyContractorM.mixBidField` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 289 | `$.payload.bidoNotifyContractorM.note` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 290 | `$.payload.bidoNotifyContractorM.notifyNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `IB2200000066` (1), `IB2200000048` (1) | Không (Clean) |
| 291 | `$.payload.bidoNotifyContractorM.notifyType` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `TBMT` (1000) | Không (Clean) |
| 292 | `$.payload.bidoNotifyContractorM.notifyVersion` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 4 | Key | `00` (918), `01` (71) | Không (Clean) |
| 293 | `$.payload.bidoNotifyContractorM.oldInvestorName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 294 | `$.payload.bidoNotifyContractorM.originalPublicDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `2022-09-17T11:00:02.034` (1), `2022-09-17T10:48:23.226` (1) | Không (Clean) |
| 295 | `$.payload.bidoNotifyContractorM.planDecisionDate` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 296 | `$.payload.bidoNotifyContractorM.planName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 859 | Key | `Kế hoạch lựa chọn nhà thầu đợ...` (10), `Đầu tư Cải tạo và nâng cấp Nh...` (9) | Không (Clean) |
| 297 | `$.payload.bidoNotifyContractorM.planNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 893 | Key | `PL2200005036` (10), `PL2200001719` (9) | Không (Clean) |
| 298 | `$.payload.bidoNotifyContractorM.planType` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Key | `TX` (541), `DTPT` (362) | Không (Clean) |
| 299 | `$.payload.bidoNotifyContractorM.preNotifyId` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 300 | `$.payload.bidoNotifyContractorM.preNotifyNo` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 301 | `$.payload.bidoNotifyContractorM.preNotifyVersion` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 302 | `$.payload.bidoNotifyContractorM.processApply` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `LDT` (1000) | Không (Clean) |
| 303 | `$.payload.bidoNotifyContractorM.procuringEntityCode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 619 | Key | `vnz000013160` (14), `vn0307114021` (12) | Không (Clean) |
| 304 | `$.payload.bidoNotifyContractorM.procuringEntityName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 619 | Key | `Ban quản lý dự án đầu tư xây ...` (14), `Ban Quản lý dự án đầu tư xây ...` (12) | Dư khoảng trắng đầu/cuối (36 bản ghi) |
| 305 | `$.payload.bidoNotifyContractorM.projectCode` | `null, string` | **Sparse** | 47.9% | 52.1% | 0.0% | 304 | Key | `N/A` (130), `PR2200004771` (7) | Khuyết một phần (Null 52.1%); Giá trị khuyết danh/Sentinel ('N/A') |
| 306 | `$.payload.bidoNotifyContractorM.projectName` | `string, null` | **Full** | 99.5% | 0.5% | 0.0% | 869 | Key | `Mua sắm trang bị công binh` (10), `Đầu tư Cải tạo và nâng cấp Nh...` (9) | Dư khoảng trắng đầu/cuối (2 bản ghi) |
| 307 | `$.payload.bidoNotifyContractorM.propertySample` | `null, int` | **Sparse** | 12.5% | 87.5% | 0.0% | 2 | Key | `0` (81), `1` (44) | Thưa thớt cao (Null 87.5%); Giá trị khuyết danh/Sentinel ('0') |
| 308 | `$.payload.bidoNotifyContractorM.publicDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `2022-09-17T11:00:02.034` (1), `2022-09-17T10:48:23.226` (1) | Không (Clean) |
| 309 | `$.payload.bidoNotifyContractorM.reOfferNotifyId` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 310 | `$.payload.bidoNotifyContractorM.reOfferNotifyNo` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 311 | `$.payload.bidoNotifyContractorM.reOfferNotifyVersion` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 312 | `$.payload.bidoNotifyContractorM.receiveLocation` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 44 | Key | `https://muasamcong.mpi.gov.vn` (953), `Ban Công nghệ - Tổng Công ty ...` (2) | Dư khoảng trắng đầu/cuối (5 bản ghi) |
| 313 | `$.payload.bidoNotifyContractorM.receiveLocationEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 314 | `$.payload.bidoNotifyContractorM.shoppingAgencyCode` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 315 | `$.payload.bidoNotifyContractorM.shortListNo` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 316 | `$.payload.bidoNotifyContractorM.status` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `01` (878), `03` (122) | Không (Clean) |
| 317 | `$.payload.bidoNotifyContractorM.underUnitCode` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 318 | `$.payload.bidoNotifyContractorM.workType` | `string, null` | **Sparse** | 35.4% | 64.6% | 0.0% | 1 | Key | `KHAC` (354) | Khuyết một phần (Null 64.6%) |
| 319 | `$.payload.bidoPreBidConferenceList` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 320 | `$.payload.bidoPreNotifyContractorResult` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 321 | `$.payload.bidpBidLocationList` | `array` | **Sparse** | 76.8% | 0.0% | 23.2% | 0 | Key | *(Nested Array)* | Chuỗi/Mảng rỗng (23.2%) |
| 322 | `$.payload.bidpBidLocationList[]` | `object` | **Sparse** | 76.8% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 323 | `$.payload.bidpBidLocationList[].action` | `null` | **Sparse** | 0.0% | 76.8% | 0.0% | 0 | Key | *(Null / Rỗng)* | Khuyết một phần (Null 76.8%) |
| 324 | `$.payload.bidpBidLocationList[].bidDetailId` | `string` | **Sparse** | 76.8% | 0.0% | 0.0% | 764 | Key | `34ee587c-1f8e-4f89-85cd-11df0...` (6), `e76e98d3-7af0-4cac-8427-3890d...` (4) | Không (Clean) |
| 325 | `$.payload.bidpBidLocationList[].createBy` | `string` | **Sparse** | 76.8% | 0.0% | 0.0% | 387 | Key | `ADMIN` (183), `vnz000014756` (14) | Không (Clean) |
| 326 | `$.payload.bidpBidLocationList[].createDate` | `datetime` | **Sparse** | 76.8% | 0.0% | 0.0% | 809 | Key | `2022-09-22T16:46:48.291` (2), `2022-09-22T11:03:09.067` (2) | Không (Clean) |
| 327 | `$.payload.bidpBidLocationList[].districtCode` | `string, null` | **Sparse** | 47.9% | 28.9% | 0.0% | 243 | Key | `10101` (19), `22305` (11) | Khuyết một phần (Null 28.9%) |
| 328 | `$.payload.bidpBidLocationList[].districtName` | `string, null` | **Sparse** | 47.9% | 28.9% | 0.0% | 241 | Key | `Quận Ba Đình` (19), `Huyện Quế Võ` (11) | Khuyết một phần (Null 28.9%) |
| 329 | `$.payload.bidpBidLocationList[].id` | `string` | **Sparse** | 76.8% | 0.0% | 0.0% | 811 | Key | `4ac0acb5-7042-4176-8bac-92ae7...` (2), `a53380bf-d976-4294-971f-d7359...` (2) | Không (Clean) |
| 330 | `$.payload.bidpBidLocationList[].planId` | `string` | **Sparse** | 76.8% | 0.0% | 0.0% | 673 | Key | `ca721d05-e6df-4ca1-bab0-0b63e...` (10), `5461d713-229c-4be9-bdb3-aa12f...` (9) | Không (Clean) |
| 331 | `$.payload.bidpBidLocationList[].provCode` | `string` | **Sparse** | 76.8% | 0.0% | 0.0% | 61 | Key | `101` (140), `701` (44) | Không (Clean) |
| 332 | `$.payload.bidpBidLocationList[].provName` | `string` | **Sparse** | 76.8% | 0.0% | 0.0% | 65 | Key | `Thành phố Hà Nội` (140), `Tỉnh Bắc Ninh` (28) | Không (Clean) |
| 333 | `$.payload.bidpBidLocationList[].wardCode` | `string, null` | **Sparse** | 40.6% | 36.2% | 0.0% | 339 | Key | `1010105` (10), `2230517` (9) | Khuyết một phần (Null 36.2%) |
| 334 | `$.payload.bidpBidLocationList[].wardName` | `string, null` | **Sparse** | 40.6% | 36.2% | 0.0% | 332 | Key | `Phường Vĩnh Phúc` (10), `Xã Ngọc Xá` (9) | Khuyết một phần (Null 36.2%) |
| 335 | `$.payload.bidpPlanDetail` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 336 | `$.payload.bidpPlanDetail.additionalChoise` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 337 | `$.payload.bidpPlanDetail.additionalChoiseValue` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 338 | `$.payload.bidpPlanDetail.bidEstimatePrice` | `null, int` | **Sparse** | 0.1% | 99.9% | 0.0% | 1 | Key | `165131278` (1) | Null 100% (Trường rỗng/không sử dụng) |
| 339 | `$.payload.bidpPlanDetail.bidField` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 5 | Key | `HH` (378), `XL` (354) | Không (Clean) |
| 340 | `$.payload.bidpPlanDetail.bidForm` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Key | `DTRR` (568), `CHCT` (305) | Không (Clean) |
| 341 | `$.payload.bidpPlanDetail.bidMode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `1_MTHS` (798), `1_HTHS` (202) | Không (Clean) |
| 342 | `$.payload.bidpPlanDetail.bidName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 862 | Key | `Xây lắp` (27), `Thi công xây dựng công trình` (19) | Không (Clean) |
| 343 | `$.payload.bidpPlanDetail.bidNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 994 | Key | `BP2200108663` (2), `BP2200112370` (2) | Không (Clean) |
| 344 | `$.payload.bidpPlanDetail.bidPrice` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 976 | Key | `180000000` (3), `125701000` (2) | Không (Clean) |
| 345 | `$.payload.bidpPlanDetail.bidPriceDetail` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 977 | Key | `[{"invest":180000000,"exchang...` (3), `[{"invest":125701000,"exchang...` (2) | Không (Clean) |
| 346 | `$.payload.bidpPlanDetail.bidPriceEx` | `null, int` | **Sparse** | 23.2% | 76.8% | 0.0% | 229 | Key | `4200000000` (2), `481200000` (2) | Khuyết một phần (Null 76.8%) |
| 347 | `$.payload.bidpPlanDetail.bidPriceUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `VND` (1000) | Không (Clean) |
| 348 | `$.payload.bidpPlanDetail.bidStartMonth` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 7 | Key | `0` (717), `9` (264) | Giá trị khuyết danh/Sentinel ('0') |
| 349 | `$.payload.bidpPlanDetail.bidStartQuarter` | `string, null` | **Mostly full** | 96.4% | 3.6% | 0.0% | 5 | Key | `III` (628), `0` (247) | Giá trị khuyết danh/Sentinel ('0') |
| 350 | `$.payload.bidpPlanDetail.bidStartUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `Q` (717), `M` (283) | Không (Clean) |
| 351 | `$.payload.bidpPlanDetail.bidStartYear` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 4 | Key | `2022` (991), `2021` (5) | Không (Clean) |
| 352 | `$.payload.bidpPlanDetail.bidTime` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 353 | `$.payload.bidpPlanDetail.capMngtUnit` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 354 | `$.payload.bidpPlanDetail.capitalDetail` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 640 | Key | `Ngân sách nhà nước` (40), `Ngân sách Nhà nước` (27) | Không (Clean) |
| 355 | `$.payload.bidpPlanDetail.cperiod` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 77 | Key | `30` (129), `90` (109) | Không (Clean) |
| 356 | `$.payload.bidpPlanDetail.cperiodUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `D` (833), `M` (167) | Không (Clean) |
| 357 | `$.payload.bidpPlanDetail.ctype` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 5 | Key | `TG` (944), `DGCD` (44) | Không (Clean) |
| 358 | `$.payload.bidpPlanDetail.exchangeDate` | `null, datetime` | **Sparse** | 22.9% | 77.1% | 0.0% | 195 | Key | `2022-09-20T00:00:00.000+0000` (17), `2022-09-17T00:00:00.000+0000` (9) | Khuyết một phần (Null 77.1%) |
| 359 | `$.payload.bidpPlanDetail.generalTasks` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 360 | `$.payload.bidpPlanDetail.id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 995 | Key | `b82103dc-d049-4b36-936b-a27d5...` (2), `1b92563f-347e-409f-a644-9ca16...` (2) | Không (Clean) |
| 361 | `$.payload.bidpPlanDetail.investorName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 362 | `$.payload.bidpPlanDetail.isConcentrateShopping` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `0` (994), `1` (6) | Giá trị khuyết danh/Sentinel ('0') |
| 363 | `$.payload.bidpPlanDetail.isDomestic` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `1` (997), `0` (3) | Giá trị khuyết danh/Sentinel ('0') |
| 364 | `$.payload.bidpPlanDetail.isInternet` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `1` (953), `0` (47) | Giá trị khuyết danh/Sentinel ('0') |
| 365 | `$.payload.bidpPlanDetail.isMultiLot` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `0` (983), `1` (17) | Giá trị khuyết danh/Sentinel ('0') |
| 366 | `$.payload.bidpPlanDetail.isOnlineShopping` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `0` (1000) | Giá trị khuyết danh/Sentinel ('0') |
| 367 | `$.payload.bidpPlanDetail.isPrequalification` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `0` (1000) | Giá trị khuyết danh/Sentinel ('0') |
| 368 | `$.payload.bidpPlanDetail.latestAssignId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 995 | Key | `9e4cd8c5-7013-4d7c-8b85-f69cf...` (2), `b5f05694-78d6-4e85-89d1-69c25...` (2) | Không (Clean) |
| 369 | `$.payload.bidpPlanDetail.linkNotifyInfo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 995 | Key | `{"notifyNo":"IB2200022352","n...` (2), `{"notifyNo":"IB2200002218","n...` (2) | Không (Clean) |
| 370 | `$.payload.bidpPlanDetail.planDecisionDate` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 371 | `$.payload.bidpPlanDetail.planId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 896 | Key | `ca721d05-e6df-4ca1-bab0-0b63e...` (10), `5461d713-229c-4be9-bdb3-aa12f...` (9) | Không (Clean) |
| 372 | `$.payload.bidpPlanDetail.planNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 893 | Key | `PL2200005036` (10), `PL2200001719` (9) | Không (Clean) |
| 373 | `$.payload.bidpPlanDetail.planVersion` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 5 | Key | `00` (889), `01` (87) | Không (Clean) |
| 374 | `$.payload.bidpPlanDetail.processApply` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `LDT` (1000) | Không (Clean) |
| 375 | `$.payload.bidpPlanDetail.projectsLinked` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 376 | `$.payload.bidpPlanDetail.recCapital` | `null, int` | **Sparse** | 22.9% | 77.1% | 0.0% | 2 | Key | `0` (228), `8138718000` (1) | Khuyết một phần (Null 77.1%); Giá trị khuyết danh/Sentinel ('0') |
| 377 | `$.payload.bidpPlanDetail.recCapitalUnit` | `string, null` | **Sparse** | 77.1% | 22.9% | 0.0% | 1 | Key | `VND` (771) | Khuyết một phần (Null 22.9%) |
| 378 | `$.payload.bidpPlanDetail.reportToCode` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 379 | `$.payload.bidpPlanDetail.reportToName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 380 | `$.payload.bidpPlanDetail.shoppingAgencyCode` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 381 | `$.payload.bidpPlanDetail.supervisor` | `null, string` | **Sparse** | 17.4% | 82.6% | 0.0% | 132 | Key | `Cục KH&ĐT/BQP` (9), `Không` (7) | Thưa thớt cao (Null 82.6%); Giá trị khuyết danh/Sentinel ('Không') |
| 382 | `$.payload.bidpPlanDetail.underUnitCode` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 383 | `$.payload.biduClarifyReqInvAndContentViewList` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 384 | `$.payload.biduPetitionContractorDTOList` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 385 | `$.payload.cancelBidHistoryList` | `array` | **Sparse** | 0.1% | 0.0% | 99.9% | 0 | Key | *(Nested Array)* | Chuỗi/Mảng rỗng (99.9%) |
| 386 | `$.payload.cancelBidHistoryList[]` | `object` | **Sparse** | 0.1% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 387 | `$.payload.cancelBidHistoryList[].bidId` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `mi_dd0cc291-4565-4444-8e7f-51...` (1) | Không (Clean) |
| 388 | `$.payload.cancelBidHistoryList[].bidName` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `Hệ thống xử lý nước thải giai...` (1) | Không (Clean) |
| 389 | `$.payload.cancelBidHistoryList[].bidNo` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `BP2200096337` (1) | Không (Clean) |
| 390 | `$.payload.cancelBidHistoryList[].cancelBy` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `vn3800378251` (1) | Không (Clean) |
| 391 | `$.payload.cancelBidHistoryList[].cancelDate` | `datetime` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `2024-06-28T08:53:39` (1) | Không (Clean) |
| 392 | `$.payload.cancelBidHistoryList[].cancelDecisionAgency` | `null` | **Sparse** | 0.0% | 0.1% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 393 | `$.payload.cancelBidHistoryList[].cancelDecisionDate` | `datetime` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `2023-04-13T00:00:00` (1) | Không (Clean) |
| 394 | `$.payload.cancelBidHistoryList[].cancelDecisionNo` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `08/QĐ-KCNCSBL` (1) | Không (Clean) |
| 395 | `$.payload.cancelBidHistoryList[].cancelFileAttachId` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `23d7e2a7-7af4-4080-a294-5f7a6...` (1) | Không (Clean) |
| 396 | `$.payload.cancelBidHistoryList[].cancelFileAttachName` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `qd 08-04122023105344.pdf` (1) | Không (Clean) |
| 397 | `$.payload.cancelBidHistoryList[].cancelNumber` | `int` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `1` (1) | Không (Clean) |
| 398 | `$.payload.cancelBidHistoryList[].cancelReason` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `SPTDT` (1) | Không (Clean) |
| 399 | `$.payload.cancelBidHistoryList[].cancelType` | `int` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `1` (1) | Không (Clean) |
| 400 | `$.payload.cancelBidHistoryList[].createdBy` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `vn3800378251` (1) | Không (Clean) |
| 401 | `$.payload.cancelBidHistoryList[].createdDate` | `datetime` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `2024-06-28T08:55:34.388` (1) | Không (Clean) |
| 402 | `$.payload.cancelBidHistoryList[].id` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `4df0ec08-4023-41df-bf7f-87080...` (1) | Không (Clean) |
| 403 | `$.payload.cancelBidHistoryList[].notifyId` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `2c067b40-3275-4427-94d8-8b591...` (1) | Không (Clean) |
| 404 | `$.payload.cancelBidHistoryList[].notifyNo` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `IB2200001311` (1) | Không (Clean) |
| 405 | `$.payload.cancelBidHistoryList[].notifyVersion` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `00` (1) | Không (Clean) |
| 406 | `$.payload.cancelBidHistoryList[].processApply` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `LDT` (1) | Không (Clean) |
| 407 | `$.payload.cancelBidHistoryList[].recoveryDate` | `null` | **Sparse** | 0.0% | 0.1% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 408 | `$.payload.cancelBidHistoryList[].recoveryDecisionFileId` | `null` | **Sparse** | 0.0% | 0.1% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 409 | `$.payload.cancelBidHistoryList[].recoveryDecisionFileName` | `null` | **Sparse** | 0.0% | 0.1% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 410 | `$.payload.cancelBidHistoryList[].recoveryDocumentaryFileId` | `null` | **Sparse** | 0.0% | 0.1% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 411 | `$.payload.cancelBidHistoryList[].recoveryDocumentaryFileName` | `null` | **Sparse** | 0.0% | 0.1% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 412 | `$.payload.cancelBidHistoryList[].recoveryImageFileId` | `null` | **Sparse** | 0.0% | 0.1% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 413 | `$.payload.cancelBidHistoryList[].recoveryImageFileName` | `null` | **Sparse** | 0.0% | 0.1% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 414 | `$.payload.cancelBidHistoryList[].recoveryNote` | `null` | **Sparse** | 0.0% | 0.1% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 415 | `$.payload.cancelBidHistoryList[].reportFileId` | `null` | **Sparse** | 0.0% | 0.1% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 416 | `$.payload.cancelBidHistoryList[].reportFileName` | `null` | **Sparse** | 0.0% | 0.1% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 417 | `$.payload.cancelBidHistoryList[].status` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `03` (1) | Không (Clean) |
| 418 | `$.payload.cancelBidHistoryList[].updatedBy` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `vn3800378251` (1) | Không (Clean) |
| 419 | `$.payload.cancelBidHistoryList[].updatedDate` | `datetime` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `2024-06-28T08:55:34.388` (1) | Không (Clean) |
| 420 | `$.payload.statusDT` | `null, string` | **Sparse** | 5.4% | 94.6% | 0.0% | 1 | Attribute | `KCNTTDT` (54) | Thưa thớt cao (Null 94.6%) |
| 421 | `$.run_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 8 | Key | `0bd8c25942f440c88efe6bea9af77e20` (237), `bdba97bda5844fafaf1adc42bc07e99a` (187) | Không (Clean) |
| 422 | `$.source_date` | `date` | **Full** | 100.0% | 0.0% | 0.0% | 12 | Timestamp | `2022-09-28` (237), `2022-09-26` (187) | Không (Clean) |
| 423 | `$.source_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `IB2200000066` (1), `IB2200000048` (1) | Không (Clean) |
| 424 | `$.source_version` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 4 | Measure | `00` (918), `01` (71) | Không (Clean) |

#### 2.4.2. Tổng Hợp Các Vấn Đề Chất Lượng Dữ Liệu Cụ Thể Trên Bảng `notify_contractor_standard_detail`

| Nhóm Vấn Đề Chất Lượng (Issue Category) | Số Trường Bị Ảnh Hưởng | Chi Tiết Trường & Bằng Chứng Số Liệu | Hướng Xử Lý Khi Sang Tầng Silver |
| :--- | :---: | :--- | :--- |
| **1. Trường rỗng hoàn toàn (`100% Null`)** | 121 | `$.payload.bidCancelingResponse.appraisalFileId`, `$.payload.bidCancelingResponse.appraisalFileName`, `$.payload.bidCancelingResponse.cancelDecisionAgency`, `$.payload.bidCancelingResponse.reportFileId`, `$.payload.bidCancelingResponse.reportFileName`, `$.payload.bidContractorShortlistMDTO`, `$.payload.bidInvContractorOfflineDTO.briefFileId`, `$.payload.bidInvContractorOfflineDTO.briefFileIdEn`, `$.payload.bidInvContractorOfflineDTO.briefFileName`, `$.payload.bidInvContractorOfflineDTO.briefFileNameEn`, `$.payload.bidInvContractorOfflineDTO.decisionAgencyEn`, `$.payload.bidInvContractorOfflineDTO.decisionFileIdEn`, `$.payload.bidInvContractorOfflineDTO.decisionFileNameEn`, `$.payload.bidInvContractorOfflineDTO.decisionNoEn`, `$.payload.bidInvContractorOfflineDTO.fileIdEn` *(và 106 trường khác)* | Không đưa vào các cột nghiệp vụ chính của Silver; vẫn bảo toàn trong `root_json` / `raw_json`.
| **2. Trường khuyết/thưa thớt (`20% ≤ Null < 100%`)** | 53 | `$.payload.bidCancelingResponse.cancelBy` (87.8%), `$.payload.bidCancelingResponse.cancelDate` (87.8%), `$.payload.bidCancelingResponse.cancelDecisionDate` (87.8%), `$.payload.bidCancelingResponse.cancelDecisionNo` (87.8%), `$.payload.bidCancelingResponse.cancelFileAttachId` (87.8%), `$.payload.bidCancelingResponse.cancelFileAttachName` (87.8%), `$.payload.bidCancelingResponse.cancelReason` (87.8%), `$.payload.bidCancelingResponse.cancelType` (87.8%), `$.payload.bidInvContractorOfflineDTO.fileId` (95.3%), `$.payload.bidInvContractorOfflineDTO.fileName` (95.3%), `$.payload.bidInvContractorOfflineDTO.listOtherFile` (95.3%), `$.payload.bidInvContractorOfflineDTO.otherFileId` (99.3%) *(và 41 trường khác)* | Cho phép `nullable=True` trên schema Silver; thiết lập chuỗi fallback đa tầng (VD: `isMultiLot`, `public_date`, `currency`).
| **3. Chuỗi rỗng hoặc Mảng rỗng (`Empty > 0%`)** | 2 | `$.payload.bidpBidLocationList` (23.2%), `$.payload.cancelBidHistoryList` (99.9%) | Hàm `clean_text()` tự động chuẩn hóa chuỗi rỗng `""` hoặc chỉ chứa khoảng trắng về `NULL`; mảng rỗng `[]` không sinh dòng bảng con.
| **4. Đa kiểu dữ liệu (`Mixed Types`)** | 1 | `$.payload.bidNoContractorResponse.bidNotification.lotDTOList[].lotGuaranteeValue` (int/float) | Ép kiểu tường minh qua bộ parser (`parse_money`, `parse_boolean`, `parse_date`, `parse_iso_datetime`); ghi nhận `quality_issue` nếu sai định dạng.

**Danh sách chi tiết toàn bộ 39 cảnh báo dị thường (Anomalies) trên các trường có dữ liệu:**

| STT | Trường Dữ Liệu (`Field Path`) | Loại Bất Thường Phát Hiện (`Detected Anomalies`) | Số Bản Ghi Ảnh Hưởng (Trên Mẫu) | Quy Tắc Làm Sạch Tại Silver |
| :---: | :--- | :--- | ---: | :--- |
| 1 | `$.payload.bidoNotifyContractorM.projectName` | `whitespace_padding` | `5` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 2 | `$.payload.bidoNotifyContractorM.procuringEntityName` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 3 | `$.payload.bidoNotifyContractorM.investorName` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 4 | `$.payload.bidoNotifyContractorM.isDomestic` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 5 | `$.payload.bidoNotifyContractorM.bidOpenLocation` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 6 | `$.payload.bidoNotifyContractorM.receiveLocation` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 7 | `$.payload.bidoNotifyContractorM.projectCode` | `sentinel_value_detected: 'N/A'` | `521` | Lọc các chuỗi khuyết danh (`N/A`, `NULL`, `NONE`, `-`) về `NULL` |
| 8 | `$.payload.bidoNotifyContractorM.isInternet` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 9 | `$.payload.bidoNotifyContractorM.isAgreeFrame` | `sentinel_value_detected: '0'` | `994` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 10 | `$.payload.bidoNotifyContractorM.propertySample` | `sentinel_value_detected: '0'` | `875` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 11 | `$.payload.bidoNotifyContractorM.cdtType` | `sentinel_value_detected: '0'` | `461` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 12 | `$.payload.bidoNotifyContractorM.medicalType` | `sentinel_value_detected: '0'` | `1,000` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 13 | `$.payload.bidoBidStatus.procuringEntityName` | `whitespace_padding` | `222` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 14 | `$.payload.bidoBidStatus.isMultiLot` | `sentinel_value_detected: '0'` | `183` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 15 | `$.payload.bidpPlanDetail.isDomestic` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 16 | `$.payload.bidpPlanDetail.isInternet` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 17 | `$.payload.bidpPlanDetail.isPrequalification` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 18 | `$.payload.bidpPlanDetail.recCapital` | `sentinel_value_detected: '0'` | `771` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 19 | `$.payload.bidpPlanDetail.bidStartMonth` | `sentinel_value_detected: '0'` | `1,000` | Giá trị sentinel `0` khi tính theo Quý -> chuyển thành `NULL` qua `_month()` / `_positive_year()` |
| 20 | `$.payload.bidpPlanDetail.bidStartQuarter` | `sentinel_value_detected: '0'` | `36` | Giá trị sentinel `0` khi tính theo Quý -> chuyển thành `NULL` qua `_month()` / `_positive_year()` |
| 21 | `$.payload.bidpPlanDetail.isMultiLot` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 22 | `$.payload.bidpPlanDetail.isConcentrateShopping` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 23 | `$.payload.bidpPlanDetail.isOnlineShopping` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 24 | `$.payload.bidpPlanDetail.supervisor` | `sentinel_value_detected: 'Không'` | `826` | Lọc các chuỗi khuyết danh (`N/A`, `NULL`, `NONE`, `-`) về `NULL` |
| 25 | `$.payload.bidCancelingResponse.procuringEntityName` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 26 | `$.payload.bidCancelingResponse.cancelType` | `sentinel_value_detected: '0'` | `878` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 27 | `$.payload.bidNoContractorResponse.bidNotification.projectName` | `whitespace_padding` | `5` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 28 | `$.payload.bidNoContractorResponse.bidNotification.procuringEntityName` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 29 | `$.payload.bidNoContractorResponse.bidNotification.investorName` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 30 | `$.payload.bidNoContractorResponse.bidNotification.isDomestic` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 31 | `$.payload.bidNoContractorResponse.bidNotification.bidOpenLocation` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 32 | `$.payload.bidNoContractorResponse.bidNotification.receiveLocation` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 33 | `$.payload.bidNoContractorResponse.bidNotification.projectCode` | `sentinel_value_detected: 'N/A'` | `521` | Lọc các chuỗi khuyết danh (`N/A`, `NULL`, `NONE`, `-`) về `NULL` |
| 34 | `$.payload.bidNoContractorResponse.bidNotification.isInternet` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 35 | `$.payload.bidNoContractorResponse.bidNotification.isAgreeFrame` | `sentinel_value_detected: '0'` | `994` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 36 | `$.payload.bidNoContractorResponse.bidNotification.propertySample` | `sentinel_value_detected: '0'` | `875` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 37 | `$.payload.bidNoContractorResponse.bidNotification.cdtType` | `sentinel_value_detected: '0'` | `461` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 38 | `$.payload.bidNoContractorResponse.bidNotification.medicalType` | `sentinel_value_detected: '0'` | `1,000` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 39 | `$.payload.bidNoContractorResponse.bidNotification.delayDTOList[].reason` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |

---

### 2.5. Bảng `notify_contractor_vk_adb_detail` (Resource: `notify_contractor`)

- **Tổng bản ghi hoạt động (Active Rows):** `14,067` bản ghi (`976` files Parquet)
- **Số bản ghi mẫu phân tích sâu (Sampled Records):** `1,000` bản ghi
- **Khoảng thời gian mẫu khảo sát:** `2022-09-22` → `2023-02-10`
- **Tổng số trường phát hiện (Total Flattened Paths):** `252` trường
- **Khóa chính ứng viên đơn (Single Candidate Keys):** `$.source_id`, `$.content_hash`, `$.payload.bidoNotifyContractorP.id`, `$.payload.bidoNotifyContractorP.notifyNo`, `$.payload.bidoNotifyContractorP.publicDate`, `$.payload.bidoNotifyContractorP.createdDate`, `$.payload.bidoNotifyContractorP.lastModifiedDate`, `$.payload.bidoNotifyContractorP.getVersionDTOS[].no`, `$.payload.bidoNotifyContractorP.lotDTOList[].notifyNo`, `$.payload.bidoNotifyContractorP.lotDTOList[].notifyId`, `$.payload.bidoNotifyContractorP.originalPublicDate`, `$.payload.bidInvContractorOfflineDTO.id`, `$.payload.bidInvContractorOfflineDTO.notifyId`, `$.payload.bidInvContractorOfflineDTO.notifyNo`, `$.payload.bidInvContractorOfflineDTO.fileId`, `$.payload.bidInvContractorOfflineDTO.decisionFileId`, `$.payload.bidInvContractorOfflineDTO.createdDate`, `$._dlt_id`
- **Khóa ghép ứng viên (Composite Candidate Keys):** `$.source_id` + `$.source_version`

#### 2.5.1. Bảng Thống Kê Đầy Đủ 252 Trường Dữ Liệu Của `notify_contractor_vk_adb_detail`

| STT | Đường dẫn trường (`Path`) | Kiểu dữ liệu (`Types`) | Độ đầy đủ (`Tier`) | Populated % | Null % | Empty % | Distinct | Vai trò (`Role`) | Giá trị mẫu (`Top Samples`) | Vấn đề cụ thể phát hiện (`Specific Issues`) |
| :---: | :--- | :--- | :---: | ---: | ---: | ---: | ---: | :---: | :--- | :--- |
| 1 | `$` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Nested | *(Nested Object)* | Không (Clean) |
| 2 | `$._dlt_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `/FsvmsIlPY6K2g` (1), `8DIBduFEkfme8A` (1) | Không (Clean) |
| 3 | `$._dlt_load_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 110 | Key | `1790962954.2934449` (26), `1790963097.6083999` (22) | Không (Clean) |
| 4 | `$.content_hash` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Free-text | `63cc953b6fac8b759c2d45c2295a7...` (1), `7a588e1d72376000648688191beb0...` (1) | Không (Clean) |
| 5 | `$.ingested_at` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 972 | Attribute | `2026-10-02 17:40:06.279031+00:00` (2), `2026-10-02 17:40:06.338872+00:00` (2) | Không (Clean) |
| 6 | `$.payload` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Nested | *(Nested Object)* | Không (Clean) |
| 7 | `$.payload.bidInvContractorOfflineDTO` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 8 | `$.payload.bidInvContractorOfflineDTO.briefFileId` | `null, string` | **Sparse** | 6.3% | 93.7% | 0.0% | 63 | Key | `d62f565a-e14f-4ea6-8eca-fca1e...` (1), `a7b89fd4-b0fb-4eef-b689-ced4c...` (1) | Thưa thớt cao (Null 93.7%) |
| 9 | `$.payload.bidInvContractorOfflineDTO.briefFileIdEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 10 | `$.payload.bidInvContractorOfflineDTO.briefFileName` | `null, string` | **Sparse** | 6.3% | 93.7% | 0.0% | 59 | Key | `Nội dung cập nhật.docx` (3), `Bổ sung bản vẽ.docx` (2) | Thưa thớt cao (Null 93.7%) |
| 11 | `$.payload.bidInvContractorOfflineDTO.briefFileNameEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 12 | `$.payload.bidInvContractorOfflineDTO.createdBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 207 | Key | `vn0301179079` (65), `vn3500102414-06` (49) | Không (Clean) |
| 13 | `$.payload.bidInvContractorOfflineDTO.createdDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `2022-09-22T19:22:33.62` (1), `2022-09-23T17:38:30.341` (1) | Không (Clean) |
| 14 | `$.payload.bidInvContractorOfflineDTO.decisionAgency` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 370 | Key | `Eximbank` (64), `Vietsovpetro` (41) | Giá trị khuyết danh/Sentinel ('N/A') |
| 15 | `$.payload.bidInvContractorOfflineDTO.decisionAgencyEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 16 | `$.payload.bidInvContractorOfflineDTO.decisionDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 926 | Key | `2022-11-07T17:00:00.000+0000` (9), `2022-10-17T17:00:00.000+0000` (5) | Không (Clean) |
| 17 | `$.payload.bidInvContractorOfflineDTO.decisionFileId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `cd3c7475-42b3-4f22-b59e-04464...` (1), `75594744-9242-4293-b2a6-d0fcd...` (1) | Không (Clean) |
| 18 | `$.payload.bidInvContractorOfflineDTO.decisionFileIdEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 19 | `$.payload.bidInvContractorOfflineDTO.decisionFileName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 894 | Key | `BM 5. To trinh phe duyet HSYC...` (23), `HSMT.pdf` (12) | Không (Clean) |
| 20 | `$.payload.bidInvContractorOfflineDTO.decisionFileNameEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 21 | `$.payload.bidInvContractorOfflineDTO.decisionNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 946 | Key | `08/11/2022` (5), `03/NQ-BQT` (5) | Không (Clean) |
| 22 | `$.payload.bidInvContractorOfflineDTO.decisionNoEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 23 | `$.payload.bidInvContractorOfflineDTO.fileId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `5eb6ab06-9284-45c8-aaf6-1f99d...` (1), `5a5ae514-31b6-45a1-9b77-d5323...` (1) | Không (Clean) |
| 24 | `$.payload.bidInvContractorOfflineDTO.fileIdEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 25 | `$.payload.bidInvContractorOfflineDTO.fileName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 880 | Key | `HSMT.pdf` (35), `HSYC.pdf` (11) | Không (Clean) |
| 26 | `$.payload.bidInvContractorOfflineDTO.fileNameEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 27 | `$.payload.bidInvContractorOfflineDTO.id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `ffde51bd-57a3-4faa-9e49-dc184...` (1), `1ca762ee-f867-40a5-ac82-ea88e...` (1) | Không (Clean) |
| 28 | `$.payload.bidInvContractorOfflineDTO.listOtherFile` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 29 | `$.payload.bidInvContractorOfflineDTO.listOtherFileEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 30 | `$.payload.bidInvContractorOfflineDTO.notifyId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `ffde51bd-57a3-4faa-9e49-dc184...` (1), `1ca762ee-f867-40a5-ac82-ea88e...` (1) | Không (Clean) |
| 31 | `$.payload.bidInvContractorOfflineDTO.notifyNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `IB2200001176` (1), `IB2200001592` (1) | Không (Clean) |
| 32 | `$.payload.bidInvContractorOfflineDTO.notifyVersion` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 5 | Key | `00` (926), `01` (56) | Không (Clean) |
| 33 | `$.payload.bidInvContractorOfflineDTO.otherFileId` | `null, string` | **Sparse** | 24.4% | 75.6% | 0.0% | 244 | Key | `5480dad2-833f-4d4f-be61-deb2d...` (1), `865e6f74-b5df-46f2-9979-f543d...` (1) | Khuyết một phần (Null 75.6%) |
| 34 | `$.payload.bidInvContractorOfflineDTO.otherFileIdEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 35 | `$.payload.bidInvContractorOfflineDTO.otherFileName` | `null, string` | **Sparse** | 24.7% | 75.3% | 0.0% | 225 | Key | `BM 6. Thong bao moi tham gia ...` (9), `KHDT.pdf` (5) | Khuyết một phần (Null 75.3%) |
| 36 | `$.payload.bidInvContractorOfflineDTO.otherFileNameEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 37 | `$.payload.bidInvContractorOfflineDTO.updatedBy` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 38 | `$.payload.bidInvContractorOfflineDTO.updatedDate` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 39 | `$.payload.bidSubmissionByContractorViewResponse` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 40 | `$.payload.bidaInvChapterConfList` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 41 | `$.payload.bidoBidStatusDTO` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 42 | `$.payload.bidoBidStatusDTO.beforeStatus` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 7 | Key | `INIT_MT` (430), `PUB_MT` (340) | Không (Clean) |
| 43 | `$.payload.bidoBidStatusDTO.bidCloseDate` | `null, datetime` | **Gaps present** | 91.2% | 8.8% | 0.0% | 443 | Key | `2022-10-17T09:00:00` (15), `2022-10-21T09:00:00` (10) | Không (Clean) |
| 44 | `$.payload.bidoBidStatusDTO.bidId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 996 | Key | `86ed8a6b-a392-4407-bad4-cc286...` (2), `822b7843-40cc-4edd-92c2-8a961...` (2) | Không (Clean) |
| 45 | `$.payload.bidoBidStatusDTO.bidName` | `string, null` | **Full** | 99.9% | 0.1% | 0.0% | 980 | Key | `Plant greening in 2022` (2), `Sơn tường và vệ sinh kính mặt...` (2) | Không (Clean) |
| 46 | `$.payload.bidoBidStatusDTO.bidNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 993 | Key | `BP2200161492` (2), `BP2200189861` (2) | Không (Clean) |
| 47 | `$.payload.bidoBidStatusDTO.bidOpenDate` | `datetime, null` | **Gaps present** | 94.4% | 5.6% | 0.0% | 459 | Key | `2022-10-17T09:00:00` (15), `2022-10-21T09:00:00` (11) | Không (Clean) |
| 48 | `$.payload.bidoBidStatusDTO.bidReOfferOpenDate` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 49 | `$.payload.bidoBidStatusDTO.id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 996 | Key | `8c5834fc-7bb8-4402-9ad4-d078b...` (2), `9cec7b45-43e2-4fd3-9cef-29f30...` (2) | Không (Clean) |
| 50 | `$.payload.bidoBidStatusDTO.isMultiLot` | `int, null` | **Full** | 99.9% | 0.1% | 0.0% | 2 | Key | `0` (966), `1` (33) | Giá trị khuyết danh/Sentinel ('0') |
| 51 | `$.payload.bidoBidStatusDTO.notifyId` | `string, null` | **Mostly full** | 95.3% | 4.7% | 0.0% | 949 | Key | `5c4eac80-91c1-4de0-98cb-02aa3...` (2), `cd548375-a1c9-4805-8f20-7247a...` (2) | Không (Clean) |
| 52 | `$.payload.bidoBidStatusDTO.notifyNo` | `string, null` | **Mostly full** | 95.3% | 4.7% | 0.0% | 949 | Key | `IB2200028699` (2), `IB2200082353` (2) | Không (Clean) |
| 53 | `$.payload.bidoBidStatusDTO.notifyType` | `string, null` | **Gaps present** | 94.4% | 5.6% | 0.0% | 1 | Key | `TBMT` (944) | Không (Clean) |
| 54 | `$.payload.bidoBidStatusDTO.notifyVersion` | `string, null` | **Mostly full** | 95.3% | 4.7% | 0.0% | 5 | Key | `00` (885), `01` (53) | Không (Clean) |
| 55 | `$.payload.bidoBidStatusDTO.packType` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 56 | `$.payload.bidoBidStatusDTO.procuringEntityCode` | `string, null` | **Mostly full** | 95.3% | 4.7% | 0.0% | 143 | Key | `vn3500102414` (273), `vn4300378569` (68) | Không (Clean) |
| 57 | `$.payload.bidoBidStatusDTO.procuringEntityName` | `string, null` | **Mostly full** | 95.3% | 4.7% | 0.0% | 144 | Key | `Liên doanh Việt-Nga Vietsovpetro` (270), `Công ty Cổ phần Lọc Hóa dầu B...` (68) | Dư khoảng trắng đầu/cuối (30 bản ghi) |
| 58 | `$.payload.bidoBidStatusDTO.published` | `int, null` | **Sparse** | 50.5% | 49.5% | 0.0% | 1 | Key | `1` (505) | Khuyết một phần (Null 49.5%) |
| 59 | `$.payload.bidoBidStatusDTO.quantity` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 60 | `$.payload.bidoBidStatusDTO.status` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 5 | Key | `PUB_KQLCNT` (505), `PUB_MT` (465) | Không (Clean) |
| 61 | `$.payload.bidoBidStatusDTO.successBidOpenDate` | `null, datetime` | **Sparse** | 22.1% | 77.9% | 0.0% | 219 | Key | `2022-11-04T14:07:38` (2), `2022-12-12T09:00:53` (2) | Khuyết một phần (Null 77.9%) |
| 62 | `$.payload.bidoBidStatusDTO.successBidOpenDateTc` | `null, datetime` | **Sparse** | 1.0% | 99.0% | 0.0% | 10 | Key | `2022-10-24T16:43:30` (1), `2022-11-29T10:32:19` (1) | Thưa thớt cao (Null 99.0%) |
| 63 | `$.payload.bidoBidroundAdbWbMngViewDTO` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 64 | `$.payload.bidoBidroundOutMngViewDTO` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 65 | `$.payload.bidoInvBiddingDTO` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 66 | `$.payload.bidoInvFileDTO` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 67 | `$.payload.bidoNotifyContractorP` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 68 | `$.payload.bidoNotifyContractorP.appraisalFileId` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 69 | `$.payload.bidoNotifyContractorP.appraisalFileName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 70 | `$.payload.bidoNotifyContractorP.bidBefore` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 71 | `$.payload.bidoNotifyContractorP.bidCloseDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 484 | Key | `2022-10-21T09:00:00` (14), `2022-10-17T09:00:00` (13) | Không (Clean) |
| 72 | `$.payload.bidoNotifyContractorP.bidEstimatePrice` | `int, null` | **Sparse** | 68.5% | 31.5% | 0.0% | 670 | Key | `1370000000` (3), `1000000000` (2) | Khuyết một phần (Null 31.5%) |
| 73 | `$.payload.bidoNotifyContractorP.bidForm` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Key | `DTRR` (560), `CHCT` (277) | Không (Clean) |
| 74 | `$.payload.bidoNotifyContractorP.bidGuaranteeForm` | `null, string` | **Sparse** | 33.2% | 66.8% | 0.0% | 176 | Key | `Thư bảo lãnh` (28), `Bảo lãnh ngân hàng` (17) | Khuyết một phần (Null 66.8%); Giá trị khuyết danh/Sentinel ('0'); Giá trị khuyết danh/Sentinel ('Không'); Giá trị khuyết danh/Sentinel ('không'); Dư khoảng trắng đầu/cuối (28 bản ghi) |
| 75 | `$.payload.bidoNotifyContractorP.bidGuaranteeFormEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 76 | `$.payload.bidoNotifyContractorP.bidGuaranteeValue` | `int, null` | **Gaps present** | 82.3% | 17.7% | 0.0% | 336 | Key | `0` (81), `100000000` (25) | Giá trị khuyết danh/Sentinel ('0') |
| 77 | `$.payload.bidoNotifyContractorP.bidId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 996 | Key | `86ed8a6b-a392-4407-bad4-cc286...` (2), `822b7843-40cc-4edd-92c2-8a961...` (2) | Không (Clean) |
| 78 | `$.payload.bidoNotifyContractorP.bidMode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Key | `1_MTHS` (893), `1_HTHS` (105) | Không (Clean) |
| 79 | `$.payload.bidoNotifyContractorP.bidName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 979 | Key | `Plant greening in 2022` (2), `Sơn tường và vệ sinh kính mặt...` (2) | Không (Clean) |
| 80 | `$.payload.bidoNotifyContractorP.bidNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 992 | Key | `BP2200161492` (2), `BP2200189861` (2) | Không (Clean) |
| 81 | `$.payload.bidoNotifyContractorP.bidNoType` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 82 | `$.payload.bidoNotifyContractorP.bidOpenDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 490 | Key | `2022-10-21T09:00:00` (14), `2022-10-17T09:00:00` (13) | Không (Clean) |
| 83 | `$.payload.bidoNotifyContractorP.bidOpenLocation` | `string, null` | **Gaps present** | 92.5% | 7.5% | 0.0% | 331 | Key | `website: https://muasamcong.m...` (235), `[{"provName":"Tỉnh Bà Rịa - V...` (122) | Dư khoảng trắng đầu/cuối (58 bản ghi) |
| 84 | `$.payload.bidoNotifyContractorP.bidOpenLocationEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 85 | `$.payload.bidoNotifyContractorP.bidPrice` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 968 | Key | `1370000000` (4), `1150457770` (3) | Không (Clean) |
| 86 | `$.payload.bidoNotifyContractorP.bidPriceUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `VND` (1000) | Không (Clean) |
| 87 | `$.payload.bidoNotifyContractorP.bidValidityPeriod` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 19 | Key | `90` (347), `60` (215) | Không (Clean) |
| 88 | `$.payload.bidoNotifyContractorP.bidValidityPeriodUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `D` (1000) | Không (Clean) |
| 89 | `$.payload.bidoNotifyContractorP.cPeriod` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 97 | Key | `12` (115), `30` (82) | Không (Clean) |
| 90 | `$.payload.bidoNotifyContractorP.cPeriodUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `D` (581), `M` (419) | Không (Clean) |
| 91 | `$.payload.bidoNotifyContractorP.cType` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 9 | Key | `TG` (766), `DGCD` (187) | Không (Clean) |
| 92 | `$.payload.bidoNotifyContractorP.cancelBy` | `null, string` | **Sparse** | 4.0% | 96.0% | 0.0% | 38 | Key | `vnz000019713` (3), `vnz000013951` (1) | Thưa thớt cao (Null 96.0%) |
| 93 | `$.payload.bidoNotifyContractorP.cancelDate` | `null, datetime` | **Sparse** | 4.0% | 96.0% | 0.0% | 40 | Key | `2022-10-13T11:24:43` (1), `2022-09-26T13:55:43` (1) | Thưa thớt cao (Null 96.0%) |
| 94 | `$.payload.bidoNotifyContractorP.cancelDecisionAgency` | `null, string` | **Sparse** | 1.7% | 98.3% | 0.0% | 17 | Key | `VietinBank Vĩnh Phúc` (1), `Công ty cổ phần cao su Lai Châu` (1) | Thưa thớt cao (Null 98.3%) |
| 95 | `$.payload.bidoNotifyContractorP.cancelDecisionDate` | `null, datetime` | **Sparse** | 4.0% | 96.0% | 0.0% | 35 | Key | `2023-03-09T00:00:00` (3), `2022-11-21T00:00:00` (2) | Thưa thớt cao (Null 96.0%) |
| 96 | `$.payload.bidoNotifyContractorP.cancelDecisionNo` | `null, string` | **Sparse** | 4.0% | 96.0% | 0.0% | 40 | Key | `281/QĐ-CNVP-TCHC` (1), `518/QĐ-CSLC` (1) | Thưa thớt cao (Null 96.0%); Dư khoảng trắng đầu/cuối (1 bản ghi) |
| 97 | `$.payload.bidoNotifyContractorP.cancelFileAttachId` | `null, string` | **Sparse** | 4.0% | 96.0% | 0.0% | 40 | Key | `0c9c2f6f-8aa0-4ed1-8bd2-4c821...` (1), `9296d318-e675-4c88-a1b3-584fb...` (1) | Thưa thớt cao (Null 96.0%) |
| 98 | `$.payload.bidoNotifyContractorP.cancelFileAttachName` | `null, string` | **Sparse** | 4.0% | 96.0% | 0.0% | 40 | Key | `281-QĐ-CNVP-TCHC.pdf` (1), `qd huy thau QD518-2022.pdf` (1) | Thưa thớt cao (Null 96.0%) |
| 99 | `$.payload.bidoNotifyContractorP.cancelReason` | `null, string` | **Sparse** | 4.0% | 96.0% | 0.0% | 10 | Key | `TTQDPL` (21), `SPTDT` (8) | Thưa thớt cao (Null 96.0%) |
| 100 | `$.payload.bidoNotifyContractorP.cancelType` | `null, int` | **Sparse** | 4.0% | 96.0% | 0.0% | 2 | Key | `1` (29), `0` (11) | Thưa thớt cao (Null 96.0%); Giá trị khuyết danh/Sentinel ('0') |
| 101 | `$.payload.bidoNotifyContractorP.capitalDetail` | `string, null` | **Gaps present** | 82.9% | 17.1% | 0.0% | 334 | Key | `Lô 09-1` (38), `Chi phí sản xuất kinh doanh` (28) | Không (Clean) |
| 102 | `$.payload.bidoNotifyContractorP.checkCloseDate` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `0` (1000) | Giá trị khuyết danh/Sentinel ('0') |
| 103 | `$.payload.bidoNotifyContractorP.createdBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 207 | Key | `vn0301179079` (65), `vn3500102414-06` (49) | Không (Clean) |
| 104 | `$.payload.bidoNotifyContractorP.createdDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `2022-09-22T19:21:50.914` (1), `2022-09-23T17:37:55.154` (1) | Không (Clean) |
| 105 | `$.payload.bidoNotifyContractorP.delayDTOList` | `null, array` | **Sparse** | 13.7% | 86.3% | 0.0% | 0 | Key | *(Nested Array)* | Thưa thớt cao (Null 86.3%) |
| 106 | `$.payload.bidoNotifyContractorP.delayDTOList[]` | `object` | **Sparse** | 13.7% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 107 | `$.payload.bidoNotifyContractorP.delayDTOList[].bidCloseDate` | `datetime` | **Sparse** | 13.7% | 0.0% | 0.0% | 125 | Key | `2022-10-26T09:00:00` (3), `2022-11-15T09:00:00` (3) | Không (Clean) |
| 108 | `$.payload.bidoNotifyContractorP.delayDTOList[].bidCloseDelayDate` | `datetime` | **Sparse** | 13.7% | 0.0% | 0.0% | 121 | Key | `2022-11-03T09:00:00` (3), `2022-11-08T09:00:00` (3) | Không (Clean) |
| 109 | `$.payload.bidoNotifyContractorP.delayDTOList[].bidName` | `string` | **Sparse** | 13.7% | 0.0% | 0.0% | 137 | Key | `Cung cấp dịch vụ vận chuyển x...` (3), `Xây dựng hạ tầng kỹ thuật gia...` (3) | Không (Clean) |
| 110 | `$.payload.bidoNotifyContractorP.delayDTOList[].bidNo` | `string` | **Sparse** | 13.7% | 0.0% | 0.0% | 137 | Key | `BP2200191088` (3), `BP2200257066` (3) | Không (Clean) |
| 111 | `$.payload.bidoNotifyContractorP.delayDTOList[].bidOpenDate` | `datetime` | **Sparse** | 13.7% | 0.0% | 0.0% | 125 | Key | `2022-10-26T09:00:00` (3), `2022-11-15T09:00:00` (3) | Không (Clean) |
| 112 | `$.payload.bidoNotifyContractorP.delayDTOList[].bidOpenDelayDate` | `datetime` | **Sparse** | 13.7% | 0.0% | 0.0% | 121 | Key | `2022-11-03T09:00:00` (3), `2022-11-08T09:00:00` (3) | Không (Clean) |
| 113 | `$.payload.bidoNotifyContractorP.delayDTOList[].createdBy` | `string` | **Sparse** | 13.7% | 0.0% | 0.0% | 52 | Key | `vn4300378569-01` (13), `vn3500102414-07` (12) | Không (Clean) |
| 114 | `$.payload.bidoNotifyContractorP.delayDTOList[].createdDate` | `datetime` | **Sparse** | 13.7% | 0.0% | 0.0% | 151 | Key | `2022-10-27T20:17:59.026` (1), `2022-10-17T13:54:58.674` (1) | Không (Clean) |
| 115 | `$.payload.bidoNotifyContractorP.delayDTOList[].id` | `string` | **Sparse** | 13.7% | 0.0% | 0.0% | 151 | Key | `9c2d7b2a-640b-4f62-a78a-1a4fb...` (1), `a7c13a6e-d273-4891-9183-6f66b...` (1) | Không (Clean) |
| 116 | `$.payload.bidoNotifyContractorP.delayDTOList[].notifyList` | `null` | **Sparse** | 0.0% | 13.7% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 117 | `$.payload.bidoNotifyContractorP.delayDTOList[].notifyNo` | `string` | **Sparse** | 13.7% | 0.0% | 0.0% | 137 | Key | `IB2200030900` (3), `IB2200057811` (3) | Không (Clean) |
| 118 | `$.payload.bidoNotifyContractorP.delayDTOList[].notifyType` | `null` | **Sparse** | 0.0% | 13.7% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 119 | `$.payload.bidoNotifyContractorP.delayDTOList[].notifyVersion` | `string` | **Sparse** | 13.7% | 0.0% | 0.0% | 3 | Key | `00` (141), `01` (9) | Không (Clean) |
| 120 | `$.payload.bidoNotifyContractorP.delayDTOList[].reason` | `string` | **Sparse** | 13.7% | 0.0% | 0.0% | 121 | Key | `Nhằm tăng tính cạnh tranh` (7), `Không đủ 03 Nhà thầu tham dự` (5) | Dư khoảng trắng đầu/cuối (5 bản ghi) |
| 121 | `$.payload.bidoNotifyContractorP.delayDTOList[].sendEmailInfos` | `null` | **Sparse** | 0.0% | 13.7% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 122 | `$.payload.bidoNotifyContractorP.executionLocation` | `string, null` | **Sparse** | 2.2% | 97.8% | 0.0% | 8 | Key | `[{"provName":"Tỉnh Bà Rịa - V...` (10), `[{"provName":"Tỉnh Bình Thuận"}]` (5) | Thưa thớt cao (Null 97.8%) |
| 123 | `$.payload.bidoNotifyContractorP.feeType` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `F` (590), `C` (410) | Không (Clean) |
| 124 | `$.payload.bidoNotifyContractorP.feeValue` | `null, int` | **Sparse** | 42.3% | 57.7% | 0.0% | 23 | Key | `500000` (181), `1000000` (76) | Khuyết một phần (Null 57.7%); Giá trị khuyết danh/Sentinel ('0') |
| 125 | `$.payload.bidoNotifyContractorP.getVersionDTOS` | `array` | **Full** | 100.0% | 0.0% | 0.0% | 926 | Key | `[{'id': 'ffde51bd-57a3-4faa-9...` (1), `[{'id': '1ca762ee-f867-40a5-a...` (1) | Không (Clean) |
| 126 | `$.payload.bidoNotifyContractorP.getVersionDTOS[]` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 1,098 | Key | `{'id': 'ffde51bd-57a3-4faa-9e...` (1), `{'id': '1ca762ee-f867-40a5-ac...` (1) | Không (Clean) |
| 127 | `$.payload.bidoNotifyContractorP.getVersionDTOS[].id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,098 | Key | `ffde51bd-57a3-4faa-9e49-dc184...` (1), `1ca762ee-f867-40a5-ac82-ea88e...` (1) | Không (Clean) |
| 128 | `$.payload.bidoNotifyContractorP.getVersionDTOS[].no` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `IB2200029513` (5), `IB2200054389` (5) | Không (Clean) |
| 129 | `$.payload.bidoNotifyContractorP.getVersionDTOS[].publicBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 207 | Key | `vn0301179079` (67), `vn3500102414-06` (53) | Không (Clean) |
| 130 | `$.payload.bidoNotifyContractorP.getVersionDTOS[].publicDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,098 | Key | `2022-09-22T20:43:36.227` (1), `2022-09-23T17:39:31.991` (1) | Không (Clean) |
| 131 | `$.payload.bidoNotifyContractorP.getVersionDTOS[].status` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `01` (1058), `03` (40) | Không (Clean) |
| 132 | `$.payload.bidoNotifyContractorP.getVersionDTOS[].version` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 5 | Key | `00` (1000), `01` (74) | Không (Clean) |
| 133 | `$.payload.bidoNotifyContractorP.id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `ffde51bd-57a3-4faa-9e49-dc184...` (1), `1ca762ee-f867-40a5-ac82-ea88e...` (1) | Không (Clean) |
| 134 | `$.payload.bidoNotifyContractorP.investField` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 5 | Key | `HH` (581), `PTV` (292) | Không (Clean) |
| 135 | `$.payload.bidoNotifyContractorP.investorCode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 159 | Key | `vn3500102414` (277), `vn4300378569` (70) | Không (Clean) |
| 136 | `$.payload.bidoNotifyContractorP.investorName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 163 | Key | `Liên doanh Việt-Nga Vietsovpetro` (274), `Công ty Cổ phần Lọc Hóa dầu B...` (70) | Dư khoảng trắng đầu/cuối (29 bản ghi) |
| 137 | `$.payload.bidoNotifyContractorP.isDomestic` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `1` (842), `0` (158) | Giá trị khuyết danh/Sentinel ('0') |
| 138 | `$.payload.bidoNotifyContractorP.isInternet` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `0` (765), `1` (235) | Giá trị khuyết danh/Sentinel ('0') |
| 139 | `$.payload.bidoNotifyContractorP.isMultiLot` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 140 | `$.payload.bidoNotifyContractorP.isTemporaryCalc` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `0` (1000) | Giá trị khuyết danh/Sentinel ('0') |
| 141 | `$.payload.bidoNotifyContractorP.isWorkday` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `0` (1000) | Giá trị khuyết danh/Sentinel ('0') |
| 142 | `$.payload.bidoNotifyContractorP.issueLocation` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 211 | Key | `https://muasamcong.mpi.gov.vn` (482), `website: https://muasamcong.m...` (236) | Dư khoảng trắng đầu/cuối (11 bản ghi) |
| 143 | `$.payload.bidoNotifyContractorP.lastModifiedBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 207 | Key | `vn0301179079` (65), `vn3500102414-06` (49) | Không (Clean) |
| 144 | `$.payload.bidoNotifyContractorP.lastModifiedDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `2022-09-22T20:43:36.228` (1), `2022-09-23T17:39:31.991` (1) | Không (Clean) |
| 145 | `$.payload.bidoNotifyContractorP.lotDTOList` | `array` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key | *(Nested Array)* | Không (Clean) |
| 146 | `$.payload.bidoNotifyContractorP.lotDTOList[]` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 147 | `$.payload.bidoNotifyContractorP.lotDTOList[].actions` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 148 | `$.payload.bidoNotifyContractorP.lotDTOList[].bidNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 992 | Key | `BP2200340967` (28), `BP2200341299` (27) | Không (Clean) |
| 149 | `$.payload.bidoNotifyContractorP.lotDTOList[].cperiod` | `null, int` | **Sparse** | 3.5% | 96.5% | 0.0% | 18 | Key | `180` (90), `5` (35) | Thưa thớt cao (Null 96.5%) |
| 150 | `$.payload.bidoNotifyContractorP.lotDTOList[].cperiodUnit` | `null, string` | **Sparse** | 3.6% | 96.4% | 0.0% | 2 | Key | `D` (127), `M` (65) | Thưa thớt cao (Null 96.4%) |
| 151 | `$.payload.bidoNotifyContractorP.lotDTOList[].id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,156 | Key | `d99614f3-f3e3-457a-9388-1cb0c...` (1), `433171ef-ea34-45b3-be5b-046db...` (1) | Không (Clean) |
| 152 | `$.payload.bidoNotifyContractorP.lotDTOList[].lotEstimatePrice` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1,107 | Key | `1370000000` (3), `4039000000` (3) | Không (Clean) |
| 153 | `$.payload.bidoNotifyContractorP.lotDTOList[].lotGuaranteeValue` | `null, int` | **Sparse** | 3.5% | 96.5% | 0.0% | 144 | Key | `6000000` (5), `10000000` (4) | Thưa thớt cao (Null 96.5%) |
| 154 | `$.payload.bidoNotifyContractorP.lotDTOList[].lotName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,123 | Key | `Nhóm 1` (5), `Nhóm 2` (5) | Không (Clean) |
| 155 | `$.payload.bidoNotifyContractorP.lotDTOList[].lotNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,148 | Key | `BP2200161492` (2), `BP2200189861` (2) | Không (Clean) |
| 156 | `$.payload.bidoNotifyContractorP.lotDTOList[].lotPrice` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1,106 | Key | `1370000000` (4), `4039000000` (3) | Không (Clean) |
| 157 | `$.payload.bidoNotifyContractorP.lotDTOList[].notifyId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `a5d42f96-7869-4a77-8846-9ae3f...` (28), `47255492-553d-44bb-b8b0-582ce...` (27) | Không (Clean) |
| 158 | `$.payload.bidoNotifyContractorP.lotDTOList[].notifyNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `IB2200097455` (28), `IB2200097471` (27) | Không (Clean) |
| 159 | `$.payload.bidoNotifyContractorP.lotDTOList[].planNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 925 | Key | `PL2200081275` (28), `PL2200081451` (27) | Không (Clean) |
| 160 | `$.payload.bidoNotifyContractorP.notifyNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `IB2200001176` (1), `IB2200001592` (1) | Không (Clean) |
| 161 | `$.payload.bidoNotifyContractorP.notifyType` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `TBMT` (1000) | Không (Clean) |
| 162 | `$.payload.bidoNotifyContractorP.notifyVersion` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 5 | Key | `00` (926), `01` (56) | Không (Clean) |
| 163 | `$.payload.bidoNotifyContractorP.originalPublicDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `2022-09-22T20:43:36.227` (1), `2022-09-23T17:39:31.991` (1) | Không (Clean) |
| 164 | `$.payload.bidoNotifyContractorP.pName` | `string, null` | **Full** | 99.5% | 0.5% | 0.0% | 837 | Key | `Mua sắm thường xuyên` (21), `Kế hoạch mua sắm vật tư d...` (12) | Không (Clean) |
| 165 | `$.payload.bidoNotifyContractorP.planDecisionDate` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 166 | `$.payload.bidoNotifyContractorP.planName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 912 | Key | `Kế hoạch lựa chọn nhà th...` (12), `Mua sắm quà tặng Eximbank lần...` (7) | Không (Clean) |
| 167 | `$.payload.bidoNotifyContractorP.planNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 925 | Key | `PL2200084510` (12), `PL2200049674` (7) | Không (Clean) |
| 168 | `$.payload.bidoNotifyContractorP.planType` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Key | `TX` (669), `KHAC` (274) | Không (Clean) |
| 169 | `$.payload.bidoNotifyContractorP.processApply` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `KHAC` (1000) | Không (Clean) |
| 170 | `$.payload.bidoNotifyContractorP.procuringEntityCode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 160 | Key | `vn3500102414` (277), `vn4300378569` (70) | Không (Clean) |
| 171 | `$.payload.bidoNotifyContractorP.procuringEntityName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 161 | Key | `Liên doanh Việt-Nga Vietsovpetro` (274), `Công ty Cổ phần Lọc Hóa dầu B...` (70) | Dư khoảng trắng đầu/cuối (30 bản ghi) |
| 172 | `$.payload.bidoNotifyContractorP.publicDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `2022-09-22T20:43:36.227` (1), `2022-09-23T17:39:31.991` (1) | Không (Clean) |
| 173 | `$.payload.bidoNotifyContractorP.receiveLocation` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 498 | Key | `website: https://muasamcong.m...` (235), `Chi nhánh Phía Nam - Công ty ...` (20) | Dư khoảng trắng đầu/cuối (76 bản ghi) |
| 174 | `$.payload.bidoNotifyContractorP.receiveLocationEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 175 | `$.payload.bidoNotifyContractorP.reportFileId` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 176 | `$.payload.bidoNotifyContractorP.reportFileName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 177 | `$.payload.bidoNotifyContractorP.status` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `01` (960), `03` (40) | Không (Clean) |
| 178 | `$.payload.bidoPreBidConferenceVersionDTOList` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 179 | `$.payload.bidoPreBidConferences` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 180 | `$.payload.bidpPlanDetail` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 181 | `$.payload.bidpPlanDetail.additionalChoise` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 182 | `$.payload.bidpPlanDetail.additionalChoiseValue` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 183 | `$.payload.bidpPlanDetail.bidEstimatePrice` | `null, int` | **Sparse** | 1.8% | 98.2% | 0.0% | 18 | Key | `33322947718` (1), `102941933394` (1) | Thưa thớt cao (Null 98.2%) |
| 184 | `$.payload.bidpPlanDetail.bidField` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 5 | Key | `HH` (581), `PTV` (292) | Không (Clean) |
| 185 | `$.payload.bidpPlanDetail.bidForm` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Key | `DTRR` (560), `CHCT` (277) | Không (Clean) |
| 186 | `$.payload.bidpPlanDetail.bidMode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Key | `1_MTHS` (893), `1_HTHS` (105) | Không (Clean) |
| 187 | `$.payload.bidpPlanDetail.bidName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 980 | Key | `Plant greening in 2022` (2), `Sơn tường và vệ sinh kính mặt...` (2) | Không (Clean) |
| 188 | `$.payload.bidpPlanDetail.bidNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 993 | Key | `BP2200161492` (2), `BP2200189861` (2) | Không (Clean) |
| 189 | `$.payload.bidpPlanDetail.bidPrice` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 969 | Key | `1370000000` (4), `1150457770` (3) | Không (Clean) |
| 190 | `$.payload.bidpPlanDetail.bidPriceDetail` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 969 | Key | `[{"invest":1370000000,"exchan...` (4), `[{"invest":1150457770,"exchan...` (3) | Không (Clean) |
| 191 | `$.payload.bidpPlanDetail.bidPriceEx` | `int, null` | **Sparse** | 8.5% | 91.5% | 0.0% | 84 | Key | `0` (2), `2493504000` (1) | Thưa thớt cao (Null 91.5%); Giá trị khuyết danh/Sentinel ('0') |
| 192 | `$.payload.bidpPlanDetail.bidPriceUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `VND` (1000) | Không (Clean) |
| 193 | `$.payload.bidpPlanDetail.bidStartMonth` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 7 | Key | `0` (385), `10` (190) | Giá trị khuyết danh/Sentinel ('0') |
| 194 | `$.payload.bidpPlanDetail.bidStartQuarter` | `string, null` | **Full** | 99.9% | 0.1% | 0.0% | 5 | Key | `0` (614), `IV` (313) | Giá trị khuyết danh/Sentinel ('0') |
| 195 | `$.payload.bidpPlanDetail.bidStartUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `M` (615), `Q` (385) | Không (Clean) |
| 196 | `$.payload.bidpPlanDetail.bidStartYear` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `2022` (873), `2023` (127) | Không (Clean) |
| 197 | `$.payload.bidpPlanDetail.bidTime` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 198 | `$.payload.bidpPlanDetail.capMngtUnit` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 199 | `$.payload.bidpPlanDetail.capitalDetail` | `string, null` | **Gaps present** | 82.9% | 17.1% | 0.0% | 334 | Key | `Lô 09-1` (38), `Chi phí sản xuất kinh doanh` (28) | Không (Clean) |
| 200 | `$.payload.bidpPlanDetail.cperiod` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 97 | Key | `12` (115), `30` (82) | Không (Clean) |
| 201 | `$.payload.bidpPlanDetail.cperiodUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `D` (581), `M` (419) | Không (Clean) |
| 202 | `$.payload.bidpPlanDetail.ctype` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 9 | Key | `TG` (766), `DGCD` (187) | Không (Clean) |
| 203 | `$.payload.bidpPlanDetail.exchangeDate` | `null, datetime` | **Sparse** | 1.2% | 98.8% | 0.0% | 4 | Key | `2022-09-20T00:00:00.000+0000` (7), `2022-09-17T00:00:00.000+0000` (3) | Thưa thớt cao (Null 98.8%) |
| 204 | `$.payload.bidpPlanDetail.generalTasks` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 205 | `$.payload.bidpPlanDetail.id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 996 | Key | `86ed8a6b-a392-4407-bad4-cc286...` (2), `822b7843-40cc-4edd-92c2-8a961...` (2) | Không (Clean) |
| 206 | `$.payload.bidpPlanDetail.investorName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 207 | `$.payload.bidpPlanDetail.isConcentrateShopping` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `0` (992), `1` (8) | Giá trị khuyết danh/Sentinel ('0') |
| 208 | `$.payload.bidpPlanDetail.isDomestic` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `1` (842), `0` (158) | Giá trị khuyết danh/Sentinel ('0') |
| 209 | `$.payload.bidpPlanDetail.isInternet` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `0` (765), `1` (235) | Giá trị khuyết danh/Sentinel ('0') |
| 210 | `$.payload.bidpPlanDetail.isMultiLot` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `0` (965), `1` (35) | Giá trị khuyết danh/Sentinel ('0') |
| 211 | `$.payload.bidpPlanDetail.isOnlineShopping` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `0` (1000) | Giá trị khuyết danh/Sentinel ('0') |
| 212 | `$.payload.bidpPlanDetail.isPrequalification` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `0` (1000) | Giá trị khuyết danh/Sentinel ('0') |
| 213 | `$.payload.bidpPlanDetail.latestAssignId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 996 | Key | `ff2b81f0-0a19-437f-b3a2-1aa7d...` (2), `139ec3f3-77a2-4bb2-9609-092f0...` (2) | Không (Clean) |
| 214 | `$.payload.bidpPlanDetail.linkNotifyInfo` | `null, string` | **Sparse** | 43.9% | 56.1% | 0.0% | 436 | Key | `{"notifyNo":"IB2200082353","n...` (2), `{"notifyNo":"IB2200107734","n...` (2) | Khuyết một phần (Null 56.1%) |
| 215 | `$.payload.bidpPlanDetail.planDecisionDate` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 216 | `$.payload.bidpPlanDetail.planId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 932 | Key | `14de67de-1f7f-4a29-a113-f50cf...` (12), `67615a7a-2c05-4bfa-9cc4-977fc...` (7) | Không (Clean) |
| 217 | `$.payload.bidpPlanDetail.planNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 925 | Key | `PL2200084510` (12), `PL2200049674` (7) | Không (Clean) |
| 218 | `$.payload.bidpPlanDetail.planVersion` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 6 | Key | `00` (891), `01` (73) | Không (Clean) |
| 219 | `$.payload.bidpPlanDetail.processApply` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `KHAC` (1000) | Không (Clean) |
| 220 | `$.payload.bidpPlanDetail.projectsLinked` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 221 | `$.payload.bidpPlanDetail.recCapital` | `null, int` | **Sparse** | 2.7% | 97.3% | 0.0% | 13 | Key | `0` (15), `308438741` (1) | Thưa thớt cao (Null 97.3%); Giá trị khuyết danh/Sentinel ('0') |
| 222 | `$.payload.bidpPlanDetail.recCapitalUnit` | `string, null` | **Mostly full** | 98.8% | 1.2% | 0.0% | 1 | Key | `VND` (988) | Không (Clean) |
| 223 | `$.payload.bidpPlanDetail.reportToCode` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 224 | `$.payload.bidpPlanDetail.reportToName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 225 | `$.payload.bidpPlanDetail.shoppingAgencyCode` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 226 | `$.payload.bidpPlanDetail.supervisor` | `string, null` | **Sparse** | 25.0% | 75.0% | 0.0% | 118 | Key | `Công ty TNHH Điện lực Vĩnh Tân 1` (31), `Công Ty TNHH MTV Tổng Công T...` (17) | Khuyết một phần (Null 75.0%); Giá trị khuyết danh/Sentinel ('Không'); Giá trị khuyết danh/Sentinel ('không') |
| 227 | `$.payload.bidpPlanDetail.underUnitCode` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 228 | `$.payload.biduClarifyReqInvAndContentViewVersionDTOList` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 229 | `$.payload.biduClarifyReqInvAndContentViews` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 230 | `$.payload.biduPetitionContractorDTOList` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 231 | `$.payload.biduPetitionContractorVersionDTOList` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 232 | `$.payload.cancelBidHistoryList` | `array` | **Sparse** | 0.0% | 0.0% | 100.0% | 0 | Key | *(Nested Array)* | Chuỗi/Mảng rỗng (100.0%) |
| 233 | `$.payload.dtoList` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 234 | `$.payload.lsBidpBidLocationDTO` | `array` | **Sparse** | 78.5% | 0.0% | 21.5% | 0 | Key | *(Nested Array)* | Chuỗi/Mảng rỗng (21.5%) |
| 235 | `$.payload.lsBidpBidLocationDTO[]` | `object` | **Sparse** | 78.5% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 236 | `$.payload.lsBidpBidLocationDTO[].action` | `null` | **Sparse** | 0.0% | 78.5% | 0.0% | 0 | Key | *(Null / Rỗng)* | Khuyết một phần (Null 78.5%) |
| 237 | `$.payload.lsBidpBidLocationDTO[].bidDetailId` | `string` | **Sparse** | 78.5% | 0.0% | 0.0% | 782 | Key | `64fbe95e-eec0-4567-875a-fbd69...` (29), `6b2e97a0-fe97-4e5f-97b3-92f65...` (13) | Không (Clean) |
| 238 | `$.payload.lsBidpBidLocationDTO[].createBy` | `string` | **Sparse** | 78.5% | 0.0% | 0.0% | 169 | Key | `vn0301179079` (63), `vnz000022213` (60) | Không (Clean) |
| 239 | `$.payload.lsBidpBidLocationDTO[].createDate` | `datetime` | **Sparse** | 78.5% | 0.0% | 0.0% | 907 | Key | `2022-09-23T14:17:12.874` (2), `2022-09-20T09:39:04.833` (2) | Không (Clean) |
| 240 | `$.payload.lsBidpBidLocationDTO[].districtCode` | `string, null` | **Sparse** | 71.7% | 6.8% | 0.0% | 96 | Key | `71701` (250), `50505` (71) | Không (Clean) |
| 241 | `$.payload.lsBidpBidLocationDTO[].districtName` | `string, null` | **Sparse** | 71.7% | 6.8% | 0.0% | 96 | Key | `Thành phố Vũng Tàu` (250), `Huyện Bình Sơn` (71) | Không (Clean) |
| 242 | `$.payload.lsBidpBidLocationDTO[].id` | `string` | **Sparse** | 78.5% | 0.0% | 0.0% | 930 | Key | `905748c0-7133-44ad-bb1f-c713f...` (2), `2cd4fd11-bb76-4259-aaa2-a9ba8...` (2) | Không (Clean) |
| 243 | `$.payload.lsBidpBidLocationDTO[].planId` | `string` | **Sparse** | 78.5% | 0.0% | 0.0% | 734 | Key | `3cca5cce-55e1-46ea-ac2c-f27b4...` (29), `ef45fd8c-2839-49f4-a043-eb169...` (13) | Không (Clean) |
| 244 | `$.payload.lsBidpBidLocationDTO[].provCode` | `string` | **Sparse** | 78.5% | 0.0% | 0.0% | 61 | Key | `717` (316), `701` (101) | Không (Clean) |
| 245 | `$.payload.lsBidpBidLocationDTO[].provName` | `string` | **Sparse** | 78.5% | 0.0% | 0.0% | 62 | Key | `Tỉnh Bà Rịa - Vũng Tàu` (316), `TP. Hồ Chí Minh` (100) | Không (Clean) |
| 246 | `$.payload.lsBidpBidLocationDTO[].wardCode` | `string, null` | **Sparse** | 44.3% | 34.2% | 0.0% | 86 | Key | `7170119` (100), `7170127` (44) | Khuyết một phần (Null 34.2%) |
| 247 | `$.payload.lsBidpBidLocationDTO[].wardName` | `string, null` | **Sparse** | 44.3% | 34.2% | 0.0% | 84 | Key | `Phường Thắng Nhì` (100), `Phường Thắng Nhất` (44) | Khuyết một phần (Null 34.2%) |
| 248 | `$.payload.statusDT` | `null, string` | **Sparse** | 1.3% | 98.7% | 0.0% | 1 | Attribute | `KCNTTDT` (13) | Thưa thớt cao (Null 98.7%) |
| 249 | `$.run_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 110 | Key | `55a079dfe58343a5aa0caeb5172ee37f` (26), `1f27fb12fb9145e2a838e6c260007d67` (22) | Không (Clean) |
| 250 | `$.source_date` | `date` | **Full** | 100.0% | 0.0% | 0.0% | 110 | Timestamp | `2022-10-11` (26), `2022-10-26` (22) | Không (Clean) |
| 251 | `$.source_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `IB2200001176` (1), `IB2200001592` (1) | Không (Clean) |
| 252 | `$.source_version` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 5 | Measure | `00` (926), `01` (56) | Không (Clean) |

#### 2.5.2. Tổng Hợp Các Vấn Đề Chất Lượng Dữ Liệu Cụ Thể Trên Bảng `notify_contractor_vk_adb_detail`

| Nhóm Vấn Đề Chất Lượng (Issue Category) | Số Trường Bị Ảnh Hưởng | Chi Tiết Trường & Bằng Chứng Số Liệu | Hướng Xử Lý Khi Sang Tầng Silver |
| :--- | :---: | :--- | :--- |
| **1. Trường rỗng hoàn toàn (`100% Null`)** | 54 | `$.payload.bidInvContractorOfflineDTO.briefFileIdEn`, `$.payload.bidInvContractorOfflineDTO.briefFileNameEn`, `$.payload.bidInvContractorOfflineDTO.decisionAgencyEn`, `$.payload.bidInvContractorOfflineDTO.decisionFileIdEn`, `$.payload.bidInvContractorOfflineDTO.decisionFileNameEn`, `$.payload.bidInvContractorOfflineDTO.decisionNoEn`, `$.payload.bidInvContractorOfflineDTO.fileIdEn`, `$.payload.bidInvContractorOfflineDTO.fileNameEn`, `$.payload.bidInvContractorOfflineDTO.listOtherFile`, `$.payload.bidInvContractorOfflineDTO.listOtherFileEn`, `$.payload.bidInvContractorOfflineDTO.otherFileIdEn`, `$.payload.bidInvContractorOfflineDTO.otherFileNameEn`, `$.payload.bidInvContractorOfflineDTO.updatedBy`, `$.payload.bidInvContractorOfflineDTO.updatedDate`, `$.payload.bidSubmissionByContractorViewResponse` *(và 39 trường khác)* | Không đưa vào các cột nghiệp vụ chính của Silver; vẫn bảo toàn trong `root_json` / `raw_json`.
| **2. Trường khuyết/thưa thớt (`20% ≤ Null < 100%`)** | 34 | `$.payload.bidInvContractorOfflineDTO.briefFileId` (93.7%), `$.payload.bidInvContractorOfflineDTO.briefFileName` (93.7%), `$.payload.bidInvContractorOfflineDTO.otherFileId` (75.6%), `$.payload.bidInvContractorOfflineDTO.otherFileName` (75.3%), `$.payload.bidoBidStatusDTO.published` (49.5%), `$.payload.bidoBidStatusDTO.successBidOpenDate` (77.9%), `$.payload.bidoBidStatusDTO.successBidOpenDateTc` (99.0%), `$.payload.bidoNotifyContractorP.bidEstimatePrice` (31.5%), `$.payload.bidoNotifyContractorP.bidGuaranteeForm` (66.8%), `$.payload.bidoNotifyContractorP.cancelBy` (96.0%), `$.payload.bidoNotifyContractorP.cancelDate` (96.0%), `$.payload.bidoNotifyContractorP.cancelDecisionAgency` (98.3%) *(và 22 trường khác)* | Cho phép `nullable=True` trên schema Silver; thiết lập chuỗi fallback đa tầng (VD: `isMultiLot`, `public_date`, `currency`).
| **3. Chuỗi rỗng hoặc Mảng rỗng (`Empty > 0%`)** | 2 | `$.payload.cancelBidHistoryList` (100.0%), `$.payload.lsBidpBidLocationDTO` (21.5%) | Hàm `clean_text()` tự động chuẩn hóa chuỗi rỗng `""` hoặc chỉ chứa khoảng trắng về `NULL`; mảng rỗng `[]` không sinh dòng bảng con.
| **4. Đa kiểu dữ liệu (`Mixed Types`)** | 0 | *(Không có)* | Ép kiểu tường minh qua bộ parser (`parse_money`, `parse_boolean`, `parse_date`, `parse_iso_datetime`); ghi nhận `quality_issue` nếu sai định dạng.

**Danh sách chi tiết toàn bộ 30 cảnh báo dị thường (Anomalies) trên các trường có dữ liệu:**

| STT | Trường Dữ Liệu (`Field Path`) | Loại Bất Thường Phát Hiện (`Detected Anomalies`) | Số Bản Ghi Ảnh Hưởng (Trên Mẫu) | Quy Tắc Làm Sạch Tại Silver |
| :---: | :--- | :--- | ---: | :--- |
| 1 | `$.payload.bidoNotifyContractorP.procuringEntityName` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 2 | `$.payload.bidoNotifyContractorP.investorName` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 3 | `$.payload.bidoNotifyContractorP.isDomestic` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 4 | `$.payload.bidoNotifyContractorP.bidOpenLocation` | `whitespace_padding` | `75` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 5 | `$.payload.bidoNotifyContractorP.bidGuaranteeValue` | `sentinel_value_detected: '0'` | `177` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 6 | `$.payload.bidoNotifyContractorP.bidGuaranteeForm` | `sentinel_value_detected: '0'`, `sentinel_value_detected: 'Không'`, `sentinel_value_detected: 'không'`, `whitespace_padding` | `668` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 7 | `$.payload.bidoNotifyContractorP.issueLocation` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 8 | `$.payload.bidoNotifyContractorP.receiveLocation` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 9 | `$.payload.bidoNotifyContractorP.feeValue` | `sentinel_value_detected: '0'` | `577` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 10 | `$.payload.bidoNotifyContractorP.isInternet` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 11 | `$.payload.bidoNotifyContractorP.isWorkday` | `sentinel_value_detected: '0'` | `1,000` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 12 | `$.payload.bidoNotifyContractorP.isTemporaryCalc` | `sentinel_value_detected: '0'` | `1,000` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 13 | `$.payload.bidoNotifyContractorP.checkCloseDate` | `sentinel_value_detected: '0'` | `1,000` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 14 | `$.payload.bidoNotifyContractorP.cancelType` | `sentinel_value_detected: '0'` | `960` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 15 | `$.payload.bidoNotifyContractorP.cancelDecisionNo` | `whitespace_padding` | `960` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 16 | `$.payload.bidInvContractorOfflineDTO.decisionAgency` | `sentinel_value_detected: 'N/A'` | `1,000` | Lọc các chuỗi khuyết danh (`N/A`, `NULL`, `NONE`, `-`) về `NULL` |
| 17 | `$.payload.bidoBidStatusDTO.procuringEntityName` | `whitespace_padding` | `47` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 18 | `$.payload.bidoBidStatusDTO.isMultiLot` | `sentinel_value_detected: '0'` | `1` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 19 | `$.payload.bidpPlanDetail.isDomestic` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 20 | `$.payload.bidpPlanDetail.isInternet` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 21 | `$.payload.bidpPlanDetail.isPrequalification` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 22 | `$.payload.bidpPlanDetail.recCapital` | `sentinel_value_detected: '0'` | `973` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 23 | `$.payload.bidpPlanDetail.bidPriceEx` | `sentinel_value_detected: '0'` | `915` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 24 | `$.payload.bidpPlanDetail.bidStartMonth` | `sentinel_value_detected: '0'` | `1,000` | Giá trị sentinel `0` khi tính theo Quý -> chuyển thành `NULL` qua `_month()` / `_positive_year()` |
| 25 | `$.payload.bidpPlanDetail.bidStartQuarter` | `sentinel_value_detected: '0'` | `1` | Giá trị sentinel `0` khi tính theo Quý -> chuyển thành `NULL` qua `_month()` / `_positive_year()` |
| 26 | `$.payload.bidpPlanDetail.isMultiLot` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 27 | `$.payload.bidpPlanDetail.isConcentrateShopping` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 28 | `$.payload.bidpPlanDetail.isOnlineShopping` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 29 | `$.payload.bidpPlanDetail.supervisor` | `sentinel_value_detected: 'Không'`, `sentinel_value_detected: 'không'` | `750` | Lọc các chuỗi khuyết danh (`N/A`, `NULL`, `NONE`, `-`) về `NULL` |
| 30 | `$.payload.bidoNotifyContractorP.delayDTOList[].reason` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |

---

### 2.6. Bảng `notify_contractor_reoffer_detail` (Resource: `notify_contractor`)

- **Tổng bản ghi hoạt động (Active Rows):** `6,966` bản ghi (`322` files Parquet)
- **Số bản ghi mẫu phân tích sâu (Sampled Records):** `1,000` bản ghi
- **Khoảng thời gian mẫu khảo sát:** `2024-12-09` → `2025-05-27`
- **Tổng số trường phát hiện (Total Flattened Paths):** `421` trường
- **Khóa chính ứng viên đơn (Single Candidate Keys):** `$.source_id`, `$.content_hash`, `$.payload.id`, `$.payload.reofferNo`, `$.payload.notifyId`, `$.payload.notifyNo`, `$.payload.publicDate`, `$.payload.hashContent`, `$.payload.createdDate`, `$.payload.lastModifiedDate`, `$.payload.originalPublicDate`, `$.payload.approvalDetailDTO.id`, `$.payload.approvalDetailDTO.notifyId`, `$.payload.approvalDetailDTO.notifyNo`, `$.payload.approvalDetailDTO.approvalId`, `$.payload.approvalDetailDTO.bmtSendDate`, `$.payload.approvalDetailDTO.cdtApprovalDate`, `$.payload.approvalDetailDTO.createdDate`, `$.payload.approvalDetailDTO.lastModifiedDate`, `$._dlt_id`
- **Khóa ghép ứng viên (Composite Candidate Keys):** `$.source_id` + `$.source_version`

#### 2.6.1. Bảng Thống Kê Đầy Đủ 421 Trường Dữ Liệu Của `notify_contractor_reoffer_detail`

| STT | Đường dẫn trường (`Path`) | Kiểu dữ liệu (`Types`) | Độ đầy đủ (`Tier`) | Populated % | Null % | Empty % | Distinct | Vai trò (`Role`) | Giá trị mẫu (`Top Samples`) | Vấn đề cụ thể phát hiện (`Specific Issues`) |
| :---: | :--- | :--- | :---: | ---: | ---: | ---: | ---: | :---: | :--- | :--- |
| 1 | `$` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Nested | *(Nested Object)* | Không (Clean) |
| 2 | `$._dlt_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `7AxVdT1z11F12w` (1), `8jFjkUf9IqLk+A` (1) | Không (Clean) |
| 3 | `$._dlt_load_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 121 | Key | `1790602570.9853532` (38), `1790594130.4096851` (31) | Không (Clean) |
| 4 | `$.content_hash` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Free-text | `b84380071ca2db380ac102d44abf3...` (1), `d812aecc0169431d03b8d3882aeaf...` (1) | Không (Clean) |
| 5 | `$.ingested_at` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 899 | Attribute | `2026-09-28 11:15:26.955987+00:00` (16), `2026-09-28 11:15:17.977146+00:00` (9) | Không (Clean) |
| 6 | `$.payload` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Nested | *(Nested Object)* | Không (Clean) |
| 7 | `$.payload.allowReoffer` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 8 | `$.payload.appraisalFileId` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 9 | `$.payload.appraisalFileName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Free-text | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 10 | `$.payload.approvalDetailDTO` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Nested | *(Nested Object)* | Không (Clean) |
| 11 | `$.payload.approvalDetailDTO.appraisalContent` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Free-text | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 12 | `$.payload.approvalDetailDTO.appraisalCount` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 13 | `$.payload.approvalDetailDTO.appraisalFileId` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 14 | `$.payload.approvalDetailDTO.appraisalFileName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Free-text | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 15 | `$.payload.approvalDetailDTO.appraisalOtherFileId` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 16 | `$.payload.approvalDetailDTO.appraisalOtherFileName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Free-text | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 17 | `$.payload.approvalDetailDTO.appraisalStatus` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 18 | `$.payload.approvalDetailDTO.approvalContent` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Free-text | *(Null / Rỗng)* | Không (Clean) |
| 19 | `$.payload.approvalDetailDTO.approvalCount` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 20 | `$.payload.approvalDetailDTO.approvalDecAgency` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 282 | Attribute | `Bệnh viện Đa khoa tỉnh Bắc Giang` (42), `Bệnh viện Phổi Trung ương` (37) | Không (Clean) |
| 21 | `$.payload.approvalDetailDTO.approvalDecDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 933 | Timestamp | `2025-04-04T17:48:00` (4), `2025-05-26T09:54:00` (4) | Không (Clean) |
| 22 | `$.payload.approvalDetailDTO.approvalDecFileId` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 23 | `$.payload.approvalDetailDTO.approvalDecFileName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Free-text | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 24 | `$.payload.approvalDetailDTO.approvalDecModiFileId` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 25 | `$.payload.approvalDetailDTO.approvalDecModiFileName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Free-text | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 26 | `$.payload.approvalDetailDTO.approvalDecNo` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 27 | `$.payload.approvalDetailDTO.approvalDenyReason` | `null, string` | **Sparse** | 0.2% | 99.8% | 0.0% | 2 | Free-text | `Điều chỉnh phạm vi cung cấp` (1), `Do đính kèm nhầm file Quyết đ...` (1) | Thưa thớt cao (Null 99.8%) |
| 28 | `$.payload.approvalDetailDTO.approvalId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `82e6d76d-3345-49ff-8a04-2935d...` (1), `c28db633-db39-454f-a825-46ec8...` (1) | Không (Clean) |
| 29 | `$.payload.approvalDetailDTO.approvalOtherFileId` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 30 | `$.payload.approvalDetailDTO.approvalOtherFileName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Free-text | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 31 | `$.payload.approvalDetailDTO.approvalProposalFileId` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 32 | `$.payload.approvalDetailDTO.approvalProposalFileName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Free-text | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 33 | `$.payload.approvalDetailDTO.approvalResult` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Measure | `1` (1000) | Không (Clean) |
| 34 | `$.payload.approvalDetailDTO.approvalStatus` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Attribute | `DPD` (1000) | Không (Clean) |
| 35 | `$.payload.approvalDetailDTO.bmtCancelDate` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Timestamp | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 36 | `$.payload.approvalDetailDTO.bmtCancelDecDate` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Timestamp | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 37 | `$.payload.approvalDetailDTO.bmtCancelDecFileId` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 38 | `$.payload.approvalDetailDTO.bmtCancelDecFileName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Free-text | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 39 | `$.payload.approvalDetailDTO.bmtCancelDecNo` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 40 | `$.payload.approvalDetailDTO.bmtCancelReason` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Free-text | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 41 | `$.payload.approvalDetailDTO.bmtSendDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Timestamp | `2024-12-09T21:09:35` (1), `2024-12-11T17:07:15` (1) | Không (Clean) |
| 42 | `$.payload.approvalDetailDTO.bmtSender` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 341 | Attribute | `vn2400229574` (42), `vn0100908613-04` (37) | Không (Clean) |
| 43 | `$.payload.approvalDetailDTO.bmtSenderName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 282 | Free-text | `vn2400229574` (42), `vn0100908613` (37) | Không (Clean) |
| 44 | `$.payload.approvalDetailDTO.cdtApprovalDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Timestamp | `2024-12-09T21:10:53` (1), `2024-12-11T17:26:47` (1) | Không (Clean) |
| 45 | `$.payload.approvalDetailDTO.cdtApprovaler` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 325 | Attribute | `vn2400229574` (42), `vn0100908613-04` (37) | Không (Clean) |
| 46 | `$.payload.approvalDetailDTO.createdBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 341 | Attribute | `vn2400229574` (42), `vn0100908613-04` (37) | Không (Clean) |
| 47 | `$.payload.approvalDetailDTO.createdDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Timestamp | `2024-12-09T21:09:35.097` (1), `2024-12-11T17:07:15.78` (1) | Không (Clean) |
| 48 | `$.payload.approvalDetailDTO.id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `d3646bf8-af51-40bb-bf0e-4a8c5...` (1), `98360986-afc4-4dcb-a001-93a38...` (1) | Không (Clean) |
| 49 | `$.payload.approvalDetailDTO.lastModifiedBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 325 | Attribute | `vn2400229574` (42), `vn0100908613-04` (37) | Không (Clean) |
| 50 | `$.payload.approvalDetailDTO.lastModifiedDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Timestamp | `2024-12-09T21:10:53.951` (1), `2024-12-11T17:26:47.941` (1) | Không (Clean) |
| 51 | `$.payload.approvalDetailDTO.notifyId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `ec0042fc-87a6-4cfe-982a-1107b...` (1), `6d55950d-5922-421b-9cd9-d0f7b...` (1) | Không (Clean) |
| 52 | `$.payload.approvalDetailDTO.notifyNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `IB2400566433` (1), `IB2400573842` (1) | Không (Clean) |
| 53 | `$.payload.approvalDetailDTO.notifyVersion` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 4 | Key | `00` (943), `01` (48) | Không (Clean) |
| 54 | `$.payload.approvalDetailDTO.planApprovalDecFileId` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 55 | `$.payload.approvalDetailDTO.planApprovalDecFileName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Free-text | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 56 | `$.payload.approvalDetailDTO.projApprovalDecFileId` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 57 | `$.payload.approvalDetailDTO.projApprovalDecFileName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Free-text | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 58 | `$.payload.approvalDetailDTO.proposalCount` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Measure | `0` (913), `1` (79) | Giá trị khuyết danh/Sentinel ('0') |
| 59 | `$.payload.approvalDetailDTO.proposalNo` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 60 | `$.payload.approvalDetailDTO.relatedDocsFileId` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 61 | `$.payload.approvalDetailDTO.relatedDocsFileName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Free-text | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 62 | `$.payload.approvalDetailDTO.ttdAppraisalDate` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Timestamp | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 63 | `$.payload.approvalDetailDTO.ttdAppraisaler` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 64 | `$.payload.bidDetail` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 65 | `$.payload.bidDetail.additionalChoise` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 66 | `$.payload.bidDetail.bidCommodity` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 67 | `$.payload.bidDetail.bidEstimatePrice` | `null, int` | **Sparse** | 1.7% | 98.3% | 0.0% | 16 | Key | `264082890` (2), `210152880` (1) | Thưa thớt cao (Null 98.3%) |
| 68 | `$.payload.bidDetail.bidField` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `HH` (892), `PTV` (108) | Không (Clean) |
| 69 | `$.payload.bidDetail.bidForm` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `CGTTRG` (1000) | Không (Clean) |
| 70 | `$.payload.bidDetail.bidLocation` | `array` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key | *(Nested Array)* | Không (Clean) |
| 71 | `$.payload.bidDetail.bidLocation[]` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 72 | `$.payload.bidDetail.bidLocation[].action` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 73 | `$.payload.bidDetail.bidLocation[].bidDetailId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 980 | Key | `acb1ee48-da4d-4890-bb0f-ad2f8...` (9), `1f1afcc1-423d-440b-a169-acbdd...` (4) | Không (Clean) |
| 74 | `$.payload.bidDetail.bidLocation[].createBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 332 | Key | `vn2400229574` (42), `vn0100908613-04` (37) | Không (Clean) |
| 75 | `$.payload.bidDetail.bidLocation[].createDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 999 | Key | `2025-02-06T14:30:54.694` (3), `2024-12-27T15:51:22.829` (2) | Không (Clean) |
| 76 | `$.payload.bidDetail.bidLocation[].districtCode` | `string, null` | **Mostly full** | 96.3% | 3.7% | 0.0% | 181 | Key | `22101` (52), `10101` (51) | Không (Clean) |
| 77 | `$.payload.bidDetail.bidLocation[].districtName` | `string, null` | **Mostly full** | 96.3% | 3.7% | 0.0% | 181 | Key | `Thành phố  Bắc Giang` (52), `Quận Ba Đình` (51) | Không (Clean) |
| 78 | `$.payload.bidDetail.bidLocation[].id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,002 | Key | `36c1c4dd-4717-4b60-a39b-bbe05...` (3), `15ed9cec-af5e-4dd6-97b7-9b94e...` (2) | Không (Clean) |
| 79 | `$.payload.bidDetail.bidLocation[].planId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 871 | Key | `e96ce3ce-746f-4bfe-bd96-83a46...` (25), `0674b6f5-aa92-461e-8b21-411c8...` (18) | Không (Clean) |
| 80 | `$.payload.bidDetail.bidLocation[].provCode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 49 | Key | `101` (146), `701` (101) | Không (Clean) |
| 81 | `$.payload.bidDetail.bidLocation[].provName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 49 | Key | `Thành phố Hà Nội` (146), `TP. Hồ Chí Minh` (101) | Không (Clean) |
| 82 | `$.payload.bidDetail.bidLocation[].wardCode` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 83 | `$.payload.bidDetail.bidLocation[].wardName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 84 | `$.payload.bidDetail.bidMode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `1_MTHS` (1000) | Không (Clean) |
| 85 | `$.payload.bidDetail.bidName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 962 | Key | `Cung cấp lắp đặt thiết bị` (3), `Cung cấp VTTB sửa chữa  đồng ...` (3) | Không (Clean) |
| 86 | `$.payload.bidDetail.bidNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 971 | Key | `BP2500053145` (3), `BP2400966753` (2) | Không (Clean) |
| 87 | `$.payload.bidDetail.bidPrice` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 926 | Key | `300000000` (5), `199500000` (5) | Không (Clean) |
| 88 | `$.payload.bidDetail.bidPriceDetail` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 926 | Key | `[{"invest":300000000,"exchang...` (5), `[{"invest":199500000,"exchang...` (5) | Không (Clean) |
| 89 | `$.payload.bidDetail.bidPriceEx` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 90 | `$.payload.bidDetail.bidPriceUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `VND` (1000) | Không (Clean) |
| 91 | `$.payload.bidDetail.bidStartMonth` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 7 | Key | `0` (626), `4` (117) | Giá trị khuyết danh/Sentinel ('0') |
| 92 | `$.payload.bidDetail.bidStartQuarter` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 4 | Key | `II` (391), `0` (374) | Giá trị khuyết danh/Sentinel ('0') |
| 93 | `$.payload.bidDetail.bidStartUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `Q` (626), `M` (374) | Không (Clean) |
| 94 | `$.payload.bidDetail.bidStartYear` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `2025` (946), `2024` (54) | Không (Clean) |
| 95 | `$.payload.bidDetail.bidpBidDetailAssign` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 96 | `$.payload.bidDetail.bidpBidLotList` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 97 | `$.payload.bidDetail.capMngtUnit` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 98 | `$.payload.bidDetail.capitalDetail` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 400 | Key | `Nguồn thu từ dịch vụ khám bện...` (37), `Nguồn thu hoạt động sự nghiệp` (32) | Không (Clean) |
| 99 | `$.payload.bidDetail.cperiod` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 42 | Key | `30` (157), `12` (145) | Không (Clean) |
| 100 | `$.payload.bidDetail.cperiodUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 4 | Key | `D` (638), `M` (355) | Không (Clean) |
| 101 | `$.payload.bidDetail.createdBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 332 | Key | `vn2400229574` (42), `vn0100908613-04` (37) | Không (Clean) |
| 102 | `$.payload.bidDetail.createdDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 980 | Key | `2025-02-06T14:30:53.861` (3), `2024-12-27T15:51:22.827` (2) | Không (Clean) |
| 103 | `$.payload.bidDetail.ctype` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `TG` (645), `DGCD` (355) | Không (Clean) |
| 104 | `$.payload.bidDetail.detailLotList` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 105 | `$.payload.bidDetail.exchangeDate` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 106 | `$.payload.bidDetail.id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 980 | Key | `3220bc68-bb3d-4fa3-8676-7a65e...` (3), `4cef2401-596b-4ec2-94a8-1c4bc...` (2) | Không (Clean) |
| 107 | `$.payload.bidDetail.isConcentrateShopping` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `0` (1000) | Giá trị khuyết danh/Sentinel ('0') |
| 108 | `$.payload.bidDetail.isDomestic` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `1` (1000) | Không (Clean) |
| 109 | `$.payload.bidDetail.isInScope` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 110 | `$.payload.bidDetail.isInternet` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `1` (1000) | Không (Clean) |
| 111 | `$.payload.bidDetail.isMultiLot` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `0` (776), `1` (224) | Giá trị khuyết danh/Sentinel ('0') |
| 112 | `$.payload.bidDetail.isOnlineShopping` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `0` (1000) | Giá trị khuyết danh/Sentinel ('0') |
| 113 | `$.payload.bidDetail.isPrequalification` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `0` (1000) | Giá trị khuyết danh/Sentinel ('0') |
| 114 | `$.payload.bidDetail.isUpdateAssign` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 115 | `$.payload.bidDetail.lastModifiedBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 331 | Key | `vn2400229574` (42), `vn0100908613-04` (37) | Không (Clean) |
| 116 | `$.payload.bidDetail.lastModifiedDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 979 | Key | `2025-02-06T14:30:54.692` (3), `2024-12-27T15:51:27.562` (2) | Không (Clean) |
| 117 | `$.payload.bidDetail.latestAssignId` | `string, null` | **Full** | 99.9% | 0.1% | 0.0% | 979 | Key | `e64685a9-3186-41c7-b39b-aa5ab...` (3), `8c8be577-ce9b-4d05-b78b-1cd70...` (2) | Không (Clean) |
| 118 | `$.payload.bidDetail.linkNotifyInfo` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 119 | `$.payload.bidDetail.medicines` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 120 | `$.payload.bidDetail.planDecisionDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 121 | Key | `2025-04-02T23:59:59` (26), `2025-04-21T23:59:59` (26) | Không (Clean) |
| 121 | `$.payload.bidDetail.planId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 871 | Key | `e96ce3ce-746f-4bfe-bd96-83a46...` (25), `0674b6f5-aa92-461e-8b21-411c8...` (18) | Không (Clean) |
| 122 | `$.payload.bidDetail.planNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 863 | Key | `PL2500130351` (25), `PL2500066927` (18) | Không (Clean) |
| 123 | `$.payload.bidDetail.planVersion` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 4 | Key | `00` (894), `01` (80) | Không (Clean) |
| 124 | `$.payload.bidDetail.processApply` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `LDT` (1000) | Không (Clean) |
| 125 | `$.payload.bidDetail.recCapital` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 126 | `$.payload.bidDetail.recCapitalUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `VND` (1000) | Không (Clean) |
| 127 | `$.payload.bidDetail.reportToCode` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 128 | `$.payload.bidDetail.reportToName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 129 | `$.payload.bidDetail.shoppingOrg` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 130 | `$.payload.bidDetail.shoppingOrgUnit` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 131 | `$.payload.bidForm` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `CGTTRG` (1000) | Không (Clean) |
| 132 | `$.payload.bidId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 980 | Key | `3220bc68-bb3d-4fa3-8676-7a65e...` (3), `4cef2401-596b-4ec2-94a8-1c4bc...` (2) | Không (Clean) |
| 133 | `$.payload.bidMode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `1_MTHS` (1000) | Không (Clean) |
| 134 | `$.payload.bidName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 962 | Key | `Cung cấp lắp đặt thiết bị` (3), `Cung cấp VTTB sửa chữa  đồng ...` (3) | Không (Clean) |
| 135 | `$.payload.bidNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 971 | Key | `BP2500053145` (3), `BP2400966753` (2) | Không (Clean) |
| 136 | `$.payload.bidPlan` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 137 | `$.payload.bidPlan.approvalStatus` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `01` (990), `02` (10) | Không (Clean) |
| 138 | `$.payload.bidPlan.bidPack` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 15 | Key | `1` (828), `2` (32) | Không (Clean) |
| 139 | `$.payload.bidPlan.capitalType` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 140 | `$.payload.bidPlan.competentPerson` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 141 | `$.payload.bidPlan.decisionAgency` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 326 | Key | `Bệnh viện Đa khoa tỉnh Bắc Giang` (42), `Bệnh viện Phổi Trung ương` (37) | Không (Clean) |
| 142 | `$.payload.bidPlan.decisionDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 120 | Key | `2025-04-02T16:59:59.000+0000` (26), `2025-04-21T16:59:59.000+0000` (26) | Không (Clean) |
| 143 | `$.payload.bidPlan.decisionFileId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 866 | Key | `dd31ade0-177e-49f5-83e4-dfb7b...` (25), `8dccdb8d-4ae4-47c2-a6e4-45050...` (18) | Không (Clean) |
| 144 | `$.payload.bidPlan.decisionFileName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 829 | Key | `4.2- QD 839-QD-BVPTƯ- PD KHLC...` (25), `QĐ 927(2.4.2025) KHLCNT Tiêu ...` (18) | Không (Clean) |
| 145 | `$.payload.bidPlan.decisionNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 849 | Key | `839/QĐ-BVPTƯ` (25), `927/QĐ-BVNTW` (18) | Không (Clean) |
| 146 | `$.payload.bidPlan.description` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `description` (1000) | Không (Clean) |
| 147 | `$.payload.bidPlan.exchangeDate` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 148 | `$.payload.bidPlan.expertiseOrg` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 149 | `$.payload.bidPlan.expertiseReportDate` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 150 | `$.payload.bidPlan.id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 871 | Key | `e96ce3ce-746f-4bfe-bd96-83a46...` (25), `0674b6f5-aa92-461e-8b21-411c8...` (18) | Không (Clean) |
| 151 | `$.payload.bidPlan.investDetail` | `array, null` | **Sparse** | 0.9% | 0.1% | 99.0% | 8 | Key | `[{'invest': 145770000, 'money...` (2), `[{'invest': 194800000, 'money...` (1) | Chuỗi/Mảng rỗng (99.0%) |
| 152 | `$.payload.bidPlan.investDetail[]` | `object` | **Sparse** | 0.9% | 0.0% | 0.0% | 8 | Key | `{'invest': 145770000, 'moneyT...` (2), `{'invest': 194800000, 'moneyT...` (1) | Không (Clean) |
| 153 | `$.payload.bidPlan.investDetail[].exchangeRate` | `null` | **Sparse** | 0.0% | 0.9% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 154 | `$.payload.bidPlan.investDetail[].invest` | `int` | **Sparse** | 0.9% | 0.0% | 0.0% | 8 | Key | `145770000` (2), `194800000` (1) | Không (Clean) |
| 155 | `$.payload.bidPlan.investDetail[].moneyType` | `string` | **Sparse** | 0.9% | 0.0% | 0.0% | 1 | Key | `VND` (9) | Không (Clean) |
| 156 | `$.payload.bidPlan.investTotal` | `int, null` | **Full** | 99.9% | 0.1% | 0.0% | 827 | Key | `11317061203` (25), `7633868167` (18) | Không (Clean) |
| 157 | `$.payload.bidPlan.investTotalUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `VND` (1000) | Không (Clean) |
| 158 | `$.payload.bidPlan.investorCode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 282 | Key | `vn2400229574` (42), `vn0100908613` (37) | Không (Clean) |
| 159 | `$.payload.bidPlan.investorName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 282 | Key | `Bệnh viện Đa khoa tỉnh Bắc Giang` (42), `Bệnh viện Phổi Trung ương` (37) | Không (Clean) |
| 160 | `$.payload.bidPlan.isConfirmStep` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 161 | `$.payload.bidPlan.isOda` | `null, int` | **Sparse** | 1.0% | 99.0% | 0.0% | 1 | Key | `0` (10) | Thưa thớt cao (Null 99.0%); Giá trị khuyết danh/Sentinel ('0') |
| 162 | `$.payload.bidPlan.missNumPack` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 163 | `$.payload.bidPlan.name` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 848 | Key | `Kế hoạch lựa chọn nhà thầu th...` (25), `Cung cấp hóa chất, vật tư tiê...` (18) | Không (Clean) |
| 164 | `$.payload.bidPlan.numPack` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 165 | `$.payload.bidPlan.pId` | `null, string` | **Sparse** | 1.0% | 99.0% | 0.0% | 9 | Key | `c690fc2c-0049-46dd-ba20-9ec4d...` (2), `9b99b6fe-3f8b-4836-a5ea-d9a73...` (1) | Thưa thớt cao (Null 99.0%) |
| 166 | `$.payload.bidPlan.pName` | `string, null` | **Full** | 99.9% | 0.1% | 0.0% | 834 | Key | `Mua sắm hóa chất, sinh phẩm, ...` (25), `Cung cấp hóa chất, vật tư tiê...` (18) | Không (Clean) |
| 167 | `$.payload.bidPlan.pNo` | `null, string` | **Sparse** | 1.0% | 99.0% | 0.0% | 9 | Key | `PR2400080329` (2), `PR2400080296` (1) | Thưa thớt cao (Null 99.0%) |
| 168 | `$.payload.bidPlan.pVersion` | `null, string` | **Sparse** | 1.0% | 99.0% | 0.0% | 2 | Key | `00` (9), `01` (1) | Thưa thớt cao (Null 99.0%) |
| 169 | `$.payload.bidPlan.planNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 863 | Key | `PL2500130351` (25), `PL2500066927` (18) | Không (Clean) |
| 170 | `$.payload.bidPlan.planType` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 4 | Key | `TX` (946), `KHAC` (43) | Không (Clean) |
| 171 | `$.payload.bidPlan.planVersion` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 4 | Key | `00` (894), `01` (80) | Không (Clean) |
| 172 | `$.payload.bidPlan.process` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 173 | `$.payload.bidPlan.projectInvestNm` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 174 | `$.payload.bidPlan.publicDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 871 | Key | `2025-05-27T16:31:11.644` (25), `2025-04-03T16:06:01.404` (18) | Không (Clean) |
| 175 | `$.payload.bidPlan.reciprocalCapital` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 176 | `$.payload.bidPlan.status` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `01` (993), `03` (7) | Không (Clean) |
| 177 | `$.payload.bidPlan.supervisor` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 178 | `$.payload.bidPlan.totalBidPrice` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 179 | `$.payload.bidPlan.versionDTOS` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 180 | `$.payload.bidProject` | `null, object` | **Sparse** | 1.0% | 99.0% | 0.0% | 0 | Key | *(Nested Object)* | Thưa thớt cao (Null 99.0%) |
| 181 | `$.payload.bidProject.budgetRelCode` | `null` | **Sparse** | 0.0% | 1.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 182 | `$.payload.bidProject.code` | `null` | **Sparse** | 0.0% | 1.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 183 | `$.payload.bidProject.competentPersons` | `null` | **Sparse** | 0.0% | 1.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 184 | `$.payload.bidProject.createdBy` | `string` | **Sparse** | 1.0% | 0.0% | 0.0% | 7 | Key | `vn5900634329` (3), `vnz000022500` (2) | Không (Clean) |
| 185 | `$.payload.bidProject.createdDate` | `datetime` | **Sparse** | 1.0% | 0.0% | 0.0% | 9 | Key | `2024-12-27T15:31:01.932` (2), `2024-12-27T14:45:54.834` (1) | Không (Clean) |
| 186 | `$.payload.bidProject.decisionAgency` | `string` | **Sparse** | 1.0% | 0.0% | 0.0% | 8 | Key | `UBND huyện Chư Prông, tỉnh Gi...` (3), `Ủy ban nhân dân huyện Chư Pr ...` (1) | Không (Clean) |
| 187 | `$.payload.bidProject.decisionDate` | `datetime` | **Sparse** | 1.0% | 0.0% | 0.0% | 5 | Key | `2024-12-24T23:59:59` (6), `2023-06-28T23:59:59` (1) | Không (Clean) |
| 188 | `$.payload.bidProject.decisionFileId` | `string` | **Sparse** | 1.0% | 0.0% | 0.0% | 9 | Key | `21125f28-f831-4f4b-82b9-47620...` (2), `85052648-ad02-4d15-9ad3-ab432...` (1) | Không (Clean) |
| 189 | `$.payload.bidProject.decisionFileName` | `string` | **Sparse** | 1.0% | 0.0% | 0.0% | 8 | Key | `QĐ THIET BI.pdf` (2), `QĐ TU SACH.pdf` (2) | Không (Clean) |
| 190 | `$.payload.bidProject.decisionNo` | `string` | **Sparse** | 1.0% | 0.0% | 0.0% | 9 | Key | `Số 2640 /QĐ-UBND huyện Chư Pr...` (2), `Số 2633 /QĐ-UBND huyện Chư Pr...` (1) | Không (Clean) |
| 191 | `$.payload.bidProject.exChangeDate` | `null` | **Sparse** | 0.0% | 1.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 192 | `$.payload.bidProject.form` | `null, string` | **Sparse** | 0.4% | 0.6% | 0.0% | 1 | Key | `TTQLDA` (4) | Không (Clean) |
| 193 | `$.payload.bidProject.group` | `null, string` | **Sparse** | 0.4% | 0.6% | 0.0% | 2 | Key | `NC` (3), `NB` (1) | Không (Clean) |
| 194 | `$.payload.bidProject.hasLinkedPlan` | `null` | **Sparse** | 0.0% | 1.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 195 | `$.payload.bidProject.id` | `string` | **Sparse** | 1.0% | 0.0% | 0.0% | 9 | Key | `c690fc2c-0049-46dd-ba20-9ec4d...` (2), `9b99b6fe-3f8b-4836-a5ea-d9a73...` (1) | Không (Clean) |
| 196 | `$.payload.bidProject.investDetail` | `array` | **Sparse** | 1.0% | 0.0% | 0.0% | 9 | Key | `[{'invest': 145770000, 'money...` (2), `[{'invest': 256080000, 'money...` (1) | Không (Clean) |
| 197 | `$.payload.bidProject.investDetail[]` | `object` | **Sparse** | 1.0% | 0.0% | 0.0% | 9 | Key | `{'invest': 145770000, 'moneyT...` (2), `{'invest': 256080000, 'moneyT...` (1) | Không (Clean) |
| 198 | `$.payload.bidProject.investDetail[].exchangeRate` | `null` | **Sparse** | 0.0% | 1.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 199 | `$.payload.bidProject.investDetail[].invest` | `int` | **Sparse** | 1.0% | 0.0% | 0.0% | 9 | Key | `145770000` (2), `256080000` (1) | Không (Clean) |
| 200 | `$.payload.bidProject.investDetail[].moneyType` | `string` | **Sparse** | 1.0% | 0.0% | 0.0% | 1 | Key | `VND` (10) | Không (Clean) |
| 201 | `$.payload.bidProject.investForm` | `null` | **Sparse** | 0.0% | 1.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 202 | `$.payload.bidProject.investScale` | `null` | **Sparse** | 0.0% | 1.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 203 | `$.payload.bidProject.investTarget` | `null, string` | **Sparse** | 0.1% | 0.9% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 204 | `$.payload.bidProject.investTotal` | `int` | **Sparse** | 1.0% | 0.0% | 0.0% | 9 | Key | `145770000` (2), `256080000` (1) | Không (Clean) |
| 205 | `$.payload.bidProject.investTotalEx` | `null` | **Sparse** | 0.0% | 1.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 206 | `$.payload.bidProject.investTotalUnit` | `string` | **Sparse** | 1.0% | 0.0% | 0.0% | 1 | Key | `VND` (10) | Không (Clean) |
| 207 | `$.payload.bidProject.investorCode` | `string` | **Sparse** | 1.0% | 0.0% | 0.0% | 7 | Key | `vn5900634329` (3), `vnz000022500` (2) | Không (Clean) |
| 208 | `$.payload.bidProject.investorName` | `string` | **Sparse** | 1.0% | 0.0% | 0.0% | 7 | Key | `ỦY BAN NHÂN DÂN XÃ BÀU CẠN` (3), `CÔNG TY ĐIỆN LỰC NGHỆ AN - CH...` (2) | Dư khoảng trắng đầu/cuối (1 bản ghi) |
| 209 | `$.payload.bidProject.isConfirmStep` | `null` | **Sparse** | 0.0% | 1.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 210 | `$.payload.bidProject.isOda` | `int` | **Sparse** | 1.0% | 0.0% | 0.0% | 1 | Key | `0` (10) | Giá trị khuyết danh/Sentinel ('0') |
| 211 | `$.payload.bidProject.isProjectWorkFirst` | `int` | **Sparse** | 1.0% | 0.0% | 0.0% | 2 | Key | `0` (9), `1` (1) | Giá trị khuyết danh/Sentinel ('0') |
| 212 | `$.payload.bidProject.latestAssignId` | `null` | **Sparse** | 0.0% | 1.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 213 | `$.payload.bidProject.linkedPublishPlan` | `null` | **Sparse** | 0.0% | 1.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 214 | `$.payload.bidProject.name` | `string` | **Sparse** | 1.0% | 0.0% | 0.0% | 9 | Key | `Cung cấp lắp đặt thiết bị` (2), `Mua sắm trang thiết bị phục v...` (1) | Không (Clean) |
| 215 | `$.payload.bidProject.no` | `string` | **Sparse** | 1.0% | 0.0% | 0.0% | 9 | Key | `PR2400080329` (2), `PR2400080296` (1) | Không (Clean) |
| 216 | `$.payload.bidProject.performUnit` | `null` | **Sparse** | 0.0% | 1.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 217 | `$.payload.bidProject.period` | `int` | **Sparse** | 1.0% | 0.0% | 0.0% | 4 | Key | `3` (5), `2` (2) | Không (Clean) |
| 218 | `$.payload.bidProject.periodUnit` | `int` | **Sparse** | 1.0% | 0.0% | 0.0% | 3 | Key | `3` (8), `1` (1) | Không (Clean) |
| 219 | `$.payload.bidProject.projectPlanList` | `null` | **Sparse** | 0.0% | 1.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 220 | `$.payload.bidProject.publicDate` | `datetime` | **Sparse** | 1.0% | 0.0% | 0.0% | 9 | Key | `2024-12-27T15:34:07` (2), `2024-12-27T14:46:03` (1) | Không (Clean) |
| 221 | `$.payload.bidProject.reciprocalCapital` | `null` | **Sparse** | 0.0% | 1.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 222 | `$.payload.bidProject.reciprocalCapitalUnit` | `null, string` | **Sparse** | 0.4% | 0.6% | 0.0% | 1 | Key | `VND` (4) | Không (Clean) |
| 223 | `$.payload.bidProject.sponsorCode` | `null` | **Sparse** | 0.0% | 1.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 224 | `$.payload.bidProject.status` | `string` | **Sparse** | 1.0% | 0.0% | 0.0% | 1 | Key | `01` (10) | Không (Clean) |
| 225 | `$.payload.bidProject.updatedBy` | `string` | **Sparse** | 1.0% | 0.0% | 0.0% | 7 | Key | `vn5900634329` (3), `vnz000022500` (2) | Không (Clean) |
| 226 | `$.payload.bidProject.updatedDate` | `datetime` | **Sparse** | 1.0% | 0.0% | 0.0% | 9 | Key | `2024-12-27T15:33:51.678` (2), `2024-12-27T14:46:00.505` (1) | Không (Clean) |
| 227 | `$.payload.bidProject.version` | `string` | **Sparse** | 1.0% | 0.0% | 0.0% | 2 | Key | `00` (9), `01` (1) | Không (Clean) |
| 228 | `$.payload.bidProject.versionDTOS` | `null` | **Sparse** | 0.0% | 1.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 229 | `$.payload.bidValidityPeriod` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 20 | Key | `30` (349), `60` (230) | Không (Clean) |
| 230 | `$.payload.bidoListContractorReofferPassedDTOList` | `array` | **Sparse** | 22.4% | 0.0% | 77.6% | 0 | Key | *(Nested Array)* | Chuỗi/Mảng rỗng (77.6%) |
| 231 | `$.payload.bidoListContractorReofferPassedDTOList[]` | `object` | **Sparse** | 22.4% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 232 | `$.payload.bidoListContractorReofferPassedDTOList[].bidFinalPrice` | `null` | **Sparse** | 0.0% | 22.4% | 0.0% | 0 | Key | *(Null / Rỗng)* | Khuyết một phần (Null 22.4%) |
| 233 | `$.payload.bidoListContractorReofferPassedDTOList[].bidPrice` | `int` | **Sparse** | 22.4% | 0.0% | 0.0% | 222 | Key | `75403408540` (186), `11624023150` (69) | Không (Clean) |
| 234 | `$.payload.bidoListContractorReofferPassedDTOList[].contractorCode` | `string` | **Sparse** | 22.4% | 0.0% | 0.0% | 1,991 | Key | `PP2500208647` (2), `PP2500208648` (2) | Không (Clean) |
| 235 | `$.payload.bidoListContractorReofferPassedDTOList[].contractorName` | `null` | **Sparse** | 0.0% | 22.4% | 0.0% | 0 | Key | *(Null / Rỗng)* | Khuyết một phần (Null 22.4%) |
| 236 | `$.payload.bidoListContractorReofferPassedDTOList[].createdBy` | `string` | **Sparse** | 22.4% | 0.0% | 0.0% | 107 | Key | `vn1801351682` (286), `vn0400815624` (126) | Không (Clean) |
| 237 | `$.payload.bidoListContractorReofferPassedDTOList[].createdDate` | `datetime` | **Sparse** | 22.4% | 0.0% | 0.0% | 246 | Key | `2025-04-01T07:48:48.895` (114), `2025-04-01T07:48:48.894` (72) | Không (Clean) |
| 238 | `$.payload.bidoListContractorReofferPassedDTOList[].id` | `string` | **Sparse** | 22.4% | 0.0% | 0.0% | 1,996 | Key | `94921c44-2365-472b-a0b4-b10d2...` (1), `4b30aeca-a5ba-4aa4-a967-34028...` (1) | Không (Clean) |
| 239 | `$.payload.bidoListContractorReofferPassedDTOList[].lotName` | `string` | **Sparse** | 22.4% | 0.0% | 0.0% | 1,637 | Key | `Glucose` (9), `Phần 1` (8) | Không (Clean) |
| 240 | `$.payload.bidoListContractorReofferPassedDTOList[].lotNo` | `string` | **Sparse** | 22.4% | 0.0% | 0.0% | 1,991 | Key | `PP2500208647` (2), `PP2500208648` (2) | Không (Clean) |
| 241 | `$.payload.bidoListContractorReofferPassedDTOList[].priceInit` | `int` | **Sparse** | 22.4% | 0.0% | 0.0% | 1,475 | Key | `63000000` (10), `25200000` (8) | Không (Clean) |
| 242 | `$.payload.bidoListContractorReofferPassedDTOList[].priceStep` | `int, float` | **Sparse** | 22.4% | 0.0% | 0.0% | 1,272 | Key | `1000` (17), `20000` (15) | Đa kiểu dữ liệu (int, float) |
| 243 | `$.payload.bidoListContractorReofferPassedDTOList[].reofferId` | `string` | **Sparse** | 22.4% | 0.0% | 0.0% | 224 | Key | `1180bf9d-a9c2-45ed-879a-ff1d5...` (186), `041983ea-a422-4051-b1bd-b9528...` (69) | Không (Clean) |
| 244 | `$.payload.bidoListContractorReofferPassedDTOList[].reofferNo` | `string` | **Sparse** | 22.4% | 0.0% | 0.0% | 224 | Key | `IB2500121211` (186), `IB2500226292` (69) | Không (Clean) |
| 245 | `$.payload.bidoListContractorReofferPassedDTOList[].reofferVersion` | `string` | **Sparse** | 22.4% | 0.0% | 0.0% | 4 | Key | `00` (1818), `01` (126) | Không (Clean) |
| 246 | `$.payload.bidoListContractorReofferPassedDTOList[].updatedBy` | `string` | **Sparse** | 22.4% | 0.0% | 0.0% | 107 | Key | `vn1801351682` (286), `vn0400815624` (126) | Không (Clean) |
| 247 | `$.payload.bidoListContractorReofferPassedDTOList[].updatedDate` | `datetime` | **Sparse** | 22.4% | 0.0% | 0.0% | 245 | Key | `2025-04-01T07:48:48.895` (114), `2025-04-01T07:48:48.894` (72) | Không (Clean) |
| 248 | `$.payload.bidoListContractorReofferPassedDTOList[].ventureCode` | `null` | **Sparse** | 0.0% | 22.4% | 0.0% | 0 | Key | *(Null / Rỗng)* | Khuyết một phần (Null 22.4%) |
| 249 | `$.payload.bidoListContractorReofferPassedDTOList[].ventureName` | `null` | **Sparse** | 0.0% | 22.4% | 0.0% | 0 | Key | *(Null / Rỗng)* | Khuyết một phần (Null 22.4%) |
| 250 | `$.payload.bidoNotifyContractor` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 251 | `$.payload.cancelBidHistoryList` | `array` | **Sparse** | 0.1% | 0.0% | 99.9% | 0 | Key | *(Nested Array)* | Chuỗi/Mảng rỗng (99.9%) |
| 252 | `$.payload.cancelBidHistoryList[]` | `object` | **Sparse** | 0.1% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 253 | `$.payload.cancelBidHistoryList[].bidId` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `286fb7ef-41d3-4836-b444-fdc50...` (1) | Không (Clean) |
| 254 | `$.payload.cancelBidHistoryList[].bidName` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `Mua sắm giường bệnh nhân 3 ta...` (1) | Không (Clean) |
| 255 | `$.payload.cancelBidHistoryList[].bidNo` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `BP2500179370` (1) | Không (Clean) |
| 256 | `$.payload.cancelBidHistoryList[].cancelBy` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `vnz000017238-11` (1) | Không (Clean) |
| 257 | `$.payload.cancelBidHistoryList[].cancelDate` | `datetime` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `2025-06-21T18:15:46` (1) | Không (Clean) |
| 258 | `$.payload.cancelBidHistoryList[].cancelDecisionAgency` | `null` | **Sparse** | 0.0% | 0.1% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 259 | `$.payload.cancelBidHistoryList[].cancelDecisionDate` | `datetime` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `2025-06-05T00:00:00` (1) | Không (Clean) |
| 260 | `$.payload.cancelBidHistoryList[].cancelDecisionNo` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `831` (1) | Không (Clean) |
| 261 | `$.payload.cancelBidHistoryList[].cancelFileAttachId` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `74b75b63-7c1f-4779-9987-34341...` (1) | Không (Clean) |
| 262 | `$.payload.cancelBidHistoryList[].cancelFileAttachName` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `QĐ 831- Hủy gói Giường 3 tay ...` (1) | Không (Clean) |
| 263 | `$.payload.cancelBidHistoryList[].cancelNumber` | `int` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `1` (1) | Không (Clean) |
| 264 | `$.payload.cancelBidHistoryList[].cancelReason` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `KDUN` (1) | Không (Clean) |
| 265 | `$.payload.cancelBidHistoryList[].cancelType` | `int` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `1` (1) | Không (Clean) |
| 266 | `$.payload.cancelBidHistoryList[].createdBy` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `vnz000017238` (1) | Không (Clean) |
| 267 | `$.payload.cancelBidHistoryList[].createdDate` | `datetime` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `2025-11-03T15:25:13.41` (1) | Không (Clean) |
| 268 | `$.payload.cancelBidHistoryList[].id` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `a02eb076-eecd-4fb8-b078-4fafc...` (1) | Không (Clean) |
| 269 | `$.payload.cancelBidHistoryList[].notifyId` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `701604bb-1db7-41b4-bf36-58914...` (1) | Không (Clean) |
| 270 | `$.payload.cancelBidHistoryList[].notifyNo` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `IB2500120881` (1) | Không (Clean) |
| 271 | `$.payload.cancelBidHistoryList[].notifyVersion` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `00` (1) | Không (Clean) |
| 272 | `$.payload.cancelBidHistoryList[].processApply` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `LDT` (1) | Không (Clean) |
| 273 | `$.payload.cancelBidHistoryList[].recoveryDate` | `datetime` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `2025-11-03T15:25:13.403` (1) | Không (Clean) |
| 274 | `$.payload.cancelBidHistoryList[].recoveryDecisionFileId` | `null` | **Sparse** | 0.0% | 0.1% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 275 | `$.payload.cancelBidHistoryList[].recoveryDecisionFileName` | `null` | **Sparse** | 0.0% | 0.1% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 276 | `$.payload.cancelBidHistoryList[].recoveryDocumentaryFileId` | `null` | **Sparse** | 0.0% | 0.1% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 277 | `$.payload.cancelBidHistoryList[].recoveryDocumentaryFileName` | `null` | **Sparse** | 0.0% | 0.1% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 278 | `$.payload.cancelBidHistoryList[].recoveryImageFileId` | `null` | **Sparse** | 0.0% | 0.1% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 279 | `$.payload.cancelBidHistoryList[].recoveryImageFileName` | `null` | **Sparse** | 0.0% | 0.1% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 280 | `$.payload.cancelBidHistoryList[].recoveryNote` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `Thao tác nhầm` (1) | Không (Clean) |
| 281 | `$.payload.cancelBidHistoryList[].reportFileId` | `null` | **Sparse** | 0.0% | 0.1% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 282 | `$.payload.cancelBidHistoryList[].reportFileName` | `null` | **Sparse** | 0.0% | 0.1% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 283 | `$.payload.cancelBidHistoryList[].status` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `03` (1) | Không (Clean) |
| 284 | `$.payload.cancelBidHistoryList[].updatedBy` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `vnz000017238-11` (1) | Không (Clean) |
| 285 | `$.payload.cancelBidHistoryList[].updatedDate` | `datetime` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `2025-11-03T15:25:13.41` (1) | Không (Clean) |
| 286 | `$.payload.cancelBy` | `null, string` | **Sparse** | 8.2% | 91.8% | 0.0% | 60 | Attribute | `vn2400229574` (5), `vn4200280369` (4) | Thưa thớt cao (Null 91.8%) |
| 287 | `$.payload.cancelDate` | `null, datetime` | **Sparse** | 8.2% | 91.8% | 0.0% | 82 | Timestamp | `2025-11-10T09:06:45` (1), `2025-11-10T09:08:22` (1) | Thưa thớt cao (Null 91.8%) |
| 288 | `$.payload.cancelDecisionAgency` | `null, string` | **Sparse** | 1.5% | 98.5% | 0.0% | 14 | Attribute | `Bệnh viện Đa khoa tỉnh Bắc Giang` (2), `Trung tâm Y tế thành phố Hạ Long` (1) | Thưa thớt cao (Null 98.5%) |
| 289 | `$.payload.cancelDecisionDate` | `null, datetime` | **Sparse** | 8.0% | 92.0% | 0.0% | 59 | Timestamp | `2025-03-03T00:00:00` (4), `2025-05-26T00:00:00` (4) | Thưa thớt cao (Null 92.0%) |
| 290 | `$.payload.cancelDecisionNo` | `null, string` | **Sparse** | 8.0% | 92.0% | 0.0% | 78 | Key | `97/QĐ-BVĐKT` (2), `831` (2) | Thưa thớt cao (Null 92.0%) |
| 291 | `$.payload.cancelFileAttachId` | `null, string` | **Sparse** | 7.8% | 92.2% | 0.0% | 78 | Key | `f6d22f3a-da6e-4b99-9f29-b476b...` (1), `b155ef4b-45a7-44d4-a94c-ab6b1...` (1) | Thưa thớt cao (Null 92.2%) |
| 292 | `$.payload.cancelFileAttachName` | `null, string` | **Sparse** | 7.8% | 92.2% | 0.0% | 76 | Free-text | `QUYẾT ĐỊNH HỦY THẦU.pdf` (2), `QĐ 831- Hủy gói Giường 3 tay ...` (2) | Thưa thớt cao (Null 92.2%) |
| 293 | `$.payload.cancelReason` | `null, string` | **Sparse** | 8.2% | 91.8% | 0.0% | 19 | Free-text | `KDUN` (49), `TDMT` (12) | Thưa thớt cao (Null 91.8%) |
| 294 | `$.payload.cancelType` | `null, int` | **Sparse** | 6.4% | 93.6% | 0.0% | 2 | Measure | `1` (63), `2` (1) | Thưa thớt cao (Null 93.6%) |
| 295 | `$.payload.certificate` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Không (Clean) |
| 296 | `$.payload.contractType` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Attribute | `TG` (645), `DGCD` (355) | Không (Clean) |
| 297 | `$.payload.contractorPassList` | `array` | **Sparse** | 0.0% | 0.0% | 100.0% | 0 | Nested | *(Nested Array)* | Chuỗi/Mảng rỗng (100.0%) |
| 298 | `$.payload.contractorsResult` | `array, null` | **Gaps present** | 93.1% | 1.1% | 5.8% | 0 | Nested | *(Nested Array)* | Chuỗi/Mảng rỗng (5.8%) |
| 299 | `$.payload.contractorsResultAll` | `array, null` | **Gaps present** | 93.1% | 1.1% | 5.8% | 0 | Nested | *(Nested Array)* | Chuỗi/Mảng rỗng (5.8%) |
| 300 | `$.payload.contractorsResultAll[]` | `object` | **Gaps present** | 93.1% | 0.0% | 0.0% | 0 | Nested | *(Nested Object)* | Không (Clean) |
| 301 | `$.payload.contractorsResultAll[].contractorCode` | `string` | **Gaps present** | 93.1% | 0.0% | 0.0% | 1,376 | Key | `vn0109950754` (237), `vn0109373842` (179) | Không (Clean) |
| 302 | `$.payload.contractorsResultAll[].contractorName` | `string` | **Gaps present** | 93.1% | 0.0% | 0.0% | 1,378 | Free-text | `CÔNG TY TNHH THIẾT BỊ Y TẾ & ...` (237), `CÔNG TY TNHH THIẾT BỊ VẬT TƯ ...` (179) | Dư khoảng trắng đầu/cuối (357 bản ghi) |
| 303 | `$.payload.contractorsResultAll[].createdBy` | `string` | **Gaps present** | 93.1% | 0.0% | 0.0% | 1,456 | Attribute | `vn0109950754` (237), `vn0109373842` (173) | Không (Clean) |
| 304 | `$.payload.contractorsResultAll[].createdDate` | `datetime` | **Gaps present** | 93.1% | 0.0% | 0.0% | 8,955 | Timestamp | `2024-12-12T11:31:47.925022` (1), `2024-12-12T08:21:34.985876` (1) | Không (Clean) |
| 305 | `$.payload.contractorsResultAll[].formCode` | `string` | **Gaps present** | 93.1% | 0.0% | 0.0% | 4 | Key | `BD.DT.02.2699` (4729), `BD.DT.02.2692` (3702) | Không (Clean) |
| 306 | `$.payload.contractorsResultAll[].formValue` | `string` | **Gaps present** | 93.1% | 0.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Không (Clean) |
| 307 | `$.payload.contractorsResultAll[].id` | `string` | **Gaps present** | 93.1% | 0.0% | 0.0% | 8,955 | Key | `7618174a-4925-44ef-8efc-d95db...` (1), `3f28acbd-ba61-440d-9dfb-30329...` (1) | Không (Clean) |
| 308 | `$.payload.contractorsResultAll[].idMongo` | `string` | **Gaps present** | 93.1% | 0.0% | 0.0% | 8,955 | Key | `7586c83e-bc57-4552-a132-4577d...` (1), `7da8a0f2-221f-4cd3-9e3a-346c4...` (1) | Không (Clean) |
| 309 | `$.payload.contractorsResultAll[].isNewest` | `int` | **Gaps present** | 93.1% | 0.0% | 0.0% | 2 | Measure | `1` (4683), `0` (4272) | Giá trị khuyết danh/Sentinel ('0') |
| 310 | `$.payload.contractorsResultAll[].lastModifiedBy` | `string` | **Gaps present** | 93.1% | 0.0% | 0.0% | 1,456 | Attribute | `vn0109950754` (237), `vn0109373842` (173) | Không (Clean) |
| 311 | `$.payload.contractorsResultAll[].lastModifiedDate` | `datetime` | **Gaps present** | 93.1% | 0.0% | 0.0% | 8,955 | Timestamp | `2024-12-12T11:31:47.925022` (1), `2024-12-12T08:21:34.985876` (1) | Không (Clean) |
| 312 | `$.payload.contractorsResultAll[].lotName` | `string` | **Gaps present** | 93.1% | 0.0% | 0.0% | 2,123 | Free-text | `Cung cấp máy hủy tài liệu và ...` (75), `Giấy A4` (66) | Không (Clean) |
| 313 | `$.payload.contractorsResultAll[].lotNo` | `string` | **Gaps present** | 93.1% | 0.0% | 0.0% | 2,331 | Key | `BP2400914875` (75), `PP2500230175` (66) | Không (Clean) |
| 314 | `$.payload.contractorsResultAll[].reofferDate` | `datetime` | **Gaps present** | 93.1% | 0.0% | 0.0% | 8,955 | Timestamp | `2024-12-12T11:31:47.924975` (1), `2024-12-12T08:21:34.985821` (1) | Không (Clean) |
| 315 | `$.payload.contractorsResultAll[].reofferId` | `string` | **Gaps present** | 93.1% | 0.0% | 0.0% | 931 | Key | `1180bf9d-a9c2-45ed-879a-ff1d5...` (258), `1a11544a-6091-4534-a1c6-ba1e4...` (201) | Không (Clean) |
| 316 | `$.payload.contractorsResultAll[].reofferPrice` | `int, float` | **Gaps present** | 93.1% | 0.0% | 0.0% | 5,974 | Measure | `45000000` (20), `60000000` (18) | Đa kiểu dữ liệu (int, float) |
| 317 | `$.payload.contractorsResultAll[].reofferPriceFinal` | `int, float` | **Gaps present** | 93.1% | 0.0% | 0.0% | 6,068 | Measure | `45000000` (20), `60000000` (17) | Đa kiểu dữ liệu (int, float) |
| 318 | `$.payload.contractorsResultAll[].reofferSubmissionId` | `string` | **Gaps present** | 93.1% | 0.0% | 0.0% | 2,998 | Key | `8412f2e3-cdc1-4265-a45a-1a721...` (56), `fbc7033e-5148-4530-b0e3-b1dae...` (40) | Không (Clean) |
| 319 | `$.payload.contractorsResultAll[].times` | `int` | **Gaps present** | 93.1% | 0.0% | 0.0% | 76 | Timestamp | `1` (2339), `2` (933) | Giá trị khuyết danh/Sentinel ('-1') |
| 320 | `$.payload.contractorsResultAll[].ventureCode` | `null` | **Sparse** | 0.0% | 93.1% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 93.1%) |
| 321 | `$.payload.contractorsResultAll[].ventureName` | `null` | **Sparse** | 0.0% | 93.1% | 0.0% | 0 | Free-text | *(Null / Rỗng)* | Thưa thớt cao (Null 93.1%) |
| 322 | `$.payload.contractorsResult[]` | `object` | **Gaps present** | 93.1% | 0.0% | 0.0% | 0 | Nested | *(Nested Object)* | Không (Clean) |
| 323 | `$.payload.contractorsResult[].contractorCode` | `string` | **Gaps present** | 93.1% | 0.0% | 0.0% | 1,376 | Key | `vn0110446823` (110), `vn0109950754` (90) | Không (Clean) |
| 324 | `$.payload.contractorsResult[].contractorName` | `string` | **Gaps present** | 93.1% | 0.0% | 0.0% | 1,378 | Free-text | `CÔNG TY CỔ PHẦN THIẾT BỊ Y TẾ...` (110), `CÔNG TY TNHH THIẾT BỊ Y TẾ & ...` (90) | Dư khoảng trắng đầu/cuối (247 bản ghi) |
| 325 | `$.payload.contractorsResult[].createdBy` | `string` | **Gaps present** | 93.1% | 0.0% | 0.0% | 1,456 | Attribute | `vn0110446823` (110), `vn0109950754` (90) | Không (Clean) |
| 326 | `$.payload.contractorsResult[].createdDate` | `datetime` | **Gaps present** | 93.1% | 0.0% | 0.0% | 4,683 | Timestamp | `2024-12-12T11:31:47.925022` (1), `2024-12-12T08:21:34.985876` (1) | Không (Clean) |
| 327 | `$.payload.contractorsResult[].formCode` | `string` | **Gaps present** | 93.1% | 0.0% | 0.0% | 4 | Key | `BD.DT.02.2699` (2758), `BD.DT.02.2692` (1702) | Không (Clean) |
| 328 | `$.payload.contractorsResult[].formValue` | `string` | **Gaps present** | 93.1% | 0.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Không (Clean) |
| 329 | `$.payload.contractorsResult[].id` | `string` | **Gaps present** | 93.1% | 0.0% | 0.0% | 4,683 | Key | `7618174a-4925-44ef-8efc-d95db...` (1), `3f28acbd-ba61-440d-9dfb-30329...` (1) | Không (Clean) |
| 330 | `$.payload.contractorsResult[].idMongo` | `string` | **Gaps present** | 93.1% | 0.0% | 0.0% | 4,683 | Key | `7586c83e-bc57-4552-a132-4577d...` (1), `7da8a0f2-221f-4cd3-9e3a-346c4...` (1) | Không (Clean) |
| 331 | `$.payload.contractorsResult[].isNewest` | `int` | **Gaps present** | 93.1% | 0.0% | 0.0% | 1 | Measure | `1` (4683) | Không (Clean) |
| 332 | `$.payload.contractorsResult[].lastModifiedBy` | `string` | **Gaps present** | 93.1% | 0.0% | 0.0% | 1,456 | Attribute | `vn0110446823` (110), `vn0109950754` (90) | Không (Clean) |
| 333 | `$.payload.contractorsResult[].lastModifiedDate` | `datetime` | **Gaps present** | 93.1% | 0.0% | 0.0% | 4,683 | Timestamp | `2024-12-12T11:31:47.925022` (1), `2024-12-12T08:21:34.985876` (1) | Không (Clean) |
| 334 | `$.payload.contractorsResult[].lotName` | `string` | **Gaps present** | 93.1% | 0.0% | 0.0% | 2,123 | Free-text | `Giấy A4` (19), `Gói thầu 1: Cung cấp máy tính` (15) | Không (Clean) |
| 335 | `$.payload.contractorsResult[].lotNo` | `string` | **Gaps present** | 93.1% | 0.0% | 0.0% | 2,331 | Key | `PP2500230175` (19), `BP2400914729` (15) | Không (Clean) |
| 336 | `$.payload.contractorsResult[].reofferDate` | `datetime` | **Gaps present** | 93.1% | 0.0% | 0.0% | 4,683 | Timestamp | `2024-12-12T11:31:47.924975` (1), `2024-12-12T08:21:34.985821` (1) | Không (Clean) |
| 337 | `$.payload.contractorsResult[].reofferId` | `string` | **Gaps present** | 93.1% | 0.0% | 0.0% | 931 | Key | `1180bf9d-a9c2-45ed-879a-ff1d5...` (188), `1a11544a-6091-4534-a1c6-ba1e4...` (98) | Không (Clean) |
| 338 | `$.payload.contractorsResult[].reofferPrice` | `int, float` | **Gaps present** | 93.1% | 0.0% | 0.0% | 3,532 | Measure | `9000000` (11), `21000000` (10) | Đa kiểu dữ liệu (int, float) |
| 339 | `$.payload.contractorsResult[].reofferPriceFinal` | `int, float` | **Gaps present** | 93.1% | 0.0% | 0.0% | 3,573 | Measure | `21000000` (10), `9000000` (10) | Đa kiểu dữ liệu (int, float) |
| 340 | `$.payload.contractorsResult[].reofferSubmissionId` | `string` | **Gaps present** | 93.1% | 0.0% | 0.0% | 2,998 | Key | `928f8b78-2fd6-4989-83e3-35ca2...` (21), `603b8cad-d8a8-496d-a276-6f00f...` (20) | Không (Clean) |
| 341 | `$.payload.contractorsResult[].times` | `int` | **Gaps present** | 93.1% | 0.0% | 0.0% | 68 | Timestamp | `1` (1812), `2` (480) | Giá trị khuyết danh/Sentinel ('-1') |
| 342 | `$.payload.contractorsResult[].ventureCode` | `null` | **Sparse** | 0.0% | 93.1% | 0.0% | 0 | Key | *(Null / Rỗng)* | Thưa thớt cao (Null 93.1%) |
| 343 | `$.payload.contractorsResult[].ventureName` | `null` | **Sparse** | 0.0% | 93.1% | 0.0% | 0 | Free-text | *(Null / Rỗng)* | Thưa thớt cao (Null 93.1%) |
| 344 | `$.payload.createdBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 341 | Attribute | `vn2400229574` (42), `vn0100908613-04` (37) | Không (Clean) |
| 345 | `$.payload.createdDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Timestamp | `2024-12-09T21:09:14.78` (1), `2024-12-11T17:07:11.066` (1) | Không (Clean) |
| 346 | `$.payload.createdDateNotify` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 347 | `$.payload.decisionAgency` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 348 | `$.payload.decisionAgencyPass` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 349 | `$.payload.decisionDate` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Timestamp | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 350 | `$.payload.decisionDatePass` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Timestamp | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 351 | `$.payload.decisionFileId` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 352 | `$.payload.decisionFileIdPass` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 353 | `$.payload.decisionFileName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Free-text | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 354 | `$.payload.decisionFileNamePass` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Free-text | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 355 | `$.payload.decisionNo` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 356 | `$.payload.decisionNoPass` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 357 | `$.payload.delayList` | `array` | **Sparse** | 0.1% | 0.0% | 99.9% | 0 | Nested | *(Nested Array)* | Chuỗi/Mảng rỗng (99.9%) |
| 358 | `$.payload.delayList[]` | `object` | **Sparse** | 0.1% | 0.0% | 0.0% | 0 | Nested | *(Nested Object)* | Không (Clean) |
| 359 | `$.payload.delayList[].bidCloseDate` | `datetime` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `2025-04-02T10:10:00` (1) | Không (Clean) |
| 360 | `$.payload.delayList[].bidCloseDelayDate` | `datetime` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `2025-04-03T16:05:00` (1) | Không (Clean) |
| 361 | `$.payload.delayList[].bidName` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `Mua sắm găng tay y tế các cỡ` (1) | Không (Clean) |
| 362 | `$.payload.delayList[].bidNo` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `BP2500168105` (1) | Không (Clean) |
| 363 | `$.payload.delayList[].bidOpenDate` | `datetime` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `2025-04-03T10:10:00` (1) | Không (Clean) |
| 364 | `$.payload.delayList[].bidOpenDelayDate` | `datetime` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `2025-04-03T16:05:00` (1) | Không (Clean) |
| 365 | `$.payload.delayList[].createdBy` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Attribute | `root` (1) | Không (Clean) |
| 366 | `$.payload.delayList[].createdDate` | `datetime` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Timestamp | `2025-04-03T10:22:32.184` (1) | Không (Clean) |
| 367 | `$.payload.delayList[].id` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `e6555f8d-a2e5-479e-af2c-2ed63...` (1) | Không (Clean) |
| 368 | `$.payload.delayList[].notifyList` | `null` | **Sparse** | 0.0% | 0.1% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 369 | `$.payload.delayList[].notifyNo` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `IB2500113016` (1) | Không (Clean) |
| 370 | `$.payload.delayList[].notifyType` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `TBMT` (1) | Không (Clean) |
| 371 | `$.payload.delayList[].notifyVersion` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `00` (1) | Không (Clean) |
| 372 | `$.payload.delayList[].reason` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Free-text | `Gia hạn tự động các gói thầu ...` (1) | Không (Clean) |
| 373 | `$.payload.delayList[].sendEmailInfos` | `null` | **Sparse** | 0.0% | 0.1% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Không (Clean) |
| 374 | `$.payload.hashContent` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Free-text | `8ac3bd1bdf6c0f55be8d97e376cce...` (1), `ad00c334d9f5c46114e3ff901e8de...` (1) | Không (Clean) |
| 375 | `$.payload.id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `ec0042fc-87a6-4cfe-982a-1107b...` (1), `6d55950d-5922-421b-9cd9-d0f7b...` (1) | Không (Clean) |
| 376 | `$.payload.infoReBidNotification` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 377 | `$.payload.investField` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Attribute | `HH` (892), `PTV` (108) | Không (Clean) |
| 378 | `$.payload.investorCode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 282 | Key | `vn2400229574` (42), `vn0100908613` (37) | Không (Clean) |
| 379 | `$.payload.investorName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 282 | Free-text | `Bệnh viện Đa khoa tỉnh Bắc Giang` (42), `Bệnh viện Phổi Trung ương` (37) | Không (Clean) |
| 380 | `$.payload.isDomestic` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Measure | `1` (1000) | Không (Clean) |
| 381 | `$.payload.isInternet` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Measure | `1` (1000) | Không (Clean) |
| 382 | `$.payload.isMultiLot` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Measure | `0` (776), `1` (224) | Giá trị khuyết danh/Sentinel ('0') |
| 383 | `$.payload.isPrequalification` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Measure | `0` (1000) | Giá trị khuyết danh/Sentinel ('0') |
| 384 | `$.payload.lastModifiedBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 341 | Attribute | `vn2400229574` (42), `vn0100908613-04` (37) | Không (Clean) |
| 385 | `$.payload.lastModifiedDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Timestamp | `2024-12-09T21:10:53.996` (1), `2024-12-11T17:26:47.948` (1) | Không (Clean) |
| 386 | `$.payload.medicalType` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Attribute | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 387 | `$.payload.notiPublicKey` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 388 | `$.payload.notifyId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `ec0042fc-87a6-4cfe-982a-1107b...` (1), `6d55950d-5922-421b-9cd9-d0f7b...` (1) | Không (Clean) |
| 389 | `$.payload.notifyNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `IB2400566433` (1), `IB2400573842` (1) | Không (Clean) |
| 390 | `$.payload.notifyType` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `CGTTRG` (1000) | Không (Clean) |
| 391 | `$.payload.notifyVersion` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 4 | Key | `00` (943), `01` (48) | Không (Clean) |
| 392 | `$.payload.originalPublicDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Timestamp | `2024-12-09T21:10:53.945` (1), `2024-12-11T17:26:47.937` (1) | Không (Clean) |
| 393 | `$.payload.planName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 848 | Free-text | `Kế hoạch lựa chọn nhà thầu th...` (25), `Cung cấp hóa chất, vật tư tiê...` (18) | Không (Clean) |
| 394 | `$.payload.planNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 863 | Key | `PL2500130351` (25), `PL2500066927` (18) | Không (Clean) |
| 395 | `$.payload.planType` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 4 | Attribute | `TX` (946), `KHAC` (43) | Không (Clean) |
| 396 | `$.payload.priceInit` | `int, null` | **Gaps present** | 93.2% | 6.8% | 0.0% | 860 | Measure | `300000000` (5), `199500000` (5) | Không (Clean) |
| 397 | `$.payload.priceStep` | `int, null, float` | **Sparse** | 77.6% | 22.4% | 0.0% | 378 | Measure | `300000` (96), `500000` (95) | Khuyết một phần (Null 22.4%); Đa kiểu dữ liệu (int, float) |
| 398 | `$.payload.processApply` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Attribute | `LDT` (1000) | Không (Clean) |
| 399 | `$.payload.procuringEntityCode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 282 | Key | `vn2400229574` (42), `vn0100908613` (37) | Không (Clean) |
| 400 | `$.payload.procuringEntityName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 282 | Free-text | `Bệnh viện Đa khoa tỉnh Bắc Giang` (42), `Bệnh viện Phổi Trung ương` (37) | Không (Clean) |
| 401 | `$.payload.projectName` | `string, null` | **Full** | 99.9% | 0.1% | 0.0% | 834 | Free-text | `Mua sắm hóa chất, sinh phẩm, ...` (25), `Cung cấp hóa chất, vật tư tiê...` (18) | Không (Clean) |
| 402 | `$.payload.publicDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Timestamp | `2024-12-09T21:10:53.996` (1), `2024-12-11T17:26:47.948` (1) | Không (Clean) |
| 403 | `$.payload.publicKey` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Attribute | `publicKey` (1000) | Không (Clean) |
| 404 | `$.payload.reofferCloseDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 473 | Timestamp | `2025-05-30T10:30:00` (25), `2025-04-10T09:00:00` (19) | Không (Clean) |
| 405 | `$.payload.reofferNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `IB2400566433` (1), `IB2400573842` (1) | Không (Clean) |
| 406 | `$.payload.reofferOpenDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 534 | Timestamp | `2025-06-02T10:30:00` (25), `2025-04-11T09:00:00` (21) | Không (Clean) |
| 407 | `$.payload.reofferVersion` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 4 | Measure | `00` (943), `01` (48) | Không (Clean) |
| 408 | `$.payload.reportFileId` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 409 | `$.payload.reportFileName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Free-text | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 410 | `$.payload.serial` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 322 | Measure | `11166036430929866113935107681...` (42), `11166062791625987737408018144...` (37) | Không (Clean) |
| 411 | `$.payload.signatures` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 325 | Attribute | `M/eyDuLog3OmH02QBA5a8MJMBXuo7...` (1), `h5FmkG9PNkOj9Z2xZ/KHp5MNFH/05...` (1) | Không (Clean) |
| 412 | `$.payload.status` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Measure | `01` (919), `03` (81) | Không (Clean) |
| 413 | `$.payload.statusReoffer` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Attribute | `RESULT_MST` (989), `PUB_MT` (10) | Không (Clean) |
| 414 | `$.payload.versionDTOS` | `array` | **Full** | 100.0% | 0.0% | 0.0% | 999 | Nested | `[{'id': 'ec0042fc-87a6-4cfe-9...` (1), `[{'id': '6d55950d-5922-421b-9...` (1) | Không (Clean) |
| 415 | `$.payload.versionDTOS[]` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 1,068 | Nested | `{'id': 'ec0042fc-87a6-4cfe-98...` (1), `{'id': '6d55950d-5922-421b-9c...` (1) | Không (Clean) |
| 416 | `$.payload.versionDTOS[].id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,068 | Key | `ec0042fc-87a6-4cfe-982a-1107b...` (1), `6d55950d-5922-421b-9cd9-d0f7b...` (1) | Không (Clean) |
| 417 | `$.payload.versionDTOS[].version` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 5 | Measure | `00` (1000), `01` (57) | Không (Clean) |
| 418 | `$.run_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 121 | Key | `212dd28421834f2ead6c4538b734df21` (38), `04f05f927f5a4745880beb9c35ce0c33` (31) | Không (Clean) |
| 419 | `$.source_date` | `date` | **Full** | 100.0% | 0.0% | 0.0% | 121 | Timestamp | `2025-05-27` (38), `2025-04-04` (31) | Không (Clean) |
| 420 | `$.source_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `IB2400566433` (1), `IB2400573842` (1) | Không (Clean) |
| 421 | `$.source_version` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 4 | Measure | `00` (943), `01` (48) | Không (Clean) |

#### 2.6.2. Tổng Hợp Các Vấn Đề Chất Lượng Dữ Liệu Cụ Thể Trên Bảng `notify_contractor_reoffer_detail`

| Nhóm Vấn Đề Chất Lượng (Issue Category) | Số Trường Bị Ảnh Hưởng | Chi Tiết Trường & Bằng Chứng Số Liệu | Hướng Xử Lý Khi Sang Tầng Silver |
| :--- | :---: | :--- | :--- |
| **1. Trường rỗng hoàn toàn (`100% Null`)** | 86 | `$.payload.allowReoffer`, `$.payload.appraisalFileId`, `$.payload.appraisalFileName`, `$.payload.approvalDetailDTO.appraisalContent`, `$.payload.approvalDetailDTO.appraisalCount`, `$.payload.approvalDetailDTO.appraisalFileId`, `$.payload.approvalDetailDTO.appraisalFileName`, `$.payload.approvalDetailDTO.appraisalOtherFileId`, `$.payload.approvalDetailDTO.appraisalOtherFileName`, `$.payload.approvalDetailDTO.appraisalStatus`, `$.payload.approvalDetailDTO.approvalCount`, `$.payload.approvalDetailDTO.approvalDecFileId`, `$.payload.approvalDetailDTO.approvalDecFileName`, `$.payload.approvalDetailDTO.approvalDecModiFileId`, `$.payload.approvalDetailDTO.approvalDecModiFileName` *(và 71 trường khác)* | Không đưa vào các cột nghiệp vụ chính của Silver; vẫn bảo toàn trong `root_json` / `raw_json`.
| **2. Trường khuyết/thưa thớt (`20% ≤ Null < 100%`)** | 25 | `$.payload.approvalDetailDTO.approvalDenyReason` (99.8%), `$.payload.bidDetail.bidEstimatePrice` (98.3%), `$.payload.bidPlan.isOda` (99.0%), `$.payload.bidPlan.pId` (99.0%), `$.payload.bidPlan.pNo` (99.0%), `$.payload.bidPlan.pVersion` (99.0%), `$.payload.bidProject` (99.0%), `$.payload.bidoListContractorReofferPassedDTOList[].bidFinalPrice` (22.4%), `$.payload.bidoListContractorReofferPassedDTOList[].contractorName` (22.4%), `$.payload.bidoListContractorReofferPassedDTOList[].ventureCode` (22.4%), `$.payload.bidoListContractorReofferPassedDTOList[].ventureName` (22.4%), `$.payload.cancelBy` (91.8%) *(và 13 trường khác)* | Cho phép `nullable=True` trên schema Silver; thiết lập chuỗi fallback đa tầng (VD: `isMultiLot`, `public_date`, `currency`).
| **3. Chuỗi rỗng hoặc Mảng rỗng (`Empty > 0%`)** | 7 | `$.payload.bidPlan.investDetail` (99.0%), `$.payload.bidoListContractorReofferPassedDTOList` (77.6%), `$.payload.cancelBidHistoryList` (99.9%), `$.payload.contractorPassList` (100.0%), `$.payload.contractorsResult` (5.8%), `$.payload.contractorsResultAll` (5.8%), `$.payload.delayList` (99.9%) | Hàm `clean_text()` tự động chuẩn hóa chuỗi rỗng `""` hoặc chỉ chứa khoảng trắng về `NULL`; mảng rỗng `[]` không sinh dòng bảng con.
| **4. Đa kiểu dữ liệu (`Mixed Types`)** | 6 | `$.payload.bidoListContractorReofferPassedDTOList[].priceStep` (int/float), `$.payload.contractorsResultAll[].reofferPrice` (int/float), `$.payload.contractorsResultAll[].reofferPriceFinal` (int/float), `$.payload.contractorsResult[].reofferPrice` (int/float), `$.payload.contractorsResult[].reofferPriceFinal` (int/float), `$.payload.priceStep` (int/float) | Ép kiểu tường minh qua bộ parser (`parse_money`, `parse_boolean`, `parse_date`, `parse_iso_datetime`); ghi nhận `quality_issue` nếu sai định dạng.

**Danh sách chi tiết toàn bộ 18 cảnh báo dị thường (Anomalies) trên các trường có dữ liệu:**

| STT | Trường Dữ Liệu (`Field Path`) | Loại Bất Thường Phát Hiện (`Detected Anomalies`) | Số Bản Ghi Ảnh Hưởng (Trên Mẫu) | Quy Tắc Làm Sạch Tại Silver |
| :---: | :--- | :--- | ---: | :--- |
| 1 | `$.payload.isPrequalification` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 2 | `$.payload.isMultiLot` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 3 | `$.payload.bidDetail.isPrequalification` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 4 | `$.payload.bidDetail.bidStartMonth` | `sentinel_value_detected: '0'` | `1,000` | Giá trị sentinel `0` khi tính theo Quý -> chuyển thành `NULL` qua `_month()` / `_positive_year()` |
| 5 | `$.payload.bidDetail.bidStartQuarter` | `sentinel_value_detected: '0'` | `1,000` | Giá trị sentinel `0` khi tính theo Quý -> chuyển thành `NULL` qua `_month()` / `_positive_year()` |
| 6 | `$.payload.bidDetail.isMultiLot` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 7 | `$.payload.bidDetail.isConcentrateShopping` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 8 | `$.payload.bidDetail.isOnlineShopping` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 9 | `$.payload.bidPlan.isOda` | `sentinel_value_detected: '0'` | `990` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 10 | `$.payload.contractorsResult[].times` | `sentinel_value_detected: '-1'` | `1,000` | Lọc các chuỗi khuyết danh (`N/A`, `NULL`, `NONE`, `-`) về `NULL` |
| 11 | `$.payload.contractorsResult[].contractorName` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 12 | `$.payload.contractorsResultAll[].times` | `sentinel_value_detected: '-1'` | `1,000` | Lọc các chuỗi khuyết danh (`N/A`, `NULL`, `NONE`, `-`) về `NULL` |
| 13 | `$.payload.contractorsResultAll[].contractorName` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 14 | `$.payload.contractorsResultAll[].isNewest` | `sentinel_value_detected: '0'` | `1,000` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 15 | `$.payload.approvalDetailDTO.proposalCount` | `sentinel_value_detected: '0'` | `1,000` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 16 | `$.payload.bidProject.isOda` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 17 | `$.payload.bidProject.investorName` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 18 | `$.payload.bidProject.isProjectWorkFirst` | `sentinel_value_detected: '0'` | `1,000` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |

---

### 2.7. Bảng `contractor_result_detail` (Resource: `contractor_result`)

- **Tổng bản ghi hoạt động (Active Rows):** `1,500,212` bản ghi (`1,202` files Parquet)
- **Số bản ghi mẫu phân tích sâu (Sampled Records):** `1,000` bản ghi
- **Khoảng thời gian mẫu khảo sát:** `2022-09-16` → `2022-09-28`
- **Tổng số trường phát hiện (Total Flattened Paths):** `317` trường
- **Khóa chính ứng viên đơn (Single Candidate Keys):** `$.source_id`, `$.ingested_at`, `$.content_hash`, `$.payload.bideContractorInputResultDTO.id`, `$.payload.bideContractorInputResultDTO.notifyNo`, `$.payload.bideContractorInputResultDTO.bidId`, `$.payload.bideContractorInputResultDTO.bidNo`, `$.payload.bideContractorInputResultDTO.decisionFileId`, `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorResultId`, `$.payload.bideContractorInputResultDTO.lotResultDTO[].bidId`, `$.payload.bideContractorInputResultDTO.lotResultDTO[].bidNo`, `$.payload.bideContractorInputResultDTO.createdDate`, `$.payload.bideContractorInputResultDTO.lastModifiedDate`, `$.payload.bidpPlanDetailDTO.id`, `$.payload.bidpPlanDetailDTO.bidNo`, `$.payload.bidpPlanDetailDTO.bidpBidLotList[].bidNo`, `$.payload.bidpPlanDetailDTO.bidpBidLotList[].bidDetailId`, `$.payload.bidpPlanDetailDTO.detailLotList[].bidNo`, `$.payload.bidpPlanDetailDTO.detailLotList[].bidDetailId`, `$.payload.bidpPlanDetailDTO.createdDate`, `$.payload.bidpPlanDetailDTO.latestAssignId`, `$._dlt_id`
- **Khóa ghép ứng viên (Composite Candidate Keys):** `$.source_id` + `$.source_version`

#### 2.7.1. Bảng Thống Kê Đầy Đủ 317 Trường Dữ Liệu Của `contractor_result_detail`

| STT | Đường dẫn trường (`Path`) | Kiểu dữ liệu (`Types`) | Độ đầy đủ (`Tier`) | Populated % | Null % | Empty % | Distinct | Vai trò (`Role`) | Giá trị mẫu (`Top Samples`) | Vấn đề cụ thể phát hiện (`Specific Issues`) |
| :---: | :--- | :--- | :---: | ---: | ---: | ---: | ---: | :---: | :--- | :--- |
| 1 | `$` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Nested | *(Nested Object)* | Không (Clean) |
| 2 | `$._dlt_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `sOasnbiZvfMedg` (1), `U5mjCsLzKigo0A` (1) | Không (Clean) |
| 3 | `$._dlt_load_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 28 | Key | `1789486818.4732175` (50), `1789486827.4417372` (50) | Không (Clean) |
| 4 | `$.content_hash` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Free-text | `8ec4ec2a0269ed70c37a636b989a2...` (1), `5f34efc1883f7e62796989cd29a4e...` (1) | Không (Clean) |
| 5 | `$.ingested_at` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Attribute | `2026-09-15 15:40:03.171022+00:00` (1), `2026-09-15 15:40:03.309438+00:00` (1) | Không (Clean) |
| 6 | `$.payload` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Nested | *(Nested Object)* | Không (Clean) |
| 7 | `$.payload.bideContractorInputResultDTO` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 8 | `$.payload.bideContractorInputResultDTO.baseMedicine` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 9 | `$.payload.bideContractorInputResultDTO.bidEstimatePrice` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 10 | `$.payload.bideContractorInputResultDTO.bidEstimatePriceUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `VND` (1000) | Không (Clean) |
| 11 | `$.payload.bideContractorInputResultDTO.bidField` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 5 | Key | `TV` (563), `HH` (244) | Không (Clean) |
| 12 | `$.payload.bideContractorInputResultDTO.bidForm` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 6 | Key | `CDTRG` (856), `CDT` (120) | Không (Clean) |
| 13 | `$.payload.bideContractorInputResultDTO.bidId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `a28a4270-a04a-4093-808d-6db3e...` (1), `bd28a7ef-830a-4d53-a5df-11f32...` (1) | Không (Clean) |
| 14 | `$.payload.bideContractorInputResultDTO.bidMode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `1_MTHS` (999), `2_MTHS` (1) | Không (Clean) |
| 15 | `$.payload.bideContractorInputResultDTO.bidName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 832 | Key | `Xây lắp` (9), `Khảo sát địa hình và lập Báo ...` (9) | Không (Clean) |
| 16 | `$.payload.bideContractorInputResultDTO.bidNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `BP2200001486` (1), `BP2200001659` (1) | Không (Clean) |
| 17 | `$.payload.bideContractorInputResultDTO.bidPrice` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 885 | Key | `2000000` (29), `2160000` (15) | Không (Clean) |
| 18 | `$.payload.bideContractorInputResultDTO.bidPriceDetail` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 885 | Key | `[{"invest":2000000,"exchangeR...` (29), `[{"invest":2160000,"exchangeR...` (15) | Không (Clean) |
| 19 | `$.payload.bideContractorInputResultDTO.bidPriceUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `VND` (1000) | Không (Clean) |
| 20 | `$.payload.bideContractorInputResultDTO.bidoPreNotifyContractorResultDTO` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 21 | `$.payload.bideContractorInputResultDTO.bidpPlanDetailDTO` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 22 | `$.payload.bideContractorInputResultDTO.cancelBy` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 23 | `$.payload.bideContractorInputResultDTO.cancelDate` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 24 | `$.payload.bideContractorInputResultDTO.cancelDecisionAgency` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 25 | `$.payload.bideContractorInputResultDTO.cancelDecisionDate` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 26 | `$.payload.bideContractorInputResultDTO.cancelDecisionNo` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 27 | `$.payload.bideContractorInputResultDTO.cancelFileAttachId` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 28 | `$.payload.bideContractorInputResultDTO.cancelFileAttachName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 29 | `$.payload.bideContractorInputResultDTO.cancelHistoryDTOS` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 30 | `$.payload.bideContractorInputResultDTO.cancelInfos` | `array` | **Sparse** | 0.0% | 0.0% | 100.0% | 0 | Key | *(Nested Array)* | Chuỗi/Mảng rỗng (100.0%) |
| 31 | `$.payload.bideContractorInputResultDTO.cancelReason` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 32 | `$.payload.bideContractorInputResultDTO.cancelType` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 33 | `$.payload.bideContractorInputResultDTO.cancelTypeSub` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 34 | `$.payload.bideContractorInputResultDTO.contractorList` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 35 | `$.payload.bideContractorInputResultDTO.cperiod` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 42 | Key | `30` (202), `15` (106) | Giá trị khuyết danh/Sentinel ('0') |
| 36 | `$.payload.bideContractorInputResultDTO.cperiodUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `D` (928), `M` (72) | Không (Clean) |
| 37 | `$.payload.bideContractorInputResultDTO.createdBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 423 | Key | `vn2801124917` (19), `vn4500140073` (17) | Không (Clean) |
| 38 | `$.payload.bideContractorInputResultDTO.createdDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `2022-09-16T16:33:51.557` (1), `2022-09-16T15:04:14.406` (1) | Không (Clean) |
| 39 | `$.payload.bideContractorInputResultDTO.ctype` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 4 | Key | `TG` (981), `TG_DGCD` (10) | Không (Clean) |
| 40 | `$.payload.bideContractorInputResultDTO.decisionAgency` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 506 | Key | `Bệnh viện Hữu nghị Việt Tiệp` (20), `Bưu điện tỉnh Thanh Hóa` (19) | Không (Clean) |
| 41 | `$.payload.bideContractorInputResultDTO.decisionAgencyEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 42 | `$.payload.bideContractorInputResultDTO.decisionDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 966 | Key | `2022-09-21T17:00:00` (11), `2022-09-18T17:00:00` (5) | Không (Clean) |
| 43 | `$.payload.bideContractorInputResultDTO.decisionFileId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `d6104e8b-42bb-4342-9d09-fcfde...` (1), `365b7f7c-dd2f-42b1-84bd-bfc28...` (1) | Không (Clean) |
| 44 | `$.payload.bideContractorInputResultDTO.decisionFileIdEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 45 | `$.payload.bideContractorInputResultDTO.decisionFileName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 984 | Key | `QĐ CHI DINH THAU THI CONG CAI...` (3), `QĐ KQ.pdf` (2) | Không (Clean) |
| 46 | `$.payload.bideContractorInputResultDTO.decisionFileNameEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 47 | `$.payload.bideContractorInputResultDTO.decisionNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 990 | Key | `2539/QĐ-UBND` (3), `4376/QĐ-BVVT` (2) | Không (Clean) |
| 48 | `$.payload.bideContractorInputResultDTO.decisionNoEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 49 | `$.payload.bideContractorInputResultDTO.decisionVersions` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 50 | `$.payload.bideContractorInputResultDTO.deliveryCapability` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 51 | `$.payload.bideContractorInputResultDTO.estimatePriceList` | `array` | **Sparse** | 0.0% | 0.0% | 100.0% | 0 | Key | *(Nested Array)* | Chuỗi/Mảng rỗng (100.0%) |
| 52 | `$.payload.bideContractorInputResultDTO.evalReportFileInfo` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 53 | `$.payload.bideContractorInputResultDTO.evalReportFileReplaceInfo` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 54 | `$.payload.bideContractorInputResultDTO.goodFileId` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 55 | `$.payload.bideContractorInputResultDTO.goodFileName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 56 | `$.payload.bideContractorInputResultDTO.id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `33afb06f-c20f-4e0f-88d3-deed0...` (1), `facd9c87-86ef-4406-a6fe-1b8ca...` (1) | Không (Clean) |
| 57 | `$.payload.bideContractorInputResultDTO.inputMethod` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `ALL` (1000) | Không (Clean) |
| 58 | `$.payload.bideContractorInputResultDTO.investorCode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 406 | Key | `vn0200891120` (33), `vn6200024504` (26) | Không (Clean) |
| 59 | `$.payload.bideContractorInputResultDTO.investorName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 406 | Key | `Bệnh viện Hữu nghị Việt Tiệp` (33), `Ban quản lý dự án huyện Phong...` (26) | Dư khoảng trắng đầu/cuối (26 bản ghi) |
| 60 | `$.payload.bideContractorInputResultDTO.isAttachFlowEnabled` | `null, bool` | **Sparse** | 0.1% | 99.9% | 0.0% | 1 | Key | `False` (1) | Null 100% (Trường rỗng/không sử dụng) |
| 61 | `$.payload.bideContractorInputResultDTO.isCdtrgNewFlow` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 62 | `$.payload.bideContractorInputResultDTO.isDomestic` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `1` (1000) | Không (Clean) |
| 63 | `$.payload.bideContractorInputResultDTO.isGoods` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 64 | `$.payload.bideContractorInputResultDTO.isInternet` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `0` (999), `1` (1) | Giá trị khuyết danh/Sentinel ('0') |
| 65 | `$.payload.bideContractorInputResultDTO.isMedicine` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 66 | `$.payload.bideContractorInputResultDTO.isMultiLot` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `0` (997), `1` (3) | Giá trị khuyết danh/Sentinel ('0') |
| 67 | `$.payload.bideContractorInputResultDTO.isRented` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 68 | `$.payload.bideContractorInputResultDTO.lastModifiedBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 423 | Key | `vn2801124917` (19), `vn4500140073` (17) | Không (Clean) |
| 69 | `$.payload.bideContractorInputResultDTO.lastModifiedDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `2022-09-16T16:34:30.501` (1), `2022-09-16T15:04:42.719` (1) | Không (Clean) |
| 70 | `$.payload.bideContractorInputResultDTO.lotEvalResults` | `null, array` | **Sparse** | 0.1% | 99.9% | 0.0% | 0 | Key | *(Nested Array)* | Null 100% (Trường rỗng/không sử dụng) |
| 71 | `$.payload.bideContractorInputResultDTO.lotEvalResults[]` | `object` | **Sparse** | 0.1% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 72 | `$.payload.bideContractorInputResultDTO.lotEvalResults[].bidContractPeriod` | `null` | **Sparse** | 0.0% | 0.1% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 73 | `$.payload.bideContractorInputResultDTO.lotEvalResults[].bidContractPeriodUnit` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `D` (1) | Không (Clean) |
| 74 | `$.payload.bideContractorInputResultDTO.lotEvalResults[].bidName` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `Mua sắm vật tư gioăng, đệm, p...` (1) | Không (Clean) |
| 75 | `$.payload.bideContractorInputResultDTO.lotEvalResults[].bidNo` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `BP2200107519` (1) | Không (Clean) |
| 76 | `$.payload.bideContractorInputResultDTO.lotEvalResults[].content` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `{"expertComment":"Đạt yêu cầu...` (1) | Không (Clean) |
| 77 | `$.payload.bideContractorInputResultDTO.lotEvalResults[].contractorCode` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `vn8021288256` (1) | Không (Clean) |
| 78 | `$.payload.bideContractorInputResultDTO.lotEvalResults[].contractorName` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `CỬA HÀNG VẬT TƯ THIẾT BỊ TỔNG...` (1) | Không (Clean) |
| 79 | `$.payload.bideContractorInputResultDTO.lotEvalResults[].evalContent` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `{"evalBidPrice":121963500,"co...` (1) | Không (Clean) |
| 80 | `$.payload.bideContractorInputResultDTO.lotEvalResults[].evalType` | `int` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `3` (1) | Không (Clean) |
| 81 | `$.payload.bideContractorInputResultDTO.lotEvalResults[].lotEvalId` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `72d713e8-f2de-445e-b265-b4ab8...` (1) | Không (Clean) |
| 82 | `$.payload.bideContractorInputResultDTO.lotEvalResults[].lotEvalResultId` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `264ed6bd-6675-45f6-b2f1-92e59...` (1) | Không (Clean) |
| 83 | `$.payload.bideContractorInputResultDTO.lotEvalResults[].lotName` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `Mua sắm vật tư gioăng, đệm, p...` (1) | Không (Clean) |
| 84 | `$.payload.bideContractorInputResultDTO.lotEvalResults[].lotNo` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `BP2200107519` (1) | Không (Clean) |
| 85 | `$.payload.bideContractorInputResultDTO.lotEvalResults[].notifyId` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `51eaa29a-1509-46bf-8c89-a06aa...` (1) | Không (Clean) |
| 86 | `$.payload.bideContractorInputResultDTO.lotEvalResults[].notifyNo` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `IB2200000503` (1) | Không (Clean) |
| 87 | `$.payload.bideContractorInputResultDTO.lotEvalResults[].notifyType` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `TBMT` (1) | Không (Clean) |
| 88 | `$.payload.bideContractorInputResultDTO.lotEvalResults[].procuringEntityCode` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `vnz000009162` (1) | Không (Clean) |
| 89 | `$.payload.bideContractorInputResultDTO.lotEvalResults[].procuringEntityName` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `Nhà máy A31` (1) | Không (Clean) |
| 90 | `$.payload.bideContractorInputResultDTO.lotEvalResults[].result` | `string` | **Sparse** | 0.1% | 0.0% | 0.0% | 1 | Key | `2` (1) | Không (Clean) |
| 91 | `$.payload.bideContractorInputResultDTO.lotEvalResults[].techScore` | `null` | **Sparse** | 0.0% | 0.1% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 92 | `$.payload.bideContractorInputResultDTO.lotEvalResults[].ventureCode` | `null` | **Sparse** | 0.0% | 0.1% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 93 | `$.payload.bideContractorInputResultDTO.lotEvalResults[].ventureName` | `null` | **Sparse** | 0.0% | 0.1% | 0.0% | 0 | Key | *(Null / Rỗng)* | Không (Clean) |
| 94 | `$.payload.bideContractorInputResultDTO.lotOpenDetails` | `array` | **Sparse** | 0.0% | 0.0% | 100.0% | 0 | Key | *(Nested Array)* | Chuỗi/Mảng rỗng (100.0%) |
| 95 | `$.payload.bideContractorInputResultDTO.lotResultDTO` | `array` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key | *(Nested Array)* | Không (Clean) |
| 96 | `$.payload.bideContractorInputResultDTO.lotResultDTO[]` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 97 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].bidId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `0f1e778f-7c45-4893-90a8-4b756...` (3), `34696fee-494a-4728-ba77-900b3...` (2) | Không (Clean) |
| 98 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].bidName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 832 | Key | `Xây lắp` (9), `Khảo sát địa hình và lập Báo ...` (9) | Không (Clean) |
| 99 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].bidNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `BP2200119654` (3), `BP2200109800` (2) | Không (Clean) |
| 100 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].cPeriod` | `int, null` | **Full** | 99.9% | 0.1% | 0.0% | 42 | Key | `30` (202), `15` (106) | Giá trị khuyết danh/Sentinel ('0') |
| 101 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].cPeriodUnit` | `string, null` | **Full** | 99.9% | 0.1% | 0.0% | 2 | Key | `D` (929), `M` (74) | Không (Clean) |
| 102 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList` | `array` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key | *(Nested Array)* | Không (Clean) |
| 103 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[]` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 104 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].adjustPrice` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 105 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].bidExecutionTime` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 106 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].bidPriceDetail` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 107 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].bidResult` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `1` (1007) | Không (Clean) |
| 108 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].bidWiningPrice` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 879 | Key | `2000000` (30), `2160000` (14) | Không (Clean) |
| 109 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].contractSignDate` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 110 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].contractorNameEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 111 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].cperiod` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 112 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].cperiodText` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 113 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].cperiodUnit` | `null, string` | **Sparse** | 0.1% | 99.9% | 0.0% | 1 | Key | `D` (1) | Null 100% (Trường rỗng/không sử dụng) |
| 114 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].createdBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 423 | Key | `vn2801124917` (19), `vn4500140073` (17) | Không (Clean) |
| 115 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].createdDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,002 | Key | `2022-09-23T10:36:32.314` (2), `2022-09-26T16:35:21.747` (2) | Không (Clean) |
| 116 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].discountPercent` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 117 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].evalBidPrice` | `null, int` | **Sparse** | 0.1% | 99.9% | 0.0% | 1 | Key | `121963500` (1) | Null 100% (Trường rỗng/không sử dụng) |
| 118 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].falsePhase` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 119 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,007 | Key | `21db811b-b2ab-4420-97a6-17007...` (1), `4e462186-b44b-4feb-931e-f42cd...` (1) | Không (Clean) |
| 120 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].lastModifiedBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 423 | Key | `vn2801124917` (19), `vn4500140073` (17) | Không (Clean) |
| 121 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].lastModifiedDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,002 | Key | `2022-09-23T10:36:32.314` (2), `2022-09-26T16:35:21.747` (2) | Không (Clean) |
| 122 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].lotFinalPrice` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 889 | Key | `2000000` (29), `2160000` (15) | Không (Clean) |
| 123 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].lotOpenPrice` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 124 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].lotPrice` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 889 | Key | `2000000` (29), `2160000` (15) | Không (Clean) |
| 125 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].lotResultId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,004 | Key | `799f3428-83b5-422a-8a0b-0fc77...` (2), `eba888b7-ebc6-4666-b807-f35d2...` (2) | Không (Clean) |
| 126 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].negeStatus` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 127 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].newContractorName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 128 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].officePro` | `string, null` | **Gaps present** | 87.7% | 12.3% | 0.0% | 53 | Key | `101` (240), `701` (107) | Không (Clean) |
| 129 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].orgCode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 747 | Key | `vn6200096966` (28), `vn4500335403` (13) | Dư khoảng trắng đầu/cuối (1 bản ghi) |
| 130 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].orgFullname` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 737 | Key | `CÔNG TY CỔ PHẦN TẬP ĐOÀN ĐẦU ...` (28), `CÔNG TY TNHH TƯ VẤN XÂY DỰNG ...` (13) | Dư khoảng trắng đầu/cuối (32 bản ghi) |
| 131 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].otherContent` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 132 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].reason` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 133 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].reasonEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 134 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].recEmail` | `string, null` | **Gaps present** | 89.7% | 10.3% | 0.0% | 634 | Key | `cttv.xuantruong@gmail.com` (28), `tvxdhungthinh@gmail.com` (13) | Không (Clean) |
| 135 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].repIdNo` | `string, null` | **Gaps present** | 87.9% | 12.1% | 0.0% | 624 | Key | `045178392` (28), `264067134` (13) | Dư khoảng trắng đầu/cuối (7 bản ghi) |
| 136 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].role` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 137 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].taxCode` | `string, null` | **Mostly full** | 98.6% | 1.4% | 0.0% | 722 | Key | `6200096966` (28), `4500335403` (13) | Dư khoảng trắng đầu/cuối (2 bản ghi) |
| 138 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].taxNation` | `string, null` | **Full** | 99.6% | 0.4% | 0.0% | 113 | Key | `VN` (884), `Thôn Bạch Mai, xã Đồng Thái, ...` (4) | Dư khoảng trắng đầu/cuối (7 bản ghi) |
| 139 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].taxNationEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 140 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].techScore` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 141 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].ventureCode` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 142 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].ventureName` | `null, string` | **Sparse** | 0.3% | 99.7% | 0.0% | 3 | Key | `Công ty TNHH Tư vấn đầu tư xâ...` (2), `Công ty TNHH xây dựng Cường T...` (2) | Thưa thớt cao (Null 99.7%) |
| 143 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorMedicineList` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 144 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorResultId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `35577d51-a753-44e8-88cf-5f037...` (3), `a1e00794-9eb2-43eb-b1f8-a31df...` (2) | Không (Clean) |
| 145 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].createdBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 423 | Key | `vn2801124917` (19), `vn4500140073` (17) | Không (Clean) |
| 146 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].createdDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,003 | Key | `2022-09-23T10:36:32.314` (2), `2022-09-16T16:33:51.563` (1) | Không (Clean) |
| 147 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].goodsList` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 62 | Key | `[]` (757), `[{"id":"1f7cf71b97444839f0926...` (1) | Không (Clean) |
| 148 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].goodsListEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 149 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,004 | Key | `3506fae8-bd00-42a3-a41f-76e1b...` (1), `48a379e6-fbfd-4855-93b5-cf8fa...` (1) | Không (Clean) |
| 150 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].isInLotOpenDetail` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `0` (1004) | Giá trị khuyết danh/Sentinel ('0') |
| 151 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].isPersonalContractor` | `int, null` | **Gaps present** | 81.6% | 18.4% | 0.0% | 2 | Key | `2` (698), `1` (118) | Không (Clean) |
| 152 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].lastModifiedBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 423 | Key | `vn2801124917` (19), `vn4500140073` (17) | Không (Clean) |
| 153 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].lastModifiedDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,003 | Key | `2022-09-23T10:36:32.314` (2), `2022-09-16T16:33:51.563` (1) | Không (Clean) |
| 154 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].listNotPassKT` | `array` | **Sparse** | 0.0% | 0.0% | 100.0% | 0 | Key | *(Nested Array)* | Chuỗi/Mảng rỗng (100.0%) |
| 155 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].lotEstimatePrice` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 156 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].lotName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 836 | Key | `Xây lắp` (9), `Khảo sát địa hình và lập Báo ...` (9) | Không (Clean) |
| 157 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].lotNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,004 | Key | `BP2200001486` (1), `BP2200001659` (1) | Không (Clean) |
| 158 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].lotPrice` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 889 | Key | `2000000` (29), `2160000` (15) | Không (Clean) |
| 159 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].lotPriceUnit` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 160 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].reason` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 161 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].relatedServicesList` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 162 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].relatedServicesListEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 163 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].resultItemId` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 164 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].resultVersion` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 165 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].serviceDg` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 166 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].status` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `1` (1004) | Không (Clean) |
| 167 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].winningCode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 745 | Key | `vn6200096966` (28), `vn4500335403` (13) | Dư khoảng trắng đầu/cuối (1 bản ghi) |
| 168 | `$.payload.bideContractorInputResultDTO.lotResultItems` | `array` | **Sparse** | 0.0% | 0.0% | 100.0% | 0 | Key | *(Nested Array)* | Chuỗi/Mảng rỗng (100.0%) |
| 169 | `$.payload.bideContractorInputResultDTO.medicalType` | `null, int` | **Sparse** | 0.1% | 99.9% | 0.0% | 1 | Key | `0` (1) | Null 100% (Trường rỗng/không sử dụng); Giá trị khuyết danh/Sentinel ('0') |
| 170 | `$.payload.bideContractorInputResultDTO.medicines` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 171 | `$.payload.bideContractorInputResultDTO.mixBidField` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 172 | `$.payload.bideContractorInputResultDTO.notifyCancelInfo` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 173 | `$.payload.bideContractorInputResultDTO.notifyId` | `null, string` | **Sparse** | 0.1% | 99.9% | 0.0% | 1 | Key | `51eaa29a-1509-46bf-8c89-a06aa...` (1) | Null 100% (Trường rỗng/không sử dụng) |
| 174 | `$.payload.bideContractorInputResultDTO.notifyNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `IB2200000059` (1), `IB2200000040` (1) | Không (Clean) |
| 175 | `$.payload.bideContractorInputResultDTO.notifyVersion` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `00` (1000) | Không (Clean) |
| 176 | `$.payload.bideContractorInputResultDTO.officeAdd` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 177 | `$.payload.bideContractorInputResultDTO.officeDis` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 178 | `$.payload.bideContractorInputResultDTO.officePro` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 179 | `$.payload.bideContractorInputResultDTO.officeWar` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 180 | `$.payload.bideContractorInputResultDTO.openBidFileId` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 181 | `$.payload.bideContractorInputResultDTO.openBidFileName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 182 | `$.payload.bideContractorInputResultDTO.openDate` | `null, datetime` | **Sparse** | 0.1% | 99.9% | 0.0% | 1 | Key | `2022-09-26T08:40:00` (1) | Null 100% (Trường rỗng/không sử dụng) |
| 183 | `$.payload.bideContractorInputResultDTO.originalPublicDate` | `null, datetime` | **Sparse** | 0.1% | 99.9% | 0.0% | 1 | Key | `2022-09-21T08:34:40.995` (1) | Null 100% (Trường rỗng/không sử dụng) |
| 184 | `$.payload.bideContractorInputResultDTO.planDecisionDate` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 185 | `$.payload.bideContractorInputResultDTO.planNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 773 | Key | `PL2200001179` (6), `PL2200005201` (6) | Không (Clean) |
| 186 | `$.payload.bideContractorInputResultDTO.planType` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Key | `TX` (470), `DTPT` (402) | Không (Clean) |
| 187 | `$.payload.bideContractorInputResultDTO.processApply` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `LDT` (990), `KHAC` (10) | Không (Clean) |
| 188 | `$.payload.bideContractorInputResultDTO.procuringEntityCode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 406 | Key | `vn0200891120` (33), `vn6200024504` (26) | Không (Clean) |
| 189 | `$.payload.bideContractorInputResultDTO.procuringEntityName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 406 | Key | `Bệnh viện Hữu nghị Việt Tiệp` (33), `Ban quản lý dự án huyện Phong...` (26) | Dư khoảng trắng đầu/cuối (26 bản ghi) |
| 190 | `$.payload.bideContractorInputResultDTO.projectName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 191 | `$.payload.bideContractorInputResultDTO.publicDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 995 | Key | `2022-09-22T10:24:25` (2), `2022-09-26T14:51:48` (2) | Không (Clean) |
| 192 | `$.payload.bideContractorInputResultDTO.reason` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 193 | `$.payload.bideContractorInputResultDTO.receiverEmail` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 194 | `$.payload.bideContractorInputResultDTO.receiverPhone` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 195 | `$.payload.bideContractorInputResultDTO.repPhone` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 196 | `$.payload.bideContractorInputResultDTO.reportFileId` | `null, string` | **Sparse** | 1.6% | 98.4% | 0.0% | 16 | Key | `b099045c-22ff-489a-89f2-19137...` (1), `e0758589-3513-49af-8059-3965b...` (1) | Thưa thớt cao (Null 98.4%) |
| 197 | `$.payload.bideContractorInputResultDTO.reportFileIdEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 198 | `$.payload.bideContractorInputResultDTO.reportFileName` | `null, string` | **Sparse** | 1.6% | 98.4% | 0.0% | 16 | Key | `1182_QĐ_TTYT.pdf` (1), `QD phe duyet KQLCNT goi thau ...` (1) | Thưa thớt cao (Null 98.4%) |
| 199 | `$.payload.bideContractorInputResultDTO.reportFileNameEn` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 200 | `$.payload.bideContractorInputResultDTO.resultVersion` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Key | `00` (991), `01` (8) | Không (Clean) |
| 201 | `$.payload.bideContractorInputResultDTO.serviceFileId` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 202 | `$.payload.bideContractorInputResultDTO.serviceFileName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 203 | `$.payload.bideContractorInputResultDTO.status` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `IS_PUBLISH` (1000) | Không (Clean) |
| 204 | `$.payload.bideContractorInputResultDTO.type` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `1` (999), `3` (1) | Không (Clean) |
| 205 | `$.payload.bideContractorInputResultDTO.versionDTOS` | `array` | **Full** | 100.0% | 0.0% | 0.0% | 991 | Key | `[{'id': '33afb06f-c20f-4e0f-8...` (1), `[{'id': 'facd9c87-86ef-4406-a...` (1) | Không (Clean) |
| 206 | `$.payload.bideContractorInputResultDTO.versionDTOS[]` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 1,010 | Key | `{'id': '33afb06f-c20f-4e0f-88...` (1), `{'id': 'facd9c87-86ef-4406-a6...` (1) | Không (Clean) |
| 207 | `$.payload.bideContractorInputResultDTO.versionDTOS[].id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,010 | Key | `33afb06f-c20f-4e0f-88d3-deed0...` (1), `facd9c87-86ef-4406-a6fe-1b8ca...` (1) | Không (Clean) |
| 208 | `$.payload.bideContractorInputResultDTO.versionDTOS[].no` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 209 | `$.payload.bideContractorInputResultDTO.versionDTOS[].publicBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 423 | Key | `vn2801124917` (19), `vn4500140073` (17) | Không (Clean) |
| 210 | `$.payload.bideContractorInputResultDTO.versionDTOS[].publicDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,005 | Key | `2022-09-22T10:24:25` (2), `2022-09-26T14:51:48` (2) | Không (Clean) |
| 211 | `$.payload.bideContractorInputResultDTO.versionDTOS[].status` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `IS_PUBLISH` (1010) | Không (Clean) |
| 212 | `$.payload.bideContractorInputResultDTO.versionDTOS[].version` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Key | `00` (1000), `01` (9) | Không (Clean) |
| 213 | `$.payload.bidpPlanDetailDTO` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 214 | `$.payload.bidpPlanDetailDTO.additionalChoise` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 215 | `$.payload.bidpPlanDetailDTO.bidCommodity` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 216 | `$.payload.bidpPlanDetailDTO.bidEstimatePrice` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 217 | `$.payload.bidpPlanDetailDTO.bidField` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 5 | Key | `TV` (563), `HH` (244) | Không (Clean) |
| 218 | `$.payload.bidpPlanDetailDTO.bidForm` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 6 | Key | `CDTRG` (856), `CDT` (120) | Không (Clean) |
| 219 | `$.payload.bidpPlanDetailDTO.bidLocation` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 220 | `$.payload.bidpPlanDetailDTO.bidMode` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `1_MTHS` (999), `2_MTHS` (1) | Không (Clean) |
| 221 | `$.payload.bidpPlanDetailDTO.bidName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 832 | Key | `Xây lắp` (9), `Khảo sát địa hình và lập Báo ...` (9) | Không (Clean) |
| 222 | `$.payload.bidpPlanDetailDTO.bidNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `BP2200001486` (1), `BP2200001659` (1) | Không (Clean) |
| 223 | `$.payload.bidpPlanDetailDTO.bidPrice` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 885 | Key | `2000000` (29), `2160000` (15) | Không (Clean) |
| 224 | `$.payload.bidpPlanDetailDTO.bidPriceDetail` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 885 | Key | `[{"invest":2000000,"exchangeR...` (29), `[{"invest":2160000,"exchangeR...` (15) | Không (Clean) |
| 225 | `$.payload.bidpPlanDetailDTO.bidPriceEx` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 226 | `$.payload.bidpPlanDetailDTO.bidPriceUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `VND` (1000) | Không (Clean) |
| 227 | `$.payload.bidpPlanDetailDTO.bidStartMonth` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 9 | Key | `0` (592), `9` (374) | Giá trị khuyết danh/Sentinel ('0') |
| 228 | `$.payload.bidpPlanDetailDTO.bidStartQuarter` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 5 | Key | `III` (522), `0` (408) | Giá trị khuyết danh/Sentinel ('0') |
| 229 | `$.payload.bidpPlanDetailDTO.bidStartUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `Q` (592), `M` (408) | Không (Clean) |
| 230 | `$.payload.bidpPlanDetailDTO.bidStartYear` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 4 | Key | `2022` (979), `2020` (9) | Không (Clean) |
| 231 | `$.payload.bidpPlanDetailDTO.bidpBidDetailAssign` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 232 | `$.payload.bidpPlanDetailDTO.bidpBidLotList` | `array` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key | *(Nested Array)* | Không (Clean) |
| 233 | `$.payload.bidpPlanDetailDTO.bidpBidLotList[]` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 234 | `$.payload.bidpPlanDetailDTO.bidpBidLotList[].bidDetailId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `0f1e778f-7c45-4893-90a8-4b756...` (3), `34696fee-494a-4728-ba77-900b3...` (2) | Không (Clean) |
| 235 | `$.payload.bidpPlanDetailDTO.bidpBidLotList[].bidName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 832 | Key | `Xây lắp` (9), `Khảo sát địa hình và lập Báo ...` (9) | Không (Clean) |
| 236 | `$.payload.bidpPlanDetailDTO.bidpBidLotList[].bidNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `BP2200119654` (3), `BP2200109800` (2) | Không (Clean) |
| 237 | `$.payload.bidpPlanDetailDTO.bidpBidLotList[].cperiod` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 42 | Key | `30` (202), `15` (106) | Giá trị khuyết danh/Sentinel ('0') |
| 238 | `$.payload.bidpPlanDetailDTO.bidpBidLotList[].cperiodUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `D` (930), `M` (74) | Không (Clean) |
| 239 | `$.payload.bidpPlanDetailDTO.bidpBidLotList[].createdBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 425 | Key | `vn2801124917` (19), `vn4500140073` (17) | Không (Clean) |
| 240 | `$.payload.bidpPlanDetailDTO.bidpBidLotList[].createdDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,004 | Key | `2022-09-16T10:41:33.267` (1), `2022-09-16T14:58:30.114` (1) | Không (Clean) |
| 241 | `$.payload.bidpPlanDetailDTO.bidpBidLotList[].id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,004 | Key | `32d53c60-9478-4b09-9b20-8382b...` (1), `f218ffa3-fdd1-4b0c-989f-7cd4e...` (1) | Không (Clean) |
| 242 | `$.payload.bidpPlanDetailDTO.bidpBidLotList[].lotName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 836 | Key | `Xây lắp` (9), `Khảo sát địa hình và lập Báo ...` (9) | Không (Clean) |
| 243 | `$.payload.bidpPlanDetailDTO.bidpBidLotList[].lotNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,004 | Key | `BP2200001486` (1), `BP2200001659` (1) | Không (Clean) |
| 244 | `$.payload.bidpPlanDetailDTO.bidpBidLotList[].lotPrice` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 889 | Key | `2000000` (29), `2160000` (15) | Không (Clean) |
| 245 | `$.payload.bidpPlanDetailDTO.bidpBidLotList[].planId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 776 | Key | `5ee0ebf8-e31f-41a2-8792-e01c8...` (6), `5bc3b521-33b8-402d-936b-01cd9...` (6) | Không (Clean) |
| 246 | `$.payload.bidpPlanDetailDTO.bidpBidLotList[].planNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 773 | Key | `PL2200001179` (6), `PL2200005201` (6) | Không (Clean) |
| 247 | `$.payload.bidpPlanDetailDTO.bidpBidLotList[].planVersion` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 4 | Key | `00` (946), `01` (44) | Không (Clean) |
| 248 | `$.payload.bidpPlanDetailDTO.bidpBidLotList[].updatedBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 425 | Key | `vn2801124917` (19), `vn4500140073` (17) | Không (Clean) |
| 249 | `$.payload.bidpPlanDetailDTO.bidpBidLotList[].updatedDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,003 | Key | `2022-09-23T10:26:17.506` (2), `2022-09-16T10:41:33.267` (1) | Không (Clean) |
| 250 | `$.payload.bidpPlanDetailDTO.capMngtUnit` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 251 | `$.payload.bidpPlanDetailDTO.capitalDetail` | `string, null` | **Full** | 99.6% | 0.4% | 0.0% | 487 | Key | `Ngân sách nhà nước` (38), `Nguồn thu dịch vụ y tế` (26) | Không (Clean) |
| 252 | `$.payload.bidpPlanDetailDTO.cperiod` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 42 | Key | `30` (202), `15` (106) | Giá trị khuyết danh/Sentinel ('0') |
| 253 | `$.payload.bidpPlanDetailDTO.cperiodUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `D` (928), `M` (72) | Không (Clean) |
| 254 | `$.payload.bidpPlanDetailDTO.createdBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 425 | Key | `vn2801124917` (19), `vn4500140073` (17) | Không (Clean) |
| 255 | `$.payload.bidpPlanDetailDTO.createdDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `2022-09-16T10:41:33.263` (1), `2022-09-16T14:58:30.113` (1) | Không (Clean) |
| 256 | `$.payload.bidpPlanDetailDTO.ctype` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 4 | Key | `TG` (981), `TG_DGCD` (10) | Không (Clean) |
| 257 | `$.payload.bidpPlanDetailDTO.detailLotList` | `array` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key | *(Nested Array)* | Không (Clean) |
| 258 | `$.payload.bidpPlanDetailDTO.detailLotList[]` | `object` | **Full** | 100.0% | 0.0% | 0.0% | 0 | Key | *(Nested Object)* | Không (Clean) |
| 259 | `$.payload.bidpPlanDetailDTO.detailLotList[].action` | `string` | **Sparse** | 0.0% | 0.0% | 100.0% | 0 | Key | *(Null / Rỗng)* | Chuỗi/Mảng rỗng (100.0%) |
| 260 | `$.payload.bidpPlanDetailDTO.detailLotList[].bidDetailId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `0f1e778f-7c45-4893-90a8-4b756...` (3), `34696fee-494a-4728-ba77-900b3...` (2) | Không (Clean) |
| 261 | `$.payload.bidpPlanDetailDTO.detailLotList[].bidName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 832 | Key | `Xây lắp` (9), `Khảo sát địa hình và lập Báo ...` (9) | Không (Clean) |
| 262 | `$.payload.bidpPlanDetailDTO.detailLotList[].bidNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `BP2200119654` (3), `BP2200109800` (2) | Không (Clean) |
| 263 | `$.payload.bidpPlanDetailDTO.detailLotList[].contractType` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 264 | `$.payload.bidpPlanDetailDTO.detailLotList[].cperiod` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 42 | Key | `30` (202), `15` (106) | Giá trị khuyết danh/Sentinel ('0') |
| 265 | `$.payload.bidpPlanDetailDTO.detailLotList[].cperiodUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `D` (930), `M` (74) | Không (Clean) |
| 266 | `$.payload.bidpPlanDetailDTO.detailLotList[].dangBaoChe` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 267 | `$.payload.bidpPlanDetailDTO.detailLotList[].duongDung` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 268 | `$.payload.bidpPlanDetailDTO.detailLotList[].groupMedicine` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 269 | `$.payload.bidpPlanDetailDTO.detailLotList[].id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,004 | Key | `32d53c60-9478-4b09-9b20-8382b...` (1), `f218ffa3-fdd1-4b0c-989f-7cd4e...` (1) | Không (Clean) |
| 270 | `$.payload.bidpPlanDetailDTO.detailLotList[].lotEstimatePrice` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 271 | `$.payload.bidpPlanDetailDTO.detailLotList[].lotName` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 836 | Key | `Xây lắp` (9), `Khảo sát địa hình và lập Báo ...` (9) | Không (Clean) |
| 272 | `$.payload.bidpPlanDetailDTO.detailLotList[].lotNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,004 | Key | `BP2200001486` (1), `BP2200001659` (1) | Không (Clean) |
| 273 | `$.payload.bidpPlanDetailDTO.detailLotList[].lotPrice` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 889 | Key | `2000000` (29), `2160000` (15) | Không (Clean) |
| 274 | `$.payload.bidpPlanDetailDTO.detailLotList[].medicineCode` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 275 | `$.payload.bidpPlanDetailDTO.detailLotList[].medicines` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 276 | `$.payload.bidpPlanDetailDTO.detailLotList[].nongDo` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 277 | `$.payload.bidpPlanDetailDTO.detailLotList[].percentageAdd` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 278 | `$.payload.bidpPlanDetailDTO.detailLotList[].planId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 776 | Key | `5ee0ebf8-e31f-41a2-8792-e01c8...` (6), `5bc3b521-33b8-402d-936b-01cd9...` (6) | Không (Clean) |
| 279 | `$.payload.bidpPlanDetailDTO.detailLotList[].planNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 773 | Key | `PL2200001179` (6), `PL2200005201` (6) | Không (Clean) |
| 280 | `$.payload.bidpPlanDetailDTO.detailLotList[].planVersion` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 4 | Key | `00` (946), `01` (44) | Không (Clean) |
| 281 | `$.payload.bidpPlanDetailDTO.detailLotList[].pricePlan` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 282 | `$.payload.bidpPlanDetailDTO.detailLotList[].qualityStandards` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 283 | `$.payload.bidpPlanDetailDTO.detailLotList[].quantity` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 284 | `$.payload.bidpPlanDetailDTO.detailLotList[].quantityAdd` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 285 | `$.payload.bidpPlanDetailDTO.detailLotList[].tenThuoc` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 286 | `$.payload.bidpPlanDetailDTO.detailLotList[].uom` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 287 | `$.payload.bidpPlanDetailDTO.detailLotList[].valueAdd` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 288 | `$.payload.bidpPlanDetailDTO.exchangeDate` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 289 | `$.payload.bidpPlanDetailDTO.id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `a28a4270-a04a-4093-808d-6db3e...` (1), `bd28a7ef-830a-4d53-a5df-11f32...` (1) | Không (Clean) |
| 290 | `$.payload.bidpPlanDetailDTO.isConcentrateShopping` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `0` (1000) | Giá trị khuyết danh/Sentinel ('0') |
| 291 | `$.payload.bidpPlanDetailDTO.isDomestic` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `1` (1000) | Không (Clean) |
| 292 | `$.payload.bidpPlanDetailDTO.isInScope` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 293 | `$.payload.bidpPlanDetailDTO.isInternet` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `0` (999), `1` (1) | Giá trị khuyết danh/Sentinel ('0') |
| 294 | `$.payload.bidpPlanDetailDTO.isMultiLot` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `0` (997), `1` (3) | Giá trị khuyết danh/Sentinel ('0') |
| 295 | `$.payload.bidpPlanDetailDTO.isOnlineShopping` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `0` (1000) | Giá trị khuyết danh/Sentinel ('0') |
| 296 | `$.payload.bidpPlanDetailDTO.isPrequalification` | `int` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `0` (1000) | Giá trị khuyết danh/Sentinel ('0') |
| 297 | `$.payload.bidpPlanDetailDTO.isUpdateAssign` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 298 | `$.payload.bidpPlanDetailDTO.lastModifiedBy` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 425 | Key | `vn2801124917` (19), `vn4500140073` (17) | Không (Clean) |
| 299 | `$.payload.bidpPlanDetailDTO.lastModifiedDate` | `datetime` | **Full** | 100.0% | 0.0% | 0.0% | 993 | Key | `2022-09-20T17:56:05.919` (3), `2022-09-26T17:09:36.002` (3) | Không (Clean) |
| 300 | `$.payload.bidpPlanDetailDTO.latestAssignId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `4b11779b-b6ca-4bab-94c9-ab515...` (1), `3de1bfd5-e25e-491d-86a2-500ea...` (1) | Không (Clean) |
| 301 | `$.payload.bidpPlanDetailDTO.linkNotifyInfo` | `null, string` | **Sparse** | 0.1% | 99.9% | 0.0% | 1 | Key | `{"notifyNo":"IB2200000503","n...` (1) | Null 100% (Trường rỗng/không sử dụng) |
| 302 | `$.payload.bidpPlanDetailDTO.medicines` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 303 | `$.payload.bidpPlanDetailDTO.planDecisionDate` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 304 | `$.payload.bidpPlanDetailDTO.planId` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 776 | Key | `5ee0ebf8-e31f-41a2-8792-e01c8...` (6), `5bc3b521-33b8-402d-936b-01cd9...` (6) | Không (Clean) |
| 305 | `$.payload.bidpPlanDetailDTO.planNo` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 773 | Key | `PL2200001179` (6), `PL2200005201` (6) | Không (Clean) |
| 306 | `$.payload.bidpPlanDetailDTO.planVersion` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 4 | Key | `00` (944), `01` (43) | Không (Clean) |
| 307 | `$.payload.bidpPlanDetailDTO.processApply` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 2 | Key | `LDT` (990), `KHAC` (10) | Không (Clean) |
| 308 | `$.payload.bidpPlanDetailDTO.recCapital` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 309 | `$.payload.bidpPlanDetailDTO.recCapitalUnit` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1 | Key | `VND` (1000) | Không (Clean) |
| 310 | `$.payload.bidpPlanDetailDTO.reportToCode` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 311 | `$.payload.bidpPlanDetailDTO.reportToName` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 312 | `$.payload.bidpPlanDetailDTO.shoppingOrg` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 313 | `$.payload.bidpPlanDetailDTO.shoppingOrgUnit` | `null` | **Sparse** | 0.0% | 100.0% | 0.0% | 0 | Key | *(Null / Rỗng)* | Null 100% (Trường rỗng/không sử dụng) |
| 314 | `$.run_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 8 | Key | `74597cb45426485b93071e1e6058edbe` (209), `17cee85632c64bac86376cfc934d984a` (192) | Không (Clean) |
| 315 | `$.source_date` | `date` | **Full** | 100.0% | 0.0% | 0.0% | 12 | Timestamp | `2022-09-27` (209), `2022-09-26` (192) | Không (Clean) |
| 316 | `$.source_id` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 1,000 | Key | `IB2200000059` (1), `IB2200000040` (1) | Không (Clean) |
| 317 | `$.source_version` | `string` | **Full** | 100.0% | 0.0% | 0.0% | 3 | Measure | `00` (991), `01` (8) | Không (Clean) |

#### 2.7.2. Tổng Hợp Các Vấn Đề Chất Lượng Dữ Liệu Cụ Thể Trên Bảng `contractor_result_detail`

| Nhóm Vấn Đề Chất Lượng (Issue Category) | Số Trường Bị Ảnh Hưởng | Chi Tiết Trường & Bằng Chứng Số Liệu | Hướng Xử Lý Khi Sang Tầng Silver |
| :--- | :---: | :--- | :--- |
| **1. Trường rỗng hoàn toàn (`100% Null`)** | 121 | `$.payload.bideContractorInputResultDTO.baseMedicine`, `$.payload.bideContractorInputResultDTO.bidEstimatePrice`, `$.payload.bideContractorInputResultDTO.bidoPreNotifyContractorResultDTO`, `$.payload.bideContractorInputResultDTO.bidpPlanDetailDTO`, `$.payload.bideContractorInputResultDTO.cancelBy`, `$.payload.bideContractorInputResultDTO.cancelDate`, `$.payload.bideContractorInputResultDTO.cancelDecisionAgency`, `$.payload.bideContractorInputResultDTO.cancelDecisionDate`, `$.payload.bideContractorInputResultDTO.cancelDecisionNo`, `$.payload.bideContractorInputResultDTO.cancelFileAttachId`, `$.payload.bideContractorInputResultDTO.cancelFileAttachName`, `$.payload.bideContractorInputResultDTO.cancelHistoryDTOS`, `$.payload.bideContractorInputResultDTO.cancelReason`, `$.payload.bideContractorInputResultDTO.cancelType`, `$.payload.bideContractorInputResultDTO.cancelTypeSub` *(và 106 trường khác)* | Không đưa vào các cột nghiệp vụ chính của Silver; vẫn bảo toàn trong `root_json` / `raw_json`.
| **2. Trường khuyết/thưa thớt (`20% ≤ Null < 100%`)** | 3 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].ventureName` (99.7%), `$.payload.bideContractorInputResultDTO.reportFileId` (98.4%), `$.payload.bideContractorInputResultDTO.reportFileName` (98.4%) | Cho phép `nullable=True` trên schema Silver; thiết lập chuỗi fallback đa tầng (VD: `isMultiLot`, `public_date`, `currency`).
| **3. Chuỗi rỗng hoặc Mảng rỗng (`Empty > 0%`)** | 6 | `$.payload.bideContractorInputResultDTO.cancelInfos` (100.0%), `$.payload.bideContractorInputResultDTO.estimatePriceList` (100.0%), `$.payload.bideContractorInputResultDTO.lotOpenDetails` (100.0%), `$.payload.bideContractorInputResultDTO.lotResultDTO[].listNotPassKT` (100.0%), `$.payload.bideContractorInputResultDTO.lotResultItems` (100.0%), `$.payload.bidpPlanDetailDTO.detailLotList[].action` (100.0%) | Hàm `clean_text()` tự động chuẩn hóa chuỗi rỗng `""` hoặc chỉ chứa khoảng trắng về `NULL`; mảng rỗng `[]` không sinh dòng bảng con.
| **4. Đa kiểu dữ liệu (`Mixed Types`)** | 0 | *(Không có)* | Ép kiểu tường minh qua bộ parser (`parse_money`, `parse_boolean`, `parse_date`, `parse_iso_datetime`); ghi nhận `quality_issue` nếu sai định dạng.

**Danh sách chi tiết toàn bộ 24 cảnh báo dị thường (Anomalies) trên các trường có dữ liệu:**

| STT | Trường Dữ Liệu (`Field Path`) | Loại Bất Thường Phát Hiện (`Detected Anomalies`) | Số Bản Ghi Ảnh Hưởng (Trên Mẫu) | Quy Tắc Làm Sạch Tại Silver |
| :---: | :--- | :--- | ---: | :--- |
| 1 | `$.payload.bideContractorInputResultDTO.procuringEntityName` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 2 | `$.payload.bideContractorInputResultDTO.investorName` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 3 | `$.payload.bideContractorInputResultDTO.isInternet` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 4 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].taxNation` | `whitespace_padding` | `4` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 5 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].taxCode` | `whitespace_padding` | `14` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 6 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].repIdNo` | `whitespace_padding` | `121` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 7 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].orgCode` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 8 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].orgFullname` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 9 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].winningCode` | `whitespace_padding` | `1,000` | Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()` |
| 10 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].isInLotOpenDetail` | `sentinel_value_detected: '0'` | `1,000` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 11 | `$.payload.bideContractorInputResultDTO.lotResultDTO[].cPeriod` | `sentinel_value_detected: '0'` | `1` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 12 | `$.payload.bideContractorInputResultDTO.isMultiLot` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 13 | `$.payload.bideContractorInputResultDTO.medicalType` | `sentinel_value_detected: '0'` | `999` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 14 | `$.payload.bideContractorInputResultDTO.cperiod` | `sentinel_value_detected: '0'` | `1,000` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 15 | `$.payload.bidpPlanDetailDTO.bidpBidLotList[].cperiod` | `sentinel_value_detected: '0'` | `1,000` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 16 | `$.payload.bidpPlanDetailDTO.detailLotList[].cperiod` | `sentinel_value_detected: '0'` | `1,000` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |
| 17 | `$.payload.bidpPlanDetailDTO.isInternet` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 18 | `$.payload.bidpPlanDetailDTO.isPrequalification` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 19 | `$.payload.bidpPlanDetailDTO.bidStartMonth` | `sentinel_value_detected: '0'` | `1,000` | Giá trị sentinel `0` khi tính theo Quý -> chuyển thành `NULL` qua `_month()` / `_positive_year()` |
| 20 | `$.payload.bidpPlanDetailDTO.bidStartQuarter` | `sentinel_value_detected: '0'` | `1,000` | Giá trị sentinel `0` khi tính theo Quý -> chuyển thành `NULL` qua `_month()` / `_positive_year()` |
| 21 | `$.payload.bidpPlanDetailDTO.isMultiLot` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 22 | `$.payload.bidpPlanDetailDTO.isConcentrateShopping` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 23 | `$.payload.bidpPlanDetailDTO.isOnlineShopping` | `sentinel_value_detected: '0'` | `1,000` | Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()` |
| 24 | `$.payload.bidpPlanDetailDTO.cperiod` | `sentinel_value_detected: '0'` | `1,000` | Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'` |

---

## 3. Bảng Ánh Xạ Trường Dữ Liệu Từ Bronze Qua Các Bảng Tầng Silver (Bronze to Silver Schema Mapping)

Phần này quy định **bảng ánh xạ chi tiết (Field-to-Column Lineage & Transformation Mapping)** từ các bảng vật lý tầng Bronze sang hệ thống 15 bảng chuẩn hóa quan hệ thuần túy (Pure Relational 1-Hop) tại tầng Silver (DuckDB / Apache Iceberg).

### 3.0. Sơ Đồ Tổng Quan Ánh Xạ Thực Thể (Bronze Tables → Silver Tables Architecture)

| STT | Resource | Bảng Bronze Nguồn | Bảng Silver Thực Thể Chính (Tier 2 - Revision) | Bảng Silver Thực Thể Con (Tier 3 - 1:N Child) | Bảng Quản Trị & Audit (Tier 1) | Transformer Chịu Trách Nhiệm |
| :---: | :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | `project` | `project_detail` | `silver.project_revision` | `silver.project_location` | `silver.observation`, `silver.quality_issue`, `silver.quarantine` | `ProjectDetailTransformer` |
| 2 | `khlcnt` | `khlcnt_plan_detail` | `silver.plan_revision` | *(Không có bảng con trực tiếp)* | `silver.observation`, `silver.quality_issue`, `silver.quarantine` | `PlanDetailTransformer` |
| 3 | `khlcnt` | `khlcnt_bid_package_detail` | `silver.package_revision` | `silver.package_location`, `silver.package_lot` | `silver.observation`, `silver.quality_issue`, `silver.quarantine` | `PackageDetailTransformer` |
| 4 | `notify_contractor` | `notify_contractor_standard_detail`<br>`notify_contractor_vk_adb_detail`<br>`notify_contractor_reoffer_detail` | `silver.notice_revision` | `silver.notice_lot`, `silver.notice_location`, `silver.notice_reoffer_submission` | `silver.observation`, `silver.quality_issue`, `silver.quarantine` | `StandardNoticeTransformer`<br>`VkAdbNoticeTransformer`<br>`ReofferNoticeTransformer` |
| 5 | `contractor_result` | `contractor_result_detail` | `silver.result_revision` | `silver.award_lot`, `silver.award_contractor` | `silver.observation`, `silver.quality_issue`, `silver.quarantine` | `ContractorResultTransformer` |

> **Ghi chú về `khlcnt`:** Tài nguyên Kế hoạch Lựa chọn Nhà thầu bao gồm 2 bảng Bronze (`khlcnt_plan_detail` và `khlcnt_bid_package_detail`), được chuẩn hóa phân rã thành **đúng 4 bảng Silver**: `silver.plan_revision`, `silver.package_revision`, `silver.package_lot` và `silver.package_location`.

### 3.1. Dự Án Đầu Tư (`project_detail` → `silver.project_revision`, `silver.project_location`)

- **Bảng Bronze nguồn:** `project_detail`
- **Các bảng Silver đích:** `silver.project_revision`, `silver.project_location`
- **Cơ chế chuẩn hóa:** Chuẩn hóa hồ sơ Dự án đầu tư từ khối `projectDTO` kết hợp với các trường địa lý cấp root (`provCode`, `districtCode`, `plocation`). Tự động bóc tách chuỗi JSON mảng địa điểm trong `plocation` thành bảng con `silver.project_location` (quan hệ 1-N) và liên kết sang KHLCNT qua `bidpProjectProjectPlanList`.

| STT | Bảng Silver Đích (`Target Table`) | Cột Silver (`Target Column`) | Kiểu Dữ Liệu Silver | Trường Nguồn Bronze (`Bronze JSON Path`) | Độ Đầy Đủ Thực Nghiệm (`Empirical Coverage`) | Quy Tắc Chuyển Đổi & Làm Sạch (`Transformation & Cleaning Rule`) | Ý Nghĩa Nghiệp Vụ |
| :---: | :--- | :--- | :---: | :--- | :--- | :--- | :--- |
| 1 | `silver.project_revision` | `revision_id` | `VARCHAR (PK)` | `$.source_id`<br>`$.source_version`<br>`$.content_hash` | `$.source_id`: **100.0%** (Null 0.0%)<br>`$.source_version`: **100.0%** (Null 0.0%)<br>`$.content_hash`: **100.0%** (Null 0.0%) | SHA-256(['project', entity_id, source_version, root_hash]) | Khóa chính định danh phiên bản dự án (bất biến). |
| 2 | `silver.project_revision` | `entity_id` | `VARCHAR` | `$.payload.projectDTO.id`<br>`$.source_id` | `projectDTO.id`: **100.0%** (Null 0.0%)<br>`$.source_id`: **100.0%** (Null 0.0%) | SHA-256(['project', 'uuid', clean_text(projectDTO.id)]) | Khóa thực thể xuyên suốt các phiên bản điều chỉnh dự án. |
| 3 | `silver.project_revision` | `entity_type` | `VARCHAR` | `$.payload.projectDTO.id` | `projectDTO.id`: **100.0%** (Null 0.0%) | Hằng số 'project' | Phân loại thực thể trong kiến trúc Silver Lakehouse. |
| 4 | `silver.project_revision` | `observation_id` | `VARCHAR (FK)` | `$._dlt_id`<br>`$.content_hash` | `$._dlt_id`: **100.0%** (Null 0.0%)<br>`$.content_hash`: **100.0%** (Null 0.0%) | SHA-256([file_sha256, row_ordinal, content_hash]) | Khóa ngoại truy vết dòng vật lý trong bảng `silver.observation`. |
| 5 | `silver.project_revision` | `is_current` | `BOOLEAN` | `$.source_version`<br>`$.payload.projectDTO.version` | `$.source_version`: **100.0%** (Null 0.0%)<br>`projectDTO.version`: **100.0%** (Null 0.0%) | Release Resolver (SCD Type 2 flag) | Đánh dấu `true` cho bản ghi hiệu lực mới nhất của dự án. |
| 6 | `silver.project_revision` | `project_no` | `VARCHAR` | `$.payload.projectDTO.no`<br>`$.payload.no` | `projectDTO.no`: **100.0%** (Null 0.0%) | clean_text(projectDTO.no) | Mã số dự án hiển thị trên cổng e-GP (VD: PR2400000017). |
| 7 | `silver.project_revision` | `business_number` | `VARCHAR` | `$.payload.projectDTO.no`<br>`$.payload.no` | `projectDTO.no`: **100.0%** (Null 0.0%) | clean_text(projectDTO.no) | Mã nghiệp vụ chuẩn hóa dùng chung cho tra cứu liên thực thể. |
| 8 | `silver.project_revision` | `title` | `VARCHAR` | `$.payload.projectDTO.name`<br>`$.payload.name` | `projectDTO.name`: **100.0%** (Null 0.0%)<br>`name`: **100.0%** (Null 0.0%) | clean_text(projectDTO.name) | Tên dự án đầu tư xây dựng / mua sắm công. |
| 9 | `silver.project_revision` | `investor_code` | `VARCHAR` | `$.payload.projectDTO.investorCode` | `projectDTO.investorCode`: **100.0%** (Null 0.0%) | clean_text(projectDTO.investorCode) | Mã định danh Chủ đầu tư trên hệ thống e-GP. |
| 10 | `silver.project_revision` | `investor_name` | `VARCHAR` | `$.payload.projectDTO.investorName` | `projectDTO.investorName`: **100.0%** (Null 0.0%) | clean_text(projectDTO.investorName) | Tên pháp nhân đầy đủ của Chủ đầu tư. |
| 11 | `silver.project_revision` | `buyer_id` | `VARCHAR` | `$.payload.projectDTO.investorCode` | `projectDTO.investorCode`: **100.0%** (Null 0.0%) | clean_text(projectDTO.investorCode) | Alias chuẩn hóa của bên mua / chủ đầu tư. |
| 12 | `silver.project_revision` | `buyer_name` | `VARCHAR` | `$.payload.projectDTO.investorName` | `projectDTO.investorName`: **100.0%** (Null 0.0%) | clean_text(projectDTO.investorName) | Alias tên bên mua / chủ đầu tư. |
| 13 | `silver.project_revision` | `total_investment` | `DECIMAL(38,6)` | `$.payload.projectDTO.investTotal`<br>`$.payload.investTotal` | `projectDTO.investTotal`: **100.0%** (Null 0.0%)<br>`investTotal`: **100.0%** (Null 0.0%) | parse_money(projectDTO.investTotal) -> warn 'invalid_money' nếu lỗi | Tổng mức đầu tư được phê duyệt của dự án. |
| 14 | `silver.project_revision` | `amount` | `DECIMAL(38,6)` | `$.payload.projectDTO.investTotal`<br>`$.payload.investTotal` | `projectDTO.investTotal`: **100.0%** (Null 0.0%)<br>`investTotal`: **100.0%** (Null 0.0%) | parse_money(projectDTO.investTotal) | Trường số tiền chuẩn hóa đồng nhất giữa các bảng revision. |
| 15 | `silver.project_revision` | `currency` | `VARCHAR` | `$.payload.projectDTO.investTotalUnit`<br>`$.payload.investTotalUnit` | `projectDTO.investTotalUnit`: **100.0%** (Null 0.0%)<br>`investTotalUnit`: **100.0%** (Null 0.0%) | clean_text(investTotalUnit) ?: clean_text(investUnit) | Đơn vị tiền tệ của tổng mức đầu tư (chủ yếu là VND). |
| 16 | `silver.project_revision` | `decision_no` | `VARCHAR` | `$.payload.projectDTO.decisionNo` | `projectDTO.decisionNo`: **100.0%** (Null 0.0%) | clean_text(projectDTO.decisionNo) | Số quyết định phê duyệt dự án đầu tư. |
| 17 | `silver.project_revision` | `decision_date` | `DATE` | `$.payload.projectDTO.decisionDate` | `projectDTO.decisionDate`: **100.0%** (Null 0.0%) | parse_date(projectDTO.decisionDate); kiểm tra decisionDate <= publicDate | Ngày ký quyết định phê duyệt dự án. |
| 18 | `silver.project_revision` | `prov_code` | `VARCHAR` | `$.payload.provCode`<br>`$.payload.projectDTO.provCode` | `provCode`: **100.0%** (Null 0.0%) | clean_pipe_code(provCode) (bóc '|115|' -> '115', lọc '||' -> NULL) | Mã tỉnh/thành phố chính của dự án. |
| 19 | `silver.project_revision` | `district_code` | `VARCHAR` | `$.payload.districtCode`<br>`$.payload.projectDTO.districtCode` | `districtCode`: **100.0%** (Null 0.0%) | clean_pipe_code(districtCode) (bóc '|11509|' -> '11509', lọc '||' -> NULL) | Mã quận/huyện chính của dự án. |
| 20 | `silver.project_revision` | `public_date` | `DATE` | `$.payload.projectDTO.publicDate`<br>`$.payload.publicDate` | `projectDTO.publicDate`: **100.0%** (Null 0.0%)<br>`publicDate`: **100.0%** (Null 0.0%) | parse_date(projectDTO.publicDate) | Ngày công khai dự án (Khóa phân vùng chính trên Iceberg). |
| 21 | `silver.project_revision` | `public_date_raw` | `VARCHAR` | `$.payload.projectDTO.publicDate`<br>`$.payload.publicDate` | `projectDTO.publicDate`: **100.0%** (Null 0.0%)<br>`publicDate`: **100.0%** (Null 0.0%) | clean_text(projectDTO.publicDate) | Chuỗi thời gian công khai gốc từ nguồn. |
| 22 | `silver.project_revision` | `status_raw` | `VARCHAR` | `$.payload.projectDTO.status`<br>`$.payload.status` | `projectDTO.status`: **100.0%** (Null 0.0%)<br>`status`: **100.0%** (Null 0.0%) | clean_text(projectDTO.status) | Mã trạng thái gốc của dự án (VD: '01' = Đã đăng tải). |
| 23 | `silver.project_revision` | `root_json` | `TEXT` | `$.payload.projectDTO` | `projectDTO`: **100.0%** (Null 0.0%) | canonical_json(merged_root) | Chuỗi JSON chuẩn hóa toàn bộ thuộc tính gốc để bảo toàn 100% thông tin. |
| 24 | `silver.project_location` | `location_id` | `VARCHAR (PK)` | `$.payload.plocation`<br>`$.payload.provCode` | `plocation`: **0.0%** (Null 100.0%)<br>`provCode`: **100.0%** (Null 0.0%) | SHA-256([revision_id, 'location', ordinal, loc_data]) | Khóa chính dòng địa điểm thực hiện dự án. |
| 25 | `silver.project_location` | `revision_id` | `VARCHAR (FK)` | `$.payload.projectDTO.id` | `projectDTO.id`: **100.0%** (Null 0.0%) | FK -> silver.project_revision.revision_id | Liên kết về phiên bản dự án cha. |
| 26 | `silver.project_location` | `province_code` | `VARCHAR` | `$.payload.plocation`<br>`$.payload.provCode` | `plocation`: **0.0%** (Null 100.0%)<br>`provCode`: **100.0%** (Null 0.0%) | json.loads(plocation)[i].provCode -> clean_pipe_code (fallback $.payload.provCode) | Mã tỉnh/thành phố sau khi giải mã mảng JSON `plocation`. |
| 27 | `silver.project_location` | `province_name` | `VARCHAR` | `$.payload.plocation`<br>`$.payload.provName` | `plocation`: **0.0%** (Null 100.0%)<br>`provName`: **0.0%** (Null 100.0%) | json.loads(plocation)[i].provName -> clean_text (fallback $.payload.provName) | Tên tỉnh/thành phố thực hiện dự án. |
| 28 | `silver.project_location` | `district_code` | `VARCHAR` | `$.payload.plocation`<br>`$.payload.districtCode` | `plocation`: **0.0%** (Null 100.0%)<br>`districtCode`: **100.0%** (Null 0.0%) | json.loads(plocation)[i].districtCode -> clean_pipe_code (fallback $.payload.districtCode) | Mã quận/huyện thực hiện dự án. |
| 29 | `silver.project_location` | `district_name` | `VARCHAR` | `$.payload.plocation`<br>`$.payload.districtName` | `plocation`: **0.0%** (Null 100.0%)<br>`districtName`: **0.0%** (Null 100.0%) | json.loads(plocation)[i].districtName -> clean_text (fallback $.payload.districtName) | Tên quận/huyện thực hiện dự án. |
| 30 | `silver.project_location` | `ward_code` | `VARCHAR` | `$.payload.plocation` | `plocation`: **0.0%** (Null 100.0%) | json.loads(plocation)[i].wardCode -> clean_pipe_code | Mã phường/xã (nếu có trong phần tử `plocation`). |
| 31 | `silver.project_location` | `ward_name` | `VARCHAR` | `$.payload.plocation` | `plocation`: **0.0%** (Null 100.0%) | json.loads(plocation)[i].wardName -> clean_text | Tên phường/xã (nếu có trong phần tử `plocation`). |
| 32 | `silver.project_location` | `location_name` | `VARCHAR` | `$.payload.plocation` | `plocation`: **0.0%** (Null 100.0%) | Ghép chuỗi sạch '{wardName}, {districtName}, {provName}' | Địa danh hiển thị đầy đủ đã loại bỏ cú pháp mảng JSON thô. |

### 3.2. Kế Hoạch Lựa Chọn Nhà Thầu (`khlcnt_plan_detail` → `silver.plan_revision`)

- **Bảng Bronze nguồn:** `khlcnt_plan_detail`
- **Các bảng Silver đích:** `silver.plan_revision`
- **Cơ chế chuẩn hóa:** Chuẩn hóa thực thể Kế hoạch lựa chọn nhà thầu (KHLCNT) từ khối `bidPoBidpPlanProjectDetailView`. Giải quyết tham chiếu ngược về Dự án đầu tư (`project_id`) thông qua `pid` (UUID) hoặc `pno` (Mã dự án).

| STT | Bảng Silver Đích (`Target Table`) | Cột Silver (`Target Column`) | Kiểu Dữ Liệu Silver | Trường Nguồn Bronze (`Bronze JSON Path`) | Độ Đầy Đủ Thực Nghiệm (`Empirical Coverage`) | Quy Tắc Chuyển Đổi & Làm Sạch (`Transformation & Cleaning Rule`) | Ý Nghĩa Nghiệp Vụ |
| :---: | :--- | :--- | :---: | :--- | :--- | :--- | :--- |
| 1 | `silver.plan_revision` | `revision_id` | `VARCHAR (PK)` | `$.source_id`<br>`$.source_version`<br>`$.content_hash` | `$.source_id`: **100.0%** (Null 0.0%)<br>`$.source_version`: **100.0%** (Null 0.0%)<br>`$.content_hash`: **100.0%** (Null 0.0%) | SHA-256(['plan', entity_id, source_version, root_hash]) | Khóa chính định danh phiên bản KHLCNT. |
| 2 | `silver.plan_revision` | `entity_id` | `VARCHAR` | `$.payload.bidPoBidpPlanProjectDetailView.id` | `bidPoBidpPlanProjectDetailView.id`: **100.0%** (Null 0.0%) | SHA-256(['plan', 'uuid', clean_text(root.id)]) | Khóa định danh thực thể KHLCNT xuyên suốt các lần sửa đổi. |
| 3 | `silver.plan_revision` | `entity_type` | `VARCHAR` | `$.payload.bidPoBidpPlanProjectDetailView.id` | `bidPoBidpPlanProjectDetailView.id`: **100.0%** (Null 0.0%) | Hằng số 'plan' | Phân loại thực thể Kế hoạch. |
| 4 | `silver.plan_revision` | `observation_id` | `VARCHAR (FK)` | `$._dlt_id`<br>`$.content_hash` | `$._dlt_id`: **100.0%** (Null 0.0%)<br>`$.content_hash`: **100.0%** (Null 0.0%) | SHA-256([file_sha256, row_ordinal, content_hash]) | Khóa ngoại truy vết bản ghi Bronze gốc. |
| 5 | `silver.plan_revision` | `is_current` | `BOOLEAN` | `$.source_version` | `$.source_version`: **100.0%** (Null 0.0%) | Release Resolver (SCD Type 2 flag) | Cờ đánh dấu phiên bản KHLCNT mới nhất. |
| 6 | `silver.plan_revision` | `plan_no` | `VARCHAR` | `$.payload.bidPoBidpPlanProjectDetailView.planNo` | `bidPoBidpPlanProjectDetailView.planNo`: **100.0%** (Null 0.0%) | clean_text(root.planNo) | Số hiệu KHLCNT công bố trên e-GP (VD: PL2400001234). |
| 7 | `silver.plan_revision` | `title` | `VARCHAR` | `$.payload.bidPoBidpPlanProjectDetailView.name` | `bidPoBidpPlanProjectDetailView.name`: **100.0%** (Null 0.0%) | clean_text(root.name) | Tên kế hoạch lựa chọn nhà thầu. |
| 8 | `silver.plan_revision` | `investor_name` | `VARCHAR` | `$.payload.bidPoBidpPlanProjectDetailView.investorName` | `bidPoBidpPlanProjectDetailView.investorName`: **100.0%** (Null 0.0%) | clean_text(root.investorName) | Tên Chủ đầu tư / Cơ quan lập kế hoạch. |
| 9 | `silver.plan_revision` | `project_id` | `VARCHAR (FK)` | `$.payload.bidPoBidpPlanProjectDetailView.pid`<br>`$.payload.bidPoBidpPlanProjectDetailView.pno` | `bidPoBidpPlanProjectDetailView.pid`: **62.9%** (Null 37.1%)<br>`bidPoBidpPlanProjectDetailView.pno`: **99.1%** (Null 0.9%) | Reference Resolver phân giải từ `pid` (scheme='uuid') hoặc `pno` (scheme='number') | Khóa ngoại trỏ sang `silver.project_revision.entity_id`. |
| 10 | `silver.plan_revision` | `project_no` | `VARCHAR` | `$.payload.bidPoBidpPlanProjectDetailView.pno` | `bidPoBidpPlanProjectDetailView.pno`: **99.1%** (Null 0.9%) | clean_project_reference(root.pno) (lọc sentinel 'N/A', 'NULL', 'NONE') | Mã số dự án đầu tư cha. |
| 11 | `silver.plan_revision` | `plan_type` | `VARCHAR` | `$.payload.bidPoBidpPlanProjectDetailView.planType` | `bidPoBidpPlanProjectDetailView.planType`: **100.0%** (Null 0.0%) | clean_text(root.planType) | Phân loại kế hoạch (DTPT: Đầu tư phát triển, TX: Thường xuyên, KHAC). |
| 12 | `silver.plan_revision` | `total_investment` | `DECIMAL(38,6)` | `$.payload.bidPoBidpPlanProjectDetailView.investTotal` | `bidPoBidpPlanProjectDetailView.investTotal`: **100.0%** (Null 0.0%) | parse_money(root.investTotal) -> warn 'invalid_money' nếu lỗi | Tổng mức đầu tư / dự toán của kế hoạch. |
| 13 | `silver.plan_revision` | `currency` | `VARCHAR` | `$.payload.bidPoBidpPlanProjectDetailView.investTotalUnit` | `bidPoBidpPlanProjectDetailView.investTotalUnit`: **95.3%** (Null 4.7%) | clean_text(root.investTotalUnit) | Đơn vị tiền tệ (VND). |
| 14 | `silver.plan_revision` | `decision_date` | `DATE` | `$.payload.bidPoBidpPlanProjectDetailView.decisionDate` | `bidPoBidpPlanProjectDetailView.decisionDate`: **100.0%** (Null 0.0%) | parse_date(root.decisionDate) | Ngày ban hành quyết định phê duyệt KHLCNT. |
| 15 | `silver.plan_revision` | `decision_no` | `VARCHAR` | `$.payload.bidPoBidpPlanProjectDetailView.decisionNo` | `bidPoBidpPlanProjectDetailView.decisionNo`: **100.0%** (Null 0.0%) | clean_text(root.decisionNo) | Số quyết định phê duyệt KHLCNT. |
| 16 | `silver.plan_revision` | `decision_agency` | `VARCHAR` | `$.payload.bidPoBidpPlanProjectDetailView.decisionAgency` | `bidPoBidpPlanProjectDetailView.decisionAgency`: **100.0%** (Null 0.0%) | clean_text(root.decisionAgency) | Cơ quan có thẩm quyền phê duyệt KHLCNT. |
| 17 | `silver.plan_revision` | `bid_pack` | `BIGINT` | `$.payload.bidPoBidpPlanProjectDetailView.bidPack` | `bidPoBidpPlanProjectDetailView.bidPack`: **100.0%** (Null 0.0%) | _parse_bid_pack(root.bidPack) (số nguyên không âm) | Tổng số lượng gói thầu thuộc kế hoạch. |
| 18 | `silver.plan_revision` | `location` | `VARCHAR` | `$.payload.bidPoBidpPlanProjectDetailView.location` | `bidPoBidpPlanProjectDetailView.location`: **1.5%** (Null 98.5%) | clean_text(root.location) | Địa điểm thực hiện ghi ở cấp kế hoạch (thường thưa thớt, chi tiết nằm ở gói thầu). |
| 19 | `silver.plan_revision` | `public_date` | `DATE` | `$.payload.bidPoBidpPlanProjectDetailView.publicDate` | `bidPoBidpPlanProjectDetailView.publicDate`: **100.0%** (Null 0.0%) | parse_date(root.publicDate) | Ngày đăng tải KHLCNT (Khóa phân vùng Iceberg). |
| 20 | `silver.plan_revision` | `status_raw` | `VARCHAR` | `$.payload.bidPoBidpPlanProjectDetailView.status` | `bidPoBidpPlanProjectDetailView.status`: **100.0%** (Null 0.0%) | clean_text(root.status) | Trạng thái gốc của KHLCNT. |
| 21 | `silver.plan_revision` | `root_json` | `TEXT` | `$.payload.bidPoBidpPlanProjectDetailView` | `bidPoBidpPlanProjectDetailView`: **100.0%** (Null 0.0%) | canonical_json(root) | Bảo toàn nguyên vẹn JSON của `bidPoBidpPlanProjectDetailView`. |

### 3.3. Gói Thầu Trong KHLCNT (`khlcnt_bid_package_detail` → `silver.package_revision`, `silver.package_location`, `silver.package_lot`)

- **Bảng Bronze nguồn:** `khlcnt_bid_package_detail`
- **Các bảng Silver đích:** `silver.package_revision`, `silver.package_location`, `silver.package_lot`
- **Cơ chế chuẩn hóa:** Chuẩn hóa thông tin Gói thầu thuộc KHLCNT từ payload phẳng cấp root và phân rã 2 mảng con: `bidLocation[]` thành bảng `silver.package_location` và `bidpBidLotList[]` thành bảng `silver.package_lot`.

| STT | Bảng Silver Đích (`Target Table`) | Cột Silver (`Target Column`) | Kiểu Dữ Liệu Silver | Trường Nguồn Bronze (`Bronze JSON Path`) | Độ Đầy Đủ Thực Nghiệm (`Empirical Coverage`) | Quy Tắc Chuyển Đổi & Làm Sạch (`Transformation & Cleaning Rule`) | Ý Nghĩa Nghiệp Vụ |
| :---: | :--- | :--- | :---: | :--- | :--- | :--- | :--- |
| 1 | `silver.package_revision` | `revision_id` | `VARCHAR (PK)` | `$.source_id`<br>`$.source_version`<br>`$.content_hash` | `$.source_id`: **100.0%** (Null 0.0%)<br>`$.source_version`: **0.0%** (Null 100.0%)<br>`$.content_hash`: **100.0%** (Null 0.0%) | SHA-256(['package', entity_id, source_version, root_hash]) | Khóa chính định danh phiên bản gói thầu. |
| 2 | `silver.package_revision` | `entity_id` | `VARCHAR` | `$.payload.id`<br>`$.source_id` | `id`: **100.0%** (Null 0.0%)<br>`$.source_id`: **100.0%** (Null 0.0%) | SHA-256(['package', 'uuid', _valid_reference(payload.id)]) | Khóa định danh thực thể gói thầu. |
| 3 | `silver.package_revision` | `entity_type` | `VARCHAR` | `$.payload.id` | `id`: **100.0%** (Null 0.0%) | Hằng số 'package' | Phân loại thực thể Gói thầu. |
| 4 | `silver.package_revision` | `observation_id` | `VARCHAR (FK)` | `$._dlt_id`<br>`$.content_hash` | `$._dlt_id`: **100.0%** (Null 0.0%)<br>`$.content_hash`: **100.0%** (Null 0.0%) | SHA-256([file_sha256, row_ordinal, content_hash]) | Khóa ngoại truy vết bản ghi Bronze gốc. |
| 5 | `silver.package_revision` | `is_current` | `BOOLEAN` | `$.source_version` | `$.source_version`: **0.0%** (Null 100.0%) | Release Resolver (SCD Type 2 flag) | Đánh dấu phiên bản hiện hành mới nhất của gói thầu. |
| 6 | `silver.package_revision` | `package_no` | `VARCHAR` | `$.payload.bidNo` | `bidNo`: **100.0%** (Null 0.0%) | clean_text(payload.bidNo) | Mã gói thầu (VD: BP2400012345). |
| 7 | `silver.package_revision` | `title` | `VARCHAR` | `$.payload.bidName` | `bidName`: **100.0%** (Null 0.0%) | clean_text(payload.bidName) | Tên gói thầu trong kế hoạch. |
| 8 | `silver.package_revision` | `plan_no` | `VARCHAR` | `$.payload.planNo` | `planNo`: **100.0%** (Null 0.0%) | _valid_reference(payload.planNo) (lọc 'N/A', 'NULL') | Mã số KHLCNT cha. |
| 9 | `silver.package_revision` | `plan_id` | `VARCHAR (FK)` | `$.payload.planId`<br>`$.payload.planNo` | `planId`: **100.0%** (Null 0.0%)<br>`planNo`: **100.0%** (Null 0.0%) | Reference Resolver phân giải từ `planId` (uuid) hoặc `planNo` (number) | Khóa ngoại trỏ về `silver.plan_revision.entity_id`. |
| 10 | `silver.package_revision` | `bid_price` | `DECIMAL(38,6)` | `$.payload.bidPrice` | `bidPrice`: **100.0%** (Null 0.0%) | parse_money(payload.bidPrice) -> warn 'invalid_money' nếu lỗi | Giá gói thầu được phê duyệt trong KHLCNT. |
| 11 | `silver.package_revision` | `currency` | `VARCHAR` | `$.payload.bidPriceUnit` | `bidPriceUnit`: **100.0%** (Null 0.0%) | clean_text(payload.bidPriceUnit) | Đơn vị tiền tệ của giá gói thầu. |
| 12 | `silver.package_revision` | `process_apply` | `VARCHAR` | `$.payload.processApply` | `processApply`: **100.0%** (Null 0.0%) | clean_text(payload.processApply) | Quy trình áp dụng (LDT: Luật Đấu thầu, KHAC: Vốn khác/ADB/WB). |
| 13 | `silver.package_revision` | `bid_field` | `VARCHAR` | `$.payload.bidField` | `bidField`: **100.0%** (Null 0.0%) | clean_text(payload.bidField) | Lĩnh vực đấu thầu (HH, XL, TV, PTV, HON_HOP). |
| 14 | `silver.package_revision` | `bid_form` | `VARCHAR` | `$.payload.bidForm` | `bidForm`: **100.0%** (Null 0.0%) | clean_text(payload.bidForm) | Hình thức lựa chọn nhà thầu (DTRR, CHCT, CDT, ...). |
| 15 | `silver.package_revision` | `bid_mode` | `VARCHAR` | `$.payload.bidMode` | `bidMode`: **100.0%** (Null 0.0%) | clean_text(payload.bidMode) | Phương thức LCNT (1_MTHS: 1 giai đoạn 1 túi, 1_HTHS: 1 giai đoạn 2 túi, ...). |
| 16 | `silver.package_revision` | `capital_detail` | `VARCHAR` | `$.payload.capitalDetail` | `capitalDetail`: **100.0%** (Null 0.0%) | clean_text(payload.capitalDetail) | Chi tiết nguồn vốn thực hiện gói thầu. |
| 17 | `silver.package_revision` | `contract_type` | `VARCHAR` | `$.payload.ctype` | `ctype`: **100.0%** (Null 0.0%) | clean_text(payload.ctype) | Loại hợp đồng (TG: Trọn gói, DGCD: Đơn giá cố định, DGDC: Đơn giá điều chỉnh). |
| 18 | `silver.package_revision` | `execution_period` | `BIGINT` | `$.payload.cperiod` | `cperiod`: **100.0%** (Null 0.0%) | _nonnegative_int(payload.cperiod) | Thời gian thực hiện hợp đồng / gói thầu. |
| 19 | `silver.package_revision` | `execution_period_unit` | `VARCHAR` | `$.payload.cperiodUnit` | `cperiodUnit`: **100.0%** (Null 0.0%) | clean_text(payload.cperiodUnit) | Đơn vị thời gian thực hiện (D: Ngày, M: Tháng). |
| 20 | `silver.package_revision` | `is_domestic` | `BOOLEAN` | `$.payload.isDomestic` | `isDomestic`: **100.0%** (Null 0.0%) | parse_boolean(payload.isDomestic) (1/0 -> True/False) | Cờ đấu thầu trong nước (`true`) hay quốc tế (`false`). |
| 21 | `silver.package_revision` | `is_internet` | `BOOLEAN` | `$.payload.isInternet` | `isInternet`: **100.0%** (Null 0.0%) | parse_boolean(payload.isInternet) (1/0 -> True/False) | Cờ đấu thầu qua mạng (`true`) hay không qua mạng (`false`). |
| 22 | `silver.package_revision` | `bid_start_unit` | `VARCHAR` | `$.payload.bidStartUnit` | `bidStartUnit`: **100.0%** (Null 0.0%) | clean_text(payload.bidStartUnit) | Đơn vị thời gian bắt đầu tổ chức LCNT (Q: Quý, M: Tháng). |
| 23 | `silver.package_revision` | `bid_start_year` | `BIGINT` | `$.payload.bidStartYear` | `bidStartYear`: **100.0%** (Null 0.0%) | _positive_year(payload.bidStartYear) (lọc sentinel 0 -> NULL) | Năm dự kiến bắt đầu tổ chức lựa chọn nhà thầu. |
| 24 | `silver.package_revision` | `bid_start_quarter` | `VARCHAR` | `$.payload.bidStartQuarter` | `bidStartQuarter`: **87.0%** (Null 13.0%) | _quarter(payload.bidStartQuarter) (lọc sentinel '0' -> NULL) | Quý dự kiến bắt đầu tổ chức LCNT (I, II, III, IV). |
| 25 | `silver.package_revision` | `bid_start_month` | `BIGINT` | `$.payload.bidStartMonth` | `bidStartMonth`: **100.0%** (Null 0.0%) | _month(payload.bidStartMonth) (lọc sentinel 0 -> NULL; kiểm tra 1..12) | Tháng dự kiến bắt đầu tổ chức LCNT (1..12). |
| 26 | `silver.package_revision` | `public_date` | `DATE` | `$.payload.createdDate`<br>`$.payload.originalCreateDate`<br>`$.payload.exchangeDate` | `createdDate`: **100.0%** (Null 0.0%)<br>`originalCreateDate`: **100.0%** (Null 0.0%)<br>`exchangeDate`: **100.0%** (Null 0.0%) | parse_date(createdDate ?: originalCreateDate ?: exchangeDate) | Ngày tạo/công bố gói thầu (Khóa phân vùng Iceberg). |
| 27 | `silver.package_revision` | `root_json` | `TEXT` | `$.payload` | `$.payload`: **100.0%** (Null 0.0%) | canonical_json(payload) | Bảo toàn nguyên vẹn JSON gói thầu. |
| 28 | `silver.package_location` | `location_id` | `VARCHAR (PK)` | `$.payload.bidLocation[].id` | `bidLocation[].id`: **100.0%** (Null 0.0%) | SHA-256([revision_id, 'location', ordinal]) | Khóa chính địa điểm thực hiện gói thầu. |
| 29 | `silver.package_location` | `revision_id` | `VARCHAR (FK)` | `$.payload.id` | `id`: **100.0%** (Null 0.0%) | FK -> silver.package_revision.revision_id | Khóa ngoại trỏ về phiên bản gói thầu. |
| 30 | `silver.package_location` | `location_ordinal` | `BIGINT` | `$.payload.bidLocation` | `bidLocation`: **100.0%** (Null 0.0%) | Chỉ số phần tử (0, 1, ...) trong mảng `bidLocation[]` | Thứ tự địa điểm trong danh sách. |
| 31 | `silver.package_location` | `province_code` | `VARCHAR` | `$.payload.bidLocation[].provCode` | `bidLocation[].provCode`: **100.0%** (Null 0.0%) | clean_pipe_code(location.provCode) | Mã tỉnh/thành phố thực hiện gói thầu. |
| 32 | `silver.package_location` | `province_name` | `VARCHAR` | `$.payload.bidLocation[].provName` | `bidLocation[].provName`: **100.0%** (Null 0.0%) | clean_text(location.provName) | Tên tỉnh/thành phố thực hiện gói thầu. |
| 33 | `silver.package_location` | `district_code` | `VARCHAR` | `$.payload.bidLocation[].districtCode` | `bidLocation[].districtCode`: **0.0%** (Null 100.0%) | clean_pipe_code(location.districtCode) | Mã quận/huyện thực hiện gói thầu. |
| 34 | `silver.package_location` | `district_name` | `VARCHAR` | `$.payload.bidLocation[].districtName` | `bidLocation[].districtName`: **0.0%** (Null 100.0%) | clean_text(location.districtName) | Tên quận/huyện thực hiện gói thầu. |
| 35 | `silver.package_location` | `ward_code` | `VARCHAR` | `$.payload.bidLocation[].wardCode` | `bidLocation[].wardCode`: **0.0%** (Null 100.0%) | clean_pipe_code(location.wardCode) | Mã phường/xã thực hiện gói thầu. |
| 36 | `silver.package_location` | `ward_name` | `VARCHAR` | `$.payload.bidLocation[].wardName` | `bidLocation[].wardName`: **0.0%** (Null 100.0%) | clean_text(location.wardName) | Tên phường/xã thực hiện gói thầu. |
| 37 | `silver.package_lot` | `lot_id` | `VARCHAR (PK)` | `$.payload.bidpBidLotList[].id` | `bidpBidLotList[].id`: **99.6%** (Null 0.0%) | SHA-256([revision_id, 'lot', ordinal, source_lot_id]) | Khóa chính phần/lô thuộc gói thầu. |
| 38 | `silver.package_lot` | `revision_id` | `VARCHAR (FK)` | `$.payload.id` | `id`: **100.0%** (Null 0.0%) | FK -> silver.package_revision.revision_id | Khóa ngoại trỏ về phiên bản gói thầu. |
| 39 | `silver.package_lot` | `source_lot_id` | `VARCHAR` | `$.payload.bidpBidLotList[].id` | `bidpBidLotList[].id`: **99.6%** (Null 0.0%) | _valid_reference(lot.id) | ID gốc của phần/lô trên hệ thống e-GP. |
| 40 | `silver.package_lot` | `lot_ordinal` | `BIGINT` | `$.payload.bidpBidLotList` | `bidpBidLotList`: **99.6%** (Null 0.4%) | Chỉ số phần tử (0, 1, ...) trong mảng `bidpBidLotList[]` | Thứ tự phần/lô trong gói thầu. |
| 41 | `silver.package_lot` | `lot_no` | `VARCHAR` | `$.payload.bidpBidLotList[].lotNo`<br>`$.payload.bidpBidLotList[].bidNo` | `bidpBidLotList[].lotNo`: **99.6%** (Null 0.0%)<br>`bidpBidLotList[].bidNo`: **99.6%** (Null 0.0%) | clean_text(lot.lotNo) | Mã số phần/lô. |
| 42 | `silver.package_lot` | `lot_name` | `VARCHAR` | `$.payload.bidpBidLotList[].lotName`<br>`$.payload.bidpBidLotList[].bidName` | `bidpBidLotList[].lotName`: **99.6%** (Null 0.0%)<br>`bidpBidLotList[].bidName`: **99.6%** (Null 0.0%) | clean_text(lot.lotName) | Tên phần/lô hoặc danh mục hàng hóa/thuốc. |
| 43 | `silver.package_lot` | `lot_price` | `DECIMAL(38,6)` | `$.payload.bidpBidLotList[].lotPrice` | `bidpBidLotList[].lotPrice`: **99.6%** (Null 0.0%) | parse_money(lot.lotPrice) | Giá dự toán của phần/lô. |
| 44 | `silver.package_lot` | `currency` | `VARCHAR` | `$.payload.bidpBidLotList[].lotPriceUnit`<br>`$.payload.bidPriceUnit` | `bidPriceUnit`: **100.0%** (Null 0.0%) | clean_text(lot.lotPriceUnit) ?: clean_text(payload.bidPriceUnit) | Đơn vị tiền tệ của phần/lô. |
| 45 | `silver.package_lot` | `medicine_code` | `VARCHAR` | `$.payload.bidpBidLotList[].medicineCode` | `bidpBidLotList[].medicineCode`: **0.0%** (Null 99.6%) | clean_text(lot.medicineCode) | Mã hoạt chất / mã thuốc (đối với gói thầu dược phẩm/y tế). |
| 46 | `silver.package_lot` | `quantity` | `DECIMAL(38,6)` | `$.payload.bidpBidLotList[].quantity` | `bidpBidLotList[].quantity`: **0.0%** (Null 99.6%) | parse_money(lot.quantity) | Số lượng hàng hóa / định lượng mời thầu. |
| 47 | `silver.package_lot` | `uom` | `VARCHAR` | `$.payload.bidpBidLotList[].uom` | `bidpBidLotList[].uom`: **0.0%** (Null 99.6%) | clean_text(lot.uom) | Đơn vị tính (viên, lọ, hộp, cái, ...). |
| 48 | `silver.package_lot` | `planned_unit_price` | `DECIMAL(38,6)` | `$.payload.bidpBidLotList[].pricePlan` | `bidpBidLotList[].pricePlan`: **0.0%** (Null 99.6%) | parse_money(lot.pricePlan) | Đơn giá kế hoạch của mặt hàng trong lô. |

### 3.4. Thông Báo Mời Thầu (`notify_contractor_*` → `silver.notice_revision`, `silver.notice_lot`, `silver.notice_location`, `silver.notice_reoffer_submission`)

- **Bảng Bronze nguồn:** `notify_contractor_standard_detail`, `notify_contractor_vk_adb_detail`, `notify_contractor_reoffer_detail`
- **Các bảng Silver đích:** `silver.notice_revision`, `silver.notice_lot`, `silver.notice_location`, `silver.notice_reoffer_submission`
- **Cơ chế chuẩn hóa:** Hợp nhất 3 dòng dữ liệu Thông báo mời thầu: Standard (`bidoNotifyContractorM` / `bidNoContractorResponse.bidNotification`), VK/ADB (`bidoNotifyContractorP`), và Reoffer (`$` top-level) về mô hình đích chung `silver.notice_revision` (contract `notice-2`). Kế thừa có kiểm chứng các thuộc tính khuyết từ `bidpPlanDetail` / `bidDetail` và `bidoBidStatus`.

| STT | Bảng Silver Đích (`Target Table`) | Cột Silver (`Target Column`) | Kiểu Dữ Liệu Silver | Trường Nguồn Bronze (`Bronze JSON Path`) | Độ Đầy Đủ Thực Nghiệm (`Empirical Coverage`) | Quy Tắc Chuyển Đổi & Làm Sạch (`Transformation & Cleaning Rule`) | Ý Nghĩa Nghiệp Vụ |
| :---: | :--- | :--- | :---: | :--- | :--- | :--- | :--- |
| 1 | `silver.notice_revision` | `revision_id` | `VARCHAR (PK)` | `$.source_id`<br>`$.source_version`<br>`$.content_hash` | `$.source_id`: **100.0%** (Null 0.0%)<br>`$.source_version`: **100.0%** (Null 0.0%)<br>`$.content_hash`: **100.0%** (Null 0.0%) | SHA-256(['notice', entity_id, source_version, root_hash]) | Khóa chính định danh phiên bản TBMT. |
| 2 | `silver.notice_revision` | `entity_id` | `VARCHAR` | `$.payload.bidoNotifyContractorM.notifyNo`<br>`$.payload.bidoNotifyContractorP.notifyNo`<br>`$.payload.reofferNo` | `bidoNotifyContractorM.notifyNo`: **100.0%** (Null 0.0%) | SHA-256(['notice', id_scheme, notifyNo | reofferNo]) | Khóa thực thể TBMT xuyên suốt các lần sửa đổi / làm rõ (`00`, `01`, ...). |
| 3 | `silver.notice_revision` | `source_table / source_family` | `VARCHAR` | `$.source_id` | `$.source_id`: **100.0%** (Null 0.0%) | Ghi nhận bảng nguồn & dòng dữ liệu ('standard', 'vk_adb', 'reoffer') | Phân biệt nguồn gốc biểu mẫu TBMT. |
| 4 | `silver.notice_revision` | `source_document_id` | `VARCHAR` | `$.payload.bidoNotifyContractorM.id`<br>`$.payload.bidoNotifyContractorP.id`<br>`$.payload.id` | `bidoNotifyContractorM.id`: **100.0%** (Null 0.0%) | clean_text(<root>.id) | UUID bản ghi gốc của thông báo trên hệ thống e-GP. |
| 5 | `silver.notice_revision` | `notify_no / business_number` | `VARCHAR` | `$.payload.bidoNotifyContractorM.notifyNo`<br>`$.payload.bidoNotifyContractorP.notifyNo`<br>`$.payload.notifyNo` | `bidoNotifyContractorM.notifyNo`: **100.0%** (Null 0.0%) | clean_text(<root>.notifyNo) (kiểm tra đối chiếu với $.source_id) | Mã TBMT định danh quốc gia (VD: IB2400012345). |
| 6 | `silver.notice_revision` | `notify_version` | `VARCHAR` | `$.payload.bidoNotifyContractorM.notifyVersion`<br>`$.payload.bidoNotifyContractorP.notifyVersion`<br>`$.source_version` | `bidoNotifyContractorM.notifyVersion`: **100.0%** (Null 0.0%)<br>`$.source_version`: **100.0%** (Null 0.0%) | clean_text(<root>.notifyVersion) (kiểm tra đối chiếu với $.source_version) | Phiên bản TBMT (`00`: gốc, `01`+: sửa đổi). |
| 7 | `silver.notice_revision` | `reoffer_no / reoffer_version` | `VARCHAR` | `$.payload.reofferNo`<br>`$.payload.reofferVersion` | `reofferNo`: **100.0%** (Null 0.0%)<br>`reofferVersion`: **100.0%** (Null 0.0%) | clean_text(payload.reofferNo), clean_text(payload.reofferVersion) | Mã và phiên bản thông báo chào giá lại (chỉ có ở bảng Reoffer). |
| 8 | `silver.notice_revision` | `package_id` | `VARCHAR` | `$.payload.bidoNotifyContractorM.bidId`<br>`$.payload.bidoNotifyContractorP.bidId`<br>`$.payload.bidId` | `bidoNotifyContractorM.bidId`: **100.0%** (Null 0.0%) | clean_text(<root>.bidId) | UUID gói thầu liên kết trực tiếp với KHLCNT. |
| 9 | `silver.notice_revision` | `package_no` | `VARCHAR` | `$.payload.bidoNotifyContractorM.bidNo`<br>`$.payload.bidoNotifyContractorP.bidNo`<br>`$.payload.bidNo` | `bidoNotifyContractorM.bidNo`: **100.0%** (Null 0.0%) | clean_text(<root>.bidNo) | Mã số gói thầu. |
| 10 | `silver.notice_revision` | `title` | `VARCHAR` | `$.payload.bidoNotifyContractorM.bidName`<br>`$.payload.bidoNotifyContractorP.bidName`<br>`$.payload.bidName` | `bidoNotifyContractorM.bidName`: **100.0%** (Null 0.0%) | clean_text(<root>.bidName) | Tên gói thầu mời thầu. |
| 11 | `silver.notice_revision` | `plan_id / plan_version` | `VARCHAR` | `$.payload.bidpPlanDetail.planId`<br>`$.payload.bidpPlanDetail.planVersion`<br>`$.payload.bidDetail.planId` | `bidpPlanDetail.planId`: **100.0%** (Null 0.0%)<br>`bidpPlanDetail.planVersion`: **100.0%** (Null 0.0%) | Chỉ gán khi `bidpPlanDetail.id == package_id` (ngược lại phát cảnh báo `unverified_embedded_package`) | ID và phiên bản KHLCNT trích xuất từ khối kế hoạch nhúng đã xác thực. |
| 12 | `silver.notice_revision` | `plan_no / plan_name / plan_type_raw` | `VARCHAR` | `$.payload.bidoNotifyContractorM.planNo`<br>`$.payload.bidoNotifyContractorM.planName`<br>`$.payload.bidoNotifyContractorM.planType` | `bidoNotifyContractorM.planNo`: **100.0%** (Null 0.0%)<br>`bidoNotifyContractorM.planName`: **100.0%** (Null 0.0%)<br>`bidoNotifyContractorM.planType`: **100.0%** (Null 0.0%) | clean_text(<root>.planNo / planName / planType) | Mã, tên và phân loại kế hoạch lựa chọn nhà thầu. |
| 13 | `silver.notice_revision` | `project_name` | `VARCHAR` | `$.payload.bidoNotifyContractorM.projectName`<br>`$.payload.bidoNotifyContractorP.projectName` | `bidoNotifyContractorM.projectName`: **99.5%** (Null 0.5%) | clean_text(<root>.projectName) | Tên dự án đầu tư ghi trên TBMT. |
| 14 | `silver.notice_revision` | `procuring_entity_code / buyer_id` | `VARCHAR` | `$.payload.bidoNotifyContractorM.procuringEntityCode`<br>`$.payload.bidoNotifyContractorP.procuringEntityCode`<br>`$.payload.procuringEntityCode` | `bidoNotifyContractorM.procuringEntityCode`: **100.0%** (Null 0.0%) | clean_text(<root>.procuringEntityCode) | Mã định danh Bên mời thầu. |
| 15 | `silver.notice_revision` | `procuring_entity_name / buyer_name` | `VARCHAR` | `$.payload.bidoNotifyContractorM.procuringEntityName`<br>`$.payload.bidoNotifyContractorP.procuringEntityName`<br>`$.payload.procuringEntityName` | `bidoNotifyContractorM.procuringEntityName`: **100.0%** (Null 0.0%) | clean_text(<root>.procuringEntityName) | Tên pháp nhân Bên mời thầu. |
| 16 | `silver.notice_revision` | `investor_code / investor_name` | `VARCHAR` | `$.payload.bidoNotifyContractorM.investorCode`<br>`$.payload.bidoNotifyContractorM.investorName`<br>`$.payload.bidoNotifyContractorP.investorName` | `bidoNotifyContractorM.investorCode`: **100.0%** (Null 0.0%)<br>`bidoNotifyContractorM.investorName`: **100.0%** (Null 0.0%) | clean_text(<root>.investorCode / investorName) | Mã và tên Chủ đầu tư. |
| 17 | `silver.notice_revision` | `bid_field` | `VARCHAR` | `$.payload.bidoNotifyContractorM.investField`<br>`$.payload.bidpPlanDetail.bidField`<br>`$.payload.bidoNotifyContractorP.investField` | `bidoNotifyContractorM.investField`: **100.0%** (Null 0.0%)<br>`bidpPlanDetail.bidField`: **100.0%** (Null 0.0%) | clean_text(<root>.investField) ?: clean_text(bidpPlanDetail.bidField) | Lĩnh vực đấu thầu (có cơ chế fallback sang `bidpPlanDetail.bidField`). |
| 18 | `silver.notice_revision` | `bid_form / bid_mode / process_apply_raw` | `VARCHAR` | `$.payload.bidoNotifyContractorM.bidForm`<br>`$.payload.bidoNotifyContractorM.bidMode`<br>`$.payload.bidoNotifyContractorM.processApply` | `bidoNotifyContractorM.bidForm`: **100.0%** (Null 0.0%)<br>`bidoNotifyContractorM.bidMode`: **100.0%** (Null 0.0%)<br>`bidoNotifyContractorM.processApply`: **100.0%** (Null 0.0%) | clean_text(<root>.bidForm / bidMode / processApply) | Hình thức, phương thức và quy trình áp dụng đấu thầu. |
| 19 | `silver.notice_revision` | `bid_price / amount` | `DECIMAL(38,6)` | `$.payload.bidoNotifyContractorM.bidPrice`<br>`$.payload.bidoNotifyContractorP.bidPrice` | `bidoNotifyContractorM.bidPrice`: **100.0%** (Null 0.0%) | parse_money(<root>.bidPrice) | Giá gói thầu công bố trong TBMT. |
| 20 | `silver.notice_revision` | `estimate_price` | `DECIMAL(38,6)` | `$.payload.bidoNotifyContractorM.bidEstimatePrice`<br>`$.payload.bidoNotifyContractorP.bidEstimatePrice` | `bidoNotifyContractorM.bidEstimatePrice`: **50.4%** (Null 49.6%) | parse_money(<root>.bidEstimatePrice) | Dự toán gói thầu được duyệt (nếu cập nhật sau KHLCNT). |
| 21 | `silver.notice_revision` | `guarantee_amount / guarantee_form` | `DECIMAL(38,6) / VARCHAR` | `$.payload.bidoNotifyContractorM.guaranteeValue`<br>`$.payload.bidoNotifyContractorM.guaranteeForm`<br>`$.payload.bidoNotifyContractorP.bidGuaranteeValue`<br>`$.payload.bidoNotifyContractorP.bidGuaranteeForm` | `bidoNotifyContractorM.guaranteeValue`: **74.9%** (Null 25.1%)<br>`bidoNotifyContractorM.guaranteeForm`: **100.0%** (Null 0.0%) | Standard dùng `guaranteeValue`/`guaranteeForm`; VK-ADB dùng `bidGuaranteeValue`/`bidGuaranteeForm` | Số tiền và hình thức bảo đảm dự thầu. |
| 22 | `silver.notice_revision` | `initial_price / price_step` | `DECIMAL(38,6)` | `$.payload.priceInit`<br>`$.payload.priceStep` | `priceInit`: **93.2%** (Null 6.8%)<br>`priceStep`: **77.6%** (Null 22.4%) | parse_money(payload.priceInit), parse_money(payload.priceStep) | Giá khởi điểm và bước giá trong chào giá trực tuyến (Reoffer). |
| 23 | `silver.notice_revision` | `contract_type / contract_duration_raw / unit` | `VARCHAR` | `$.payload.bidoNotifyContractorM.contractType`<br>`$.payload.bidoNotifyContractorM.contractPeriod`<br>`$.payload.bidoNotifyContractorP.cType`<br>`$.payload.bidoNotifyContractorP.cPeriod`<br>`$.payload.bidpPlanDetail.cperiod` | `bidoNotifyContractorM.contractType`: **100.0%** (Null 0.0%)<br>`bidoNotifyContractorM.contractPeriod`: **100.0%** (Null 0.0%)<br>`bidpPlanDetail.cperiod`: **100.0%** (Null 0.0%) | Standard: `contractType`/`contractPeriod`; VK-ADB: `cType`/`cPeriod`; fallback `bidpPlanDetail.cperiod` | Loại hợp đồng và thời gian thực hiện hợp đồng. |
| 24 | `silver.notice_revision` | `published_at / public_date` | `TIMESTAMPTZ / DATE` | `$.payload.bidoNotifyContractorM.publicDate`<br>`$.payload.bidoNotifyContractorP.publicDate`<br>`$.payload.publicDate` | `bidoNotifyContractorM.publicDate`: **100.0%** (Null 0.0%) | parse_iso_datetime(<root>.publicDate) & parse_date(<root>.publicDate) | Thời điểm đăng tải TBMT và ngày phân vùng Iceberg. |
| 25 | `silver.notice_revision` | `bid_open_date / bid_close_date` | `TIMESTAMPTZ` | `$.payload.bidoNotifyContractorM.bidOpenDate`<br>`$.payload.bidoNotifyContractorM.bidCloseDate`<br>`$.payload.bidoNotifyContractorP.bidOpenDate`<br>`$.payload.bidoNotifyContractorP.bidCloseDate` | `bidoNotifyContractorM.bidOpenDate`: **100.0%** (Null 0.0%)<br>`bidoNotifyContractorM.bidCloseDate`: **100.0%** (Null 0.0%) | parse_iso_datetime(bidOpenDate / bidCloseDate); kiểm tra published_at <= bid_close_date <= bid_open_date | Thời điểm đóng thầu và mở thầu. |
| 26 | `silver.notice_revision` | `is_internet / is_domestic / is_prequalification` | `BOOLEAN` | `$.payload.bidoNotifyContractorM.isInternet`<br>`$.payload.bidoNotifyContractorM.isDomestic`<br>`$.payload.bidoNotifyContractorM.isPrequalification` | `bidoNotifyContractorM.isInternet`: **100.0%** (Null 0.0%)<br>`bidoNotifyContractorM.isDomestic`: **100.0%** (Null 0.0%)<br>`bidoNotifyContractorM.isPrequalification`: **0.0%** (Null 100.0%) | parse_boolean(<root>.isInternet / isDomestic / isPrequalification) | Các cờ thuộc tính đấu thầu qua mạng, trong nước và sơ tuyển. |
| 27 | `silver.notice_revision` | `is_multi_lot` | `BOOLEAN` | `$.payload.bidoNotifyContractorM.isMultiLot`<br>`$.payload.bidpPlanDetail.isMultiLot`<br>`$.payload.bidoBidStatus.isMultiLot` | `bidoNotifyContractorM.isMultiLot`: **0.0%** (Null 100.0%)<br>`bidpPlanDetail.isMultiLot`: **100.0%** (Null 0.0%)<br>`bidoBidStatus.isMultiLot`: **81.7%** (Null 18.3%) | parse_boolean(<root>.isMultiLot ?: bidpPlanDetail.isMultiLot ?: bidoBidStatus.isMultiLot) | Cờ gói thầu chia nhiều phần/lô (được cứu 100% nhờ chuỗi fallback 3 tầng). |
| 28 | `silver.notice_lot` | `lot_no / lot_name / lot_price / estimate_price / guarantee_amount` | `VARCHAR / DECIMAL(38,6)` | `$.payload.bidNoContractorResponse.bidNotification.lotDTOList[]`<br>`$.payload.bidoNotifyContractorM.lotDTOList[]`<br>`$.payload.bidoNotifyContractorP.lotDTOList[]` | `bidNoContractorResponse.bidNotification.lotDTOList[]`: **100.0%** (Null 0.0%) | Unnest `lotDTOList[]`; parse_money cho `lotPrice`, `lotEstimatePrice`, `lotGuaranteeValue` | Bảng con chi tiết từng phần/lô trong TBMT. |
| 29 | `silver.notice_location` | `province_code / province_name / district_code / district_name / ward_code / ward_name` | `VARCHAR` | `$.payload.bidpBidLocationList[]`<br>`$.payload.lsBidpBidLocationDTO[]`<br>`$.payload.bidDetail.bidLocation[]` | `bidpBidLocationList[]`: **76.8%** (Null 0.0%) | Unnest mảng địa điểm; áp dụng `clean_pipe_code` cho các cột mã và `clean_text` cho các cột tên | Bảng con địa điểm thực hiện gói thầu trong TBMT. |
| 30 | `silver.notice_reoffer_submission` | `submission_id / contractor_code / price / final_price / offered_at` | `VARCHAR / DECIMAL(38,6) / TIMESTAMPTZ` | `$.payload.contractorsResult[]`<br>`$.payload.contractorsResultAll[]` | `contractorsResult[]`: **93.1%** (Null 0.0%)<br>`contractorsResultAll[]`: **93.1%** (Null 0.0%) | Unnest lịch sử chào giá lại; parse_money cho `reofferPrice`, `reofferPriceFinal`; parse_iso_datetime cho `reofferDate` | Bảng con lưu toàn bộ các lượt chào giá trực tuyến trong gói Reoffer. |

### 3.5. Kết Quả Lựa Chọn Nhà Thầu (`contractor_result_detail` → `silver.result_revision`, `silver.award_lot`, `silver.award_contractor`)

- **Bảng Bronze nguồn:** `contractor_result_detail`
- **Các bảng Silver đích:** `silver.result_revision`, `silver.award_lot`, `silver.award_contractor`
- **Cơ chế chuẩn hóa:** Chuẩn hóa Kết quả lựa chọn nhà thầu (KQLCNT) theo cấu trúc phân cấp 3 tầng: Quyết định KQLCNT (`bideContractorInputResultDTO`) → Kết quả theo Lô (`lotResultDTO[]` → `silver.award_lot`) → Danh sách Nhà thầu dự thầu & trúng thầu (`contractorList[]` → `silver.award_contractor`).

| STT | Bảng Silver Đích (`Target Table`) | Cột Silver (`Target Column`) | Kiểu Dữ Liệu Silver | Trường Nguồn Bronze (`Bronze JSON Path`) | Độ Đầy Đủ Thực Nghiệm (`Empirical Coverage`) | Quy Tắc Chuyển Đổi & Làm Sạch (`Transformation & Cleaning Rule`) | Ý Nghĩa Nghiệp Vụ |
| :---: | :--- | :--- | :---: | :--- | :--- | :--- | :--- |
| 1 | `silver.result_revision` | `revision_id` | `VARCHAR (PK)` | `$.source_id`<br>`$.source_version`<br>`$.content_hash` | `$.source_id`: **100.0%** (Null 0.0%)<br>`$.source_version`: **100.0%** (Null 0.0%)<br>`$.content_hash`: **100.0%** (Null 0.0%) | SHA-256(['result', entity_id, source_version, root_hash]) | Khóa chính định danh phiên bản KQLCNT. |
| 2 | `silver.result_revision` | `entity_id` | `VARCHAR` | `$.payload.bideContractorInputResultDTO.id`<br>`$.source_id` | `bideContractorInputResultDTO.id`: **100.0%** (Null 0.0%)<br>`$.source_id`: **100.0%** (Null 0.0%) | SHA-256(['result', 'uuid', clean_text(root.id)]) | Khóa định danh thực thể KQLCNT. |
| 3 | `silver.result_revision` | `result_id / result_version` | `VARCHAR` | `$.payload.bideContractorInputResultDTO.id`<br>`$.payload.bideContractorInputResultDTO.resultVersion` | `bideContractorInputResultDTO.id`: **100.0%** (Null 0.0%)<br>`bideContractorInputResultDTO.resultVersion`: **100.0%** (Null 0.0%) | clean_text(root.id), clean_text(root.resultVersion) | UUID và phiên bản của hồ sơ KQLCNT. |
| 4 | `silver.result_revision` | `notify_no / notify_version` | `VARCHAR` | `$.payload.bideContractorInputResultDTO.notifyNo`<br>`$.payload.bideContractorInputResultDTO.notifyVersion` | `bideContractorInputResultDTO.notifyNo`: **100.0%** (Null 0.0%)<br>`bideContractorInputResultDTO.notifyVersion`: **100.0%** (Null 0.0%) | clean_text(root.notifyNo), clean_text(root.notifyVersion) | Mã TBMT và phiên bản TBMT tương ứng (Khóa liên kết sang `notice`). |
| 5 | `silver.result_revision` | `bid_id / package_no` | `VARCHAR` | `$.payload.bideContractorInputResultDTO.bidId`<br>`$.payload.bideContractorInputResultDTO.bidNo` | `bideContractorInputResultDTO.bidId`: **100.0%** (Null 0.0%)<br>`bideContractorInputResultDTO.bidNo`: **100.0%** (Null 0.0%) | clean_text(root.bidId), clean_text(root.bidNo) | UUID và mã gói thầu (Khóa liên kết sang `package`). |
| 6 | `silver.result_revision` | `business_number` | `VARCHAR` | `$.payload.bideContractorInputResultDTO.notifyNo`<br>`$.payload.bideContractorInputResultDTO.bidNo` | `bideContractorInputResultDTO.notifyNo`: **100.0%** (Null 0.0%)<br>`bideContractorInputResultDTO.bidNo`: **100.0%** (Null 0.0%) | clean_text(root.notifyNo) ?: clean_text(root.bidNo) | Mã nghiệp vụ chính của KQLCNT. |
| 7 | `silver.result_revision` | `title` | `VARCHAR` | `$.payload.bideContractorInputResultDTO.bidName` | `bideContractorInputResultDTO.bidName`: **100.0%** (Null 0.0%) | clean_text(root.bidName) | Tên gói thầu được phê duyệt kết quả. |
| 8 | `silver.result_revision` | `procuring_entity_code / buyer_id` | `VARCHAR` | `$.payload.bideContractorInputResultDTO.procuringEntityCode` | `bideContractorInputResultDTO.procuringEntityCode`: **100.0%** (Null 0.0%) | clean_text(root.procuringEntityCode) | Mã Bên mời thầu. |
| 9 | `silver.result_revision` | `procuring_entity_name / buyer_name` | `VARCHAR` | `$.payload.bideContractorInputResultDTO.procuringEntityName` | `bideContractorInputResultDTO.procuringEntityName`: **100.0%** (Null 0.0%) | clean_text(root.procuringEntityName) | Tên Bên mời thầu. |
| 10 | `silver.result_revision` | `investor_code / investor_name` | `VARCHAR` | `$.payload.bideContractorInputResultDTO.investorCode`<br>`$.payload.bideContractorInputResultDTO.investorName` | `bideContractorInputResultDTO.investorCode`: **100.0%** (Null 0.0%)<br>`bideContractorInputResultDTO.investorName`: **100.0%** (Null 0.0%) | clean_text(root.investorCode), clean_text(root.investorName) | Mã và tên Chủ đầu tư. |
| 11 | `silver.result_revision` | `decision_no / decision_agency` | `VARCHAR` | `$.payload.bideContractorInputResultDTO.decisionNo`<br>`$.payload.bideContractorInputResultDTO.decisionAgency` | `bideContractorInputResultDTO.decisionNo`: **100.0%** (Null 0.0%)<br>`bideContractorInputResultDTO.decisionAgency`: **100.0%** (Null 0.0%) | clean_text(root.decisionNo), clean_text(root.decisionAgency) | Số quyết định và cơ quan phê duyệt KQLCNT. |
| 12 | `silver.result_revision` | `decision_date` | `DATE` | `$.payload.bideContractorInputResultDTO.decisionDate` | `bideContractorInputResultDTO.decisionDate`: **100.0%** (Null 0.0%) | parse_date(root.decisionDate); kiểm tra decisionDate <= publicDate | Ngày ký quyết định phê duyệt KQLCNT. |
| 13 | `silver.result_revision` | `total_winning_price / amount` | `DECIMAL(38,6)` | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].bidWiningPrice`<br>`$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].lotFinalPrice`<br>`$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].lotPrice` | `bideContractorInputResultDTO.lotResultDTO[].contractorList[].bidWiningPrice`: **100.0%** (Null 0.0%)<br>`bideContractorInputResultDTO.lotResultDTO[].contractorList[].lotFinalPrice`: **100.0%** (Null 0.0%)<br>`bideContractorInputResultDTO.lotResultDTO[].contractorList[].lotPrice`: **100.0%** (Null 0.0%) | SUM(bidWiningPrice ?: lotFinalPrice ?: lotPrice) của các nhà thầu có `bidResult == 1` | Tổng giá trúng thầu của toàn bộ gói thầu. |
| 14 | `silver.result_revision` | `currency` | `VARCHAR` | `$.payload.bideContractorInputResultDTO.bidPriceUnit`<br>`$.payload.bideContractorInputResultDTO.bidEstimatePriceUnit` | `bideContractorInputResultDTO.bidPriceUnit`: **100.0%** (Null 0.0%)<br>`bideContractorInputResultDTO.bidEstimatePriceUnit`: **100.0%** (Null 0.0%) | clean_text(bidPriceUnit ?: bidEstimatePriceUnit) ?: 'VND' | Đơn vị tiền tệ của kết quả lựa chọn nhà thầu. |
| 15 | `silver.result_revision` | `public_date / public_date_raw` | `DATE / VARCHAR` | `$.payload.bideContractorInputResultDTO.publicDate` | `bideContractorInputResultDTO.publicDate`: **100.0%** (Null 0.0%) | parse_date(root.publicDate) & clean_text(root.publicDate) | Ngày công khai KQLCNT (Khóa phân vùng Iceberg). |
| 16 | `silver.result_revision` | `status_raw` | `VARCHAR` | `$.payload.bideContractorInputResultDTO.status` | `bideContractorInputResultDTO.status`: **100.0%** (Null 0.0%) | clean_text(root.status) | Trạng thái gốc của hồ sơ KQLCNT. |
| 17 | `silver.award_lot` | `award_lot_id` | `VARCHAR (PK)` | `$.payload.bideContractorInputResultDTO.lotResultDTO[].id` | `bideContractorInputResultDTO.lotResultDTO[].id`: **100.0%** (Null 0.0%) | SHA-256([revision_id, 'award_lot', ordinal, lot_id ?: lot_no]) | Khóa chính của phần/lô trúng thầu. |
| 18 | `silver.award_lot` | `lot_no / lot_name` | `VARCHAR` | `$.payload.bideContractorInputResultDTO.lotResultDTO[].lotNo`<br>`$.payload.bideContractorInputResultDTO.lotResultDTO[].lotName` | `bideContractorInputResultDTO.lotResultDTO[].lotNo`: **100.0%** (Null 0.0%)<br>`bideContractorInputResultDTO.lotResultDTO[].lotName`: **100.0%** (Null 0.0%) | clean_text(lotNo ?: bidNo ?: ordinal+1), clean_text(lotName ?: bidName) | Mã lô và tên lô được phê duyệt kết quả. |
| 19 | `silver.award_lot` | `lot_price` | `DECIMAL(38,6)` | `$.payload.bideContractorInputResultDTO.lotResultDTO[].lotPrice` | `bideContractorInputResultDTO.lotResultDTO[].lotPrice`: **100.0%** (Null 0.0%) | parse_money(lotPrice); chặn giá <= 0 (`invalid_zero_price`) và giá giả <= 10 VND (`dummy_placeholder_price`) | Giá gói thầu / dự toán của lô (đã lọc nhiễu giá 1đ, 10đ). |
| 20 | `silver.award_lot` | `winning_price` | `DECIMAL(38,6)` | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].bidWiningPrice` | `bideContractorInputResultDTO.lotResultDTO[].contractorList[].bidWiningPrice`: **100.0%** (Null 0.0%) | Tổng giá trúng thầu của các nhà thầu thắng cuộc (`bidResult == 1`) trong lô; cảnh báo `price_scale_anomaly` nếu lệch > 100x | Giá trúng thầu thực tế của lô. |
| 21 | `silver.award_lot` | `winning_code / status` | `VARCHAR` | `$.payload.bideContractorInputResultDTO.lotResultDTO[].winningCode`<br>`$.payload.bideContractorInputResultDTO.lotResultDTO[].status` | `bideContractorInputResultDTO.lotResultDTO[].winningCode`: **100.0%** (Null 0.0%)<br>`bideContractorInputResultDTO.lotResultDTO[].status`: **100.0%** (Null 0.0%) | clean_text(lot.winningCode), clean_text(str(lot.status)) | Mã trạng thái trao thầu và trạng thái lô. |
| 22 | `silver.award_contractor` | `award_contractor_id / award_lot_id` | `VARCHAR (PK / FK)` | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].id` | `bideContractorInputResultDTO.lotResultDTO[].contractorList[].id`: **100.0%** (Null 0.0%) | SHA-256([revision_id, 'award_contractor', lot_ordinal, c_idx, id ?: orgCode]) | Khóa chính dòng nhà thầu và khóa ngoại trỏ về `silver.award_lot`. |
| 23 | `silver.award_contractor` | `contractor_code` | `VARCHAR` | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].orgCode`<br>`$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].taxCode` | `bideContractorInputResultDTO.lotResultDTO[].contractorList[].orgCode`: **100.0%** (Null 0.0%)<br>`bideContractorInputResultDTO.lotResultDTO[].contractorList[].taxCode`: **98.6%** (Null 1.4%) | clean_text(orgCode ?: taxCode) ?: 'UNKNOWN' | Mã định danh nhà thầu (DKKD / e-GP code). |
| 24 | `silver.award_contractor` | `contractor_name` | `VARCHAR` | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].orgFullname`<br>`$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].newContractorName`<br>`$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].contractorName` | `bideContractorInputResultDTO.lotResultDTO[].contractorList[].orgFullname`: **100.0%** (Null 0.0%)<br>`bideContractorInputResultDTO.lotResultDTO[].contractorList[].newContractorName`: **0.0%** (Null 100.0%) | clean_text(orgFullname ?: newContractorName ?: contractorName) ?: 'UNKNOWN' | Tên pháp nhân đầy đủ của nhà thầu. |
| 25 | `silver.award_contractor` | `tax_code / country` | `VARCHAR` | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].taxCode`<br>`$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].taxNation` | `bideContractorInputResultDTO.lotResultDTO[].contractorList[].taxCode`: **98.6%** (Null 1.4%)<br>`bideContractorInputResultDTO.lotResultDTO[].contractorList[].taxNation`: **99.6%** (Null 0.4%) | clean_text(taxCode), clean_text(taxNation) ?: 'VN' | Mã số thuế và quốc gia đăng ký của nhà thầu. |
| 26 | `silver.award_contractor` | `is_consortium / venture_code / venture_name / role` | `BOOLEAN / VARCHAR` | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].ventureCode`<br>`$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].ventureName`<br>`$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].role` | `bideContractorInputResultDTO.lotResultDTO[].contractorList[].ventureCode`: **0.0%** (Null 100.0%)<br>`bideContractorInputResultDTO.lotResultDTO[].contractorList[].ventureName`: **0.3%** (Null 99.7%)<br>`bideContractorInputResultDTO.lotResultDTO[].contractorList[].role`: **0.0%** (Null 100.0%) | is_consortium = bool(ventureCode or ventureName); clean_text cho mã, tên và vai trò liên danh | Thông tin nhà thầu độc lập hay thành viên liên danh. |
| 27 | `silver.award_contractor` | `awarded_price` | `DECIMAL(38,6)` | `$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].bidWiningPrice`<br>`$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].lotFinalPrice`<br>`$.payload.bideContractorInputResultDTO.lotResultDTO[].contractorList[].lotPrice` | `bideContractorInputResultDTO.lotResultDTO[].contractorList[].bidWiningPrice`: **100.0%** (Null 0.0%)<br>`bideContractorInputResultDTO.lotResultDTO[].contractorList[].lotFinalPrice`: **100.0%** (Null 0.0%)<br>`bideContractorInputResultDTO.lotResultDTO[].contractorList[].lotPrice`: **100.0%** (Null 0.0%) | parse_money(bidWiningPrice ?: lotFinalPrice ?: lotPrice) | Giá trúng thầu / giá dự thầu sau đánh giá của nhà thầu. |

---

## 4. So Sánh Dịch Chuyển Schema & Chất Lượng Giữa Các Giai Đoạn (Window Drift Analysis)

### So sánh cửa sổ `2022-01-01..2022-02-21` vs. `2025-01-01..2025-01-08`

- **Trường mới xuất hiện:** *(Không có)*
- **Trường ngừng sử dụng:** *(Không có)*

#### Biến động tỷ lệ Null lớn (Null Ratio Shift ≥ 10%)

| Trường Dữ Liệu (`Field Path`) | Tỷ Lệ Null Cửa Sổ A (`2022`) | Tỷ Lệ Null Cửa Sổ B (`2025`) | Độ Lệch (`Delta`) | Giải Thích Nghiệp Vụ |
| :--- | ---: | ---: | ---: | :--- |
| `$.payload.decisionAgency` | `98.9%` | `0.0%` | `-98.9%` | Chính thức áp dụng bắt buộc từ cột mốc Golive e-GP mới (16/09/2022) |
| `$.payload.linkedPublishPlan[].expertiseOrg` | `23.5%` | `92.7%` | `+69.2%` | Trường lịch sử từ hệ thống e-GP cũ ngừng sử dụng |
| `$.payload.investTotalEx` | `0.0%` | `100.0%` | `+100.0%` | Trường lịch sử từ hệ thống e-GP cũ ngừng sử dụng |
| `$.payload.linkedPublishPlan[].missNumPack` | `23.5%` | `92.7%` | `+69.2%` | Trường lịch sử từ hệ thống e-GP cũ ngừng sử dụng |
| `$.payload.linkedPublishPlan[].decisionAgency` | `23.5%` | `92.7%` | `+69.2%` | Trường lịch sử từ hệ thống e-GP cũ ngừng sử dụng |
| `$.payload.linkedPublishPlan[].description` | `23.5%` | `92.7%` | `+69.2%` | Trường lịch sử từ hệ thống e-GP cũ ngừng sử dụng |
| `$.payload.linkedPublishPlan[].pNo` | `23.2%` | `74.2%` | `+51.0%` | Trường lịch sử từ hệ thống e-GP cũ ngừng sử dụng |
| `$.payload.linkedPublishPlan[].supervisor` | `23.5%` | `92.7%` | `+69.2%` | Trường lịch sử từ hệ thống e-GP cũ ngừng sử dụng |
| `$.payload.investForm` | `0.0%` | `100.0%` | `+100.0%` | Trường lịch sử từ hệ thống e-GP cũ ngừng sử dụng |
| `$.payload.linkedPublishPlan[].process` | `23.5%` | `92.7%` | `+69.2%` | Trường lịch sử từ hệ thống e-GP cũ ngừng sử dụng |
| `$.payload.linkedPublishPlan[].decisionFileId` | `23.5%` | `92.7%` | `+69.2%` | Trường lịch sử từ hệ thống e-GP cũ ngừng sử dụng |
| `$.payload.projectDTO.decisionAgency` | `98.9%` | `0.0%` | `-98.9%` | Chính thức áp dụng bắt buộc từ cột mốc Golive e-GP mới (16/09/2022) |
| `$.payload.linkedPublishPlan[].decisionFileName` | `23.5%` | `92.7%` | `+69.2%` | Trường lịch sử từ hệ thống e-GP cũ ngừng sử dụng |
| `$.payload.linkedPublishPlan[].competentPerson` | `23.5%` | `92.7%` | `+69.2%` | Trường lịch sử từ hệ thống e-GP cũ ngừng sử dụng |
| `$.payload.linkedPublishPlan[].reciprocalCapital` | `23.5%` | `92.7%` | `+69.2%` | Trường lịch sử từ hệ thống e-GP cũ ngừng sử dụng |
| `$.payload.investTarget` | `98.9%` | `56.4%` | `-42.5%` | Chính thức áp dụng bắt buộc từ cột mốc Golive e-GP mới (16/09/2022) |
| `$.payload.projectDTO.investTotalEx` | `0.0%` | `100.0%` | `+100.0%` | Trường lịch sử từ hệ thống e-GP cũ ngừng sử dụng |
| `$.payload.decisionFileName` | `100.0%` | `0.0%` | `-100.0%` | Chính thức áp dụng bắt buộc từ cột mốc Golive e-GP mới (16/09/2022) |
| `$.payload.projectDTO.performUnit` | `0.0%` | `100.0%` | `+100.0%` | Trường lịch sử từ hệ thống e-GP cũ ngừng sử dụng |
| `$.payload.districtName` | `100.0%` | `28.3%` | `-71.7%` | Chính thức áp dụng bắt buộc từ cột mốc Golive e-GP mới (16/09/2022) |
| `$.payload.projectDTO.investForm` | `0.0%` | `100.0%` | `+100.0%` | Trường lịch sử từ hệ thống e-GP cũ ngừng sử dụng |
| `$.payload.linkedPublishPlan[].versionDTOS` | `23.5%` | `92.7%` | `+69.2%` | Trường lịch sử từ hệ thống e-GP cũ ngừng sử dụng |
| `$.payload.linkedPublishPlan[].isConfirmStep` | `23.5%` | `92.7%` | `+69.2%` | Trường lịch sử từ hệ thống e-GP cũ ngừng sử dụng |
| `$.payload.projectDTO.code` | `0.0%` | `100.0%` | `+100.0%` | Trường lịch sử từ hệ thống e-GP cũ ngừng sử dụng |
| `$.payload.linkedPublishPlan[].capitalType` | `23.5%` | `92.7%` | `+69.2%` | Trường lịch sử từ hệ thống e-GP cũ ngừng sử dụng |
| `$.payload.plocation` | `100.0%` | `0.0%` | `-100.0%` | Chính thức áp dụng bắt buộc từ cột mốc Golive e-GP mới (16/09/2022) |
| `$.payload.linkedPublishPlan[].numPack` | `23.5%` | `92.7%` | `+69.2%` | Trường lịch sử từ hệ thống e-GP cũ ngừng sử dụng |
| `$.payload.linkedPublishPlan[].decisionNo` | `23.5%` | `92.7%` | `+69.2%` | Trường lịch sử từ hệ thống e-GP cũ ngừng sử dụng |
| `$.payload.projectDTO.investTarget` | `98.9%` | `56.4%` | `-42.5%` | Chính thức áp dụng bắt buộc từ cột mốc Golive e-GP mới (16/09/2022) |
| `$.payload.projectDTO.form` | `0.0%` | `30.9%` | `+30.9%` | Trường lịch sử từ hệ thống e-GP cũ ngừng sử dụng |
| `$.payload.linkedPublishPlan[].expertiseReportDate` | `23.5%` | `92.7%` | `+69.2%` | Trường lịch sử từ hệ thống e-GP cũ ngừng sử dụng |
| `$.payload.linkedPublishPlan[].investDetail` | `23.5%` | `92.7%` | `+69.2%` | Trường lịch sử từ hệ thống e-GP cũ ngừng sử dụng |
| `$.payload.linkedPublishPlan[].pVersion` | `23.2%` | `74.2%` | `+51.0%` | Trường lịch sử từ hệ thống e-GP cũ ngừng sử dụng |
| `$.payload.code` | `0.0%` | `100.0%` | `+100.0%` | Trường lịch sử từ hệ thống e-GP cũ ngừng sử dụng |
| `$.payload.linkedPublishPlan[].pName` | `23.5%` | `92.7%` | `+69.2%` | Trường lịch sử từ hệ thống e-GP cũ ngừng sử dụng |
| `$.payload.pform` | `0.0%` | `30.9%` | `+30.9%` | Trường lịch sử từ hệ thống e-GP cũ ngừng sử dụng |
| `$.payload.linkedPublishPlan[].exchangeDate` | `23.5%` | `92.7%` | `+69.2%` | Trường lịch sử từ hệ thống e-GP cũ ngừng sử dụng |
| `$.payload.linkedPublishPlan[].projectInvestNm` | `23.5%` | `92.7%` | `+69.2%` | Trường lịch sử từ hệ thống e-GP cũ ngừng sử dụng |
| `$.payload.provName` | `100.0%` | `19.9%` | `-80.1%` | Chính thức áp dụng bắt buộc từ cột mốc Golive e-GP mới (16/09/2022) |
| `$.payload.projectDTO.decisionFileId` | `100.0%` | `0.0%` | `-100.0%` | Chính thức áp dụng bắt buộc từ cột mốc Golive e-GP mới (16/09/2022) |
| `$.payload.projectDTO.decisionFileName` | `100.0%` | `0.0%` | `-100.0%` | Chính thức áp dụng bắt buộc từ cột mốc Golive e-GP mới (16/09/2022) |
| `$.payload.decisionFileId` | `100.0%` | `0.0%` | `-100.0%` | Chính thức áp dụng bắt buộc từ cột mốc Golive e-GP mới (16/09/2022) |

- **Đột biến kiểu dữ liệu (Type Mutations):**
  - `$.payload.decisionAgency`: `{'null': 989, 'string': 11}` → `{'string': 1000}`
  - `$.payload.investTotalEx`: `{'int': 1000}` → `{'null': 1000}`
  - `$.payload.investForm`: `{'string': 1000}` → `{'null': 1000}`
  - `$.payload.projectDTO.exChangeDate`: `{'null': 1000}` → `{'null': 998, 'datetime': 2}`
  - `$.payload.projectDTO.decisionAgency`: `{'null': 989, 'string': 11}` → `{'string': 1000}`
  - `$.payload.bidpProjectProjectPlanList[].planBidTotalUnit`: `{'string': 1005}` → `{'null': 24}`
  - `$.payload.linkedPlanOverall[].investmentFunds`: `{'null': 2}` → `{'string': 15, 'null': 9}`
  - `$.payload.projectDTO.investTotalEx`: `{'int': 1000}` → `{'null': 1000}`
  - `$.payload.decisionFileName`: `{'null': 1000}` → `{'string': 1000}`
  - `$.payload.projectDTO.performUnit`: `{'string': 1000}` → `{'null': 1000}`
  - `$.payload.linkedPublishPlan[].investTotalUnit`: `{'string': 339, 'null': 3}` → `{'string': 1195}`
  - `$.payload.districtName`: `{'null': 1000}` → `{'string': 717, 'null': 283}`
  - `$.payload.projectDTO.investForm`: `{'string': 1000}` → `{'null': 1000}`
  - `$.payload.projectDTO.code`: `{'string': 1000}` → `{'null': 1000}`
  - `$.payload.projectDTO.group`: `{'string': 1000}` → `{'string': 932, 'null': 68}`
  - `$.payload.pgroup`: `{'string': 1000}` → `{'string': 932, 'null': 68}`
  - `$.payload.plocation`: `{'null': 1000}` → `{'string': 1000}`
  - `$.payload.linkedPublishPlan[].investTotal`: `{'int': 342}` → `{'int': 1193, 'null': 2}`
  - `$.payload.projectDTO.investDetail[].exchangeRate`: `{'null': 1000, 'float': 13}` → `{'null': 1000}`
  - `$.payload.projectDTO.form`: `{'string': 1000}` → `{'null': 309, 'string': 691}`
  - `$.payload.projectDTO.reciprocalCapital`: `{'null': 994, 'int': 6}` → `{'null': 1000}`
  - `$.payload.competentPersons`: `{'string': 1000}` → `{'string': 932, 'null': 68}`
  - `$.payload.otherInfo`: `{'null': 1000}` → `{'null': 999, 'string': 1}`
  - `$.payload.investmentFunds`: `{'null': 1000}` → `{'null': 932, 'string': 68}`
  - `$.payload.code`: `{'string': 1000}` → `{'null': 1000}`
  - `$.payload.pform`: `{'string': 1000}` → `{'null': 309, 'string': 691}`
  - `$.payload.provName`: `{'null': 1000}` → `{'string': 801, 'null': 199}`
  - `$.payload.linkedPublishPlan[].isOda`: `{'int': 342}` → `{'int': 1193, 'null': 2}`
  - `$.payload.projectDTO.projectPlanList[].planBidTotalUnit`: `{'string': 1005}` → `{'null': 24}`
  - `$.payload.projectDTO.decisionFileId`: `{'null': 1000}` → `{'string': 1000}`
  - `$.payload.projectDTO.decisionFileName`: `{'null': 1000}` → `{'string': 1000}`
  - `$.payload.decisionFileId`: `{'null': 1000}` → `{'string': 1000}`
