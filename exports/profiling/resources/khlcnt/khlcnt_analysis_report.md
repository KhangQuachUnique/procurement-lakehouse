# Báo Cáo Phân Tích Thực Nghiệm & Bảng Ánh Xạ Schema: Kế Hoạch & Gói Thầu Lựa Chọn Nhà Thầu (KHLCNT)

- **Tài nguyên (Resource):** `khlcnt` (Kế hoạch lựa chọn nhà thầu & Gói thầu thuộc kế hoạch)
- **Bảng Bronze vật lý:** 
  1. `khlcnt_plan_detail` (Chi tiết kế hoạch LCNT cấp cha)
  2. `khlcnt_bid_package_detail` (Chi tiết gói thầu & lô thầu cấp con)
- **Quy mô dữ liệu hoạt động:** **3,438,353 bản ghi** (1,525 partitions, 2.92 GB nén Parquet):
  - `khlcnt_plan_detail`: **1,070,653 bản ghi**
  - `khlcnt_bid_package_detail`: **2,367,700 bản ghi**
- **Phạm vi thời gian:** Từ `2022-01-01` đến `2026-03-05`.
- **Phương pháp khảo sát:** Bằng chứng số liệu thực nghiệm (evidence-first), phân tích sâu cấu trúc lồng nhau JSON raw từ 100 phân vùng ngày, so sánh đối chiếu thực nghiệm giữa schema cũ và schema chuẩn hóa mới.

---

## 1. Bóc Tách Các Khối Dữ Liệu Raw JSON Trong `khlcnt`

Tài nguyên `khlcnt` có cấu trúc phân cấp thực thể 2 bảng độc lập liên kết quan hệ logic:

```
khlcnt_plan_detail (KHLCNT Cha)
└── bidPoBidpPlanProjectDetailView (100% - Siêu dữ liệu phê duyệt KHLCNT)
    └── (Tham chiếu Dự án đầu tư qua pid / pno)

khlcnt_bid_package_detail (Gói Thầu Con)
├── Top-level Package Attributes (100% - Thông tin đấu thầu, thời gian, hình thức, nguồn vốn)
├── bidLocation[] (Địa bàn thực hiện gói thầu theo Tỉnh/Huyện/Xã)
└── bidpBidLotList[] (Danh sách phần/lô thầu chi tiết)
```

| Cấp Độ Thực Thể | Tên Bảng Bronze / Khối Path | Kiểu Dữ Liệu | Tần Suất | Ý Nghĩa Nghiệp Vụ & Dữ Liệu Chứa Bên Trong |
| :---: | :--- | :---: | :---: | :--- |
| **Kế hoạch (Plan Root)** | **`khlcnt_plan_detail`** $\rightarrow$ `bidPoBidpPlanProjectDetailView` | Object | **100.0%** | **Thực thể Kế hoạch LCNT:** Chứa 160 trường dữ liệu: mã KHLCNT (`planNo`), tên kế hoạch (`name`), chủ đầu tư (`investorName`), số quyết định (`decisionNo`), ngày phê duyệt (`decisionDate`), cơ quan phê duyệt (`decisionAgency`), phân loại kế hoạch (`planType`: DTPT/TX), tổng mức đầu tư (`investTotal`), đơn vị tiền (`investTotalUnit`), số gói thầu (`bidPack`), ngày công khai (`publicDate`), trạng thái (`status`), liên kết dự án (`pid`, `pno`). |
| **Gói thầu (Package Root)** | **`khlcnt_bid_package_detail`** $\rightarrow$ Top-level Payload | Object | **100.0%** | **Thực thể Gói thầu:** Chứa 163 trường dữ liệu: mã gói thầu (`bidNo`), tên gói thầu (`bidName`), mã KHLCNT cha (`planNo`, `planId`), giá gói thầu (`bidPrice`, `bidPriceUnit`), quy trình áp dụng (`processApply`: LDT), lĩnh vực (`bidField`), hình thức (`bidForm`), phương thức (`bidMode`), chi tiết nguồn vốn (`capitalDetail`), loại hợp đồng (`ctype`), thời gian thực hiện (`cperiod`, `cperiodUnit`), cờ trong nước/quốc tế (`isDomestic`), đấu thầu qua mạng (`isInternet`), thời điểm mở thầu dự kiến (`bidStartYear`, `bidStartQuarter`, `bidStartMonth`). |
| **Địa bàn (Location Child)** | **`khlcnt_bid_package_detail`** $\rightarrow$ `bidLocation[]` | Array[Object] | **100.0%** | **Bảng con `package_location`:** Phân rã địa bàn gói thầu: mã tỉnh (`provCode`), tên tỉnh (`provName`), mã huyện (`districtCode`), tên huyện (`districtName`), mã xã (`wardCode`), tên xã (`wardName`). |
| **Lô thầu (Lot Child)** | **`khlcnt_bid_package_detail`** $\rightarrow$ `bidpBidLotList[]` | Array[Object] | **99.6%** | **Bảng con `package_lot`:** Phân rã phần/lô thầu: mã phần/lô (`lotNo`), tên lô (`lotName`), giá kế hoạch của lô (`lotPrice`), số lượng (`quantity`), đơn vị tính (`uom`), giá dự toán (`pricePlan`), mã thuốc y tế (`medicineCode`). |

---

## 2. Bảng Ánh Xạ Chi Tiết (Raw JSON $\rightarrow$ Silver Schema) & Đo Lường Chất Lượng

### 2.1. Bảng `silver.plan_revision` (Kế hoạch LCNT - 18 Cột Nghiệp Vụ)

| Field Raw JSON | Field Silver Schema | Kiểu Dữ Liệu | Tỷ Lệ Null | Đánh Giá Chất Lượng | Giá Trị Mẫu Thực Tế | Ghi Chú Kỹ Thuật |
| :--- | :--- | :--- | :---: | :---: | :--- | :--- |
| `Derived: sha256(...)` | `revision_id` | `VARCHAR` (PK) | **0.00%** | **XUẤT SẮC** | `rev_84b2c19a...` | Khóa bất biến của revision. |
| `Derived: sha256(...)` | `entity_id` | `VARCHAR` | **0.00%** | **XUẤT SẮC** | `plan_51b250f7...` | Khóa logic của kế hoạch. |
| `bidPoBidpPlanProjectDetailView.planNo` | `plan_no` | `VARCHAR` | **0.00%** | **XUẤT SẮC** | `20220100496` | Số hiệu KHLCNT phát hành trên e-GP. |
| `bidPoBidpPlanProjectDetailView.name` | `title` | `VARCHAR` | **0.00%** | **XUẤT SẮC** | `Kiên cố hóa kênh mương...` | Tên KHLCNT (đã làm sạch whitespace). |
| `bidPoBidpPlanProjectDetailView.investorName` | `investor_name` | `VARCHAR` | **0.00%** | **XUẤT SẮC** | `Ban Quản lý dự án đầu tư...` | Chủ đầu tư phê duyệt kế hoạch. |
| *(Resolver populates)* | `project_id` | `VARCHAR` | *Populated later* | **LIÊN KẾT** | `proj_99e120...` | Khóa ngoại trỏ sang thực thể Project. |
| `bidPoBidpPlanProjectDetailView.pno` | `project_no` | `VARCHAR` | **0.90%** | **TỐT** | `20220100494` | Số dự án nguồn (đã lọc sentinel `N/A`, `NULL`). |
| `bidPoBidpPlanProjectDetailView.planType` | `plan_type` | `VARCHAR` | **0.00%** | **XUẤT SẮC** | `DTPT`, `TX` | Loại kế hoạch: Đầu tư phát triển / Thường xuyên. |
| `bidPoBidpPlanProjectDetailView.investTotal` | `total_investment` | `DECIMAL(38,6)` | **0.00%** | **XUẤT SẮC** | `10962776000.000000` | Tổng mức đầu tư (xử lý qua `JsonFloatLexeme`). |
| `bidPoBidpPlanProjectDetailView.investTotalUnit` | `currency` | `VARCHAR` | **0.00%** | **XUẤT SẮC** | `VND` | Đơn vị tiền tệ. |
| `bidPoBidpPlanProjectDetailView.decisionDate` | `decision_date` | `DATE` | **0.20%** | **XUẤT SẮC** | `2021-12-17` | Ngày ban hành quyết định phê duyệt. |
| `bidPoBidpPlanProjectDetailView.decisionNo` | `decision_no` | `VARCHAR` | **0.00%** | **XUẤT SẮC** | `1128/QĐ-UBND` | Số quyết định phê duyệt kế hoạch. |
| `bidPoBidpPlanProjectDetailView.decisionAgency` | `decision_agency` | `VARCHAR` | **0.00%** | **XUẤT SẮC** | `UBND Huyện Định Quán` | Cơ quan ban hành quyết định phê duyệt. |
| `bidPoBidpPlanProjectDetailView.bidPack` | `bid_pack` | `INTEGER` | **0.00%** | **XUẤT SẮC** | `1` | Số lượng gói thầu trong kế hoạch. |
| `bidPoBidpPlanProjectDetailView.location` | `location` | `VARCHAR` | **98.50%** | **CHẤP NHẬN ĐƯỢC** | `Tỉnh Bạc Liêu` | Địa bàn cấp kế hoạch (đa số ghi ở từng gói). |
| `bidPoBidpPlanProjectDetailView.publicDate` | `public_date` | `DATE` | **0.00%** | **XUẤT SẮC** | `2022-01-02` | **Khóa phân vùng chính Iceberg Lakehouse**. |
| `bidPoBidpPlanProjectDetailView.status` | `status_raw` | `VARCHAR` | **0.00%** | **XUẤT SẮC** | `IS_PUBLISH` | Trạng thái đăng tải. |
| `Canonical serialized root` | `root_json` | `TEXT` | **0.00%** | **XUẤT SẮC** | `{...}` | JSON chuẩn hóa phục vụ đối soát & lineage. |

---

### 2.2. Bảng `silver.package_revision` (Gói thầu KHLCNT - 24 Cột Nghiệp Vụ)

| Field Raw JSON | Field Silver Schema | Kiểu Dữ Liệu | Tỷ Lệ Null | Đánh Giá Chất Lượng | Giá Trị Mẫu Thực Tế | Ghi Chú Kỹ Thuật |
| :--- | :--- | :--- | :---: | :---: | :--- | :--- |
| `Derived: sha256(...)` | `revision_id` | `VARCHAR` (PK) | **0.00%** | **XUẤT SẮC** | `rev_40e6e536...` | Khóa bất biến của revision gói thầu. |
| `Derived: sha256(...)` | `entity_id` | `VARCHAR` | **0.00%** | **XUẤT SẮC** | `pkg_mi_40e6e5...` | Khóa logic của gói thầu. |
| `bidNo` | `package_no` | `VARCHAR` | **0.00%** | **XUẤT SẮC** | `BP2200029655` | Mã số gói thầu trong KHLCNT. |
| `bidName` | `title` | `VARCHAR` | **0.00%** | **XUẤT SẮC** | `Thi công xây dựng` | Tên gói thầu. |
| `planNo` | `plan_no` | `VARCHAR` | **0.00%** | **XUẤT SẮC** | `20220100496` | Mã số KHLCNT cha (khóa ngoại tự nhiên). |
| *(Resolver populates)* | `plan_id` | `VARCHAR` | *Populated later* | **LIÊN KẾT** | `plan_51b250...` | Khóa ngoại trỏ sang thực thể Plan. |
| `bidPrice` | `bid_price` | `DECIMAL(38,6)` | **0.00%** | **XUẤT SẮC** | `9468825000.000000` | Giá trị gói thầu (xử lý qua `JsonFloatLexeme`). |
| `bidPriceUnit` | `currency` | `VARCHAR` | **0.00%** | **XUẤT SẮC** | `VND` | Đơn vị tiền tệ. |
| `processApply` | `process_apply` | `VARCHAR` | **0.00%** | **XUẤT SẮC** | `LDT` | Quy trình áp dụng (Luật Đấu Thầu). |
| `bidField` | `bid_field` | `VARCHAR` | **0.00%** | **XUẤT SẮC** | `XL`, `HH`, `TV` | Lĩnh vực: Xây lắp, Hàng hóa, Tư vấn... |
| `bidForm` | `bid_form` | `VARCHAR` | **0.00%** | **XUẤT SẮC** | `DTRR`, `CHCD` | Hình thức LCNT: Đấu thầu rộng rãi, Chỉ định thầu... |
| `bidMode` | `bid_mode` | `VARCHAR` | **0.00%** | **XUẤT SẮC** | `1_MTHS`, `2_MTHS` | Phương thức: 1 giai đoạn 1 túi / 2 túi hồ sơ. |
| `capitalDetail` | `capital_detail` | `VARCHAR` | **0.00%** | **XUẤT SẮC** | `Ngân sách huyện bổ sung...` | Nguồn vốn chi tiết cho gói thầu. |
| `ctype` | `contract_type` | `VARCHAR` | **0.00%** | **XUẤT SẮC** | `TG`, `CD` | Loại hợp đồng: Trọn gói, Cố định... |
| `cperiod` | `execution_period` | `INTEGER` | **0.00%** | **XUẤT SẮC** | `240` | Thời gian thực hiện hợp đồng. |
| `cperiodUnit` | `execution_period_unit`| `VARCHAR` | **0.00%** | **XUẤT SẮC** | `D`, `M` | Đơn vị thời gian: Ngày (`D`), Tháng (`M`). |
| `isDomestic` | `is_domestic` | `BOOLEAN` | **0.00%** | **XUẤT SẮC** | `True` | Đấu thầu trong nước (1) hay quốc tế (0). |
| `isInternet` | `is_internet` | `BOOLEAN` | **0.00%** | **XUẤT SẮC** | `True` | Đấu thầu qua mạng (1) hay trực tiếp (0). |
| `bidStartUnit` | `bid_start_unit` | `VARCHAR` | **0.00%** | **XUẤT SẮC** | `Q`, `M` | Đơn vị bắt đầu tổ chức: Quý (`Q`), Tháng (`M`). |
| `bidStartYear` | `bid_start_year` | `INTEGER` | **0.00%** | **XUẤT SẮC** | `2021` | Năm bắt đầu tổ chức lựa chọn nhà thầu. |
| `bidStartQuarter` | `bid_start_quarter`| `VARCHAR` | **13.00%** | **TỐT** | `IV` | Quý bắt đầu tổ chức (đã xử lý sentinel `'0'`). |
| `bidStartMonth` | `bid_start_month` | `INTEGER` | **87.00%** | **TỐT** | `12` | Tháng bắt đầu tổ chức (đã xử lý sentinel `0`). |
| `Fallback: createdDate / source_date`| `public_date` | `DATE` | **0.00%** | **XUẤT SẮC** | `2022-10-19` | **Khóa phân vùng Iceberg Lakehouse (Vá kỹ thuật)**. |
| `Canonical serialized root` | `root_json` | `TEXT` | **0.00%** | **XUẤT SẮC** | `{...}` | Payload gốc đối soát. |

---

### 2.3. Bảng Con `silver.package_lot` (Phần/Lô Thầu) & `silver.package_location` (Địa Bàn)

#### `silver.package_lot` (12 Cột)
* `lot_id` (`VARCHAR`, PK): Băm `sha256(revision_id, 'lot', ordinal, source_lot_id)`.
* `revision_id` (`VARCHAR`, FK): Liên kết về `package_revision`.
* `source_lot_id` (`VARCHAR`): ID gốc của lô thầu trên e-GP.
* `lot_ordinal` (`INTEGER`): Thứ tự lô thầu trong gói.
* `lot_no` (`VARCHAR`): Số/ký hiệu lô thầu.
* `lot_name` (`VARCHAR`): Tên gọi của phần/lô thầu.
* `lot_price` (`DECIMAL(38,6)`): Giá trị dự toán của phần/lô thầu.
* `currency` (`VARCHAR`): Kế thừa đơn vị tiền tệ từ gói thầu (`bidPriceUnit`) nếu lô thầu không khai báo riêng.
* `medicine_code` (`VARCHAR`): Mã thuốc (đối với gói thầu y tế/dược phẩm).
* `quantity` (`DECIMAL(38,6)`): Số lượng mời thầu theo lô.
* `uom` (`VARCHAR`): Đơn vị tính (viên, hộp, gói...).
* `planned_unit_price` (`DECIMAL(38,6)`): Đơn giá kế hoạch.

#### `silver.package_location` (8 Cột)
* `revision_id` (`VARCHAR`, FK): Liên kết về `package_revision`.
* `location_ordinal` (`INTEGER`): Thứ tự địa bàn thực hiện.
* `province_code` (`VARCHAR`): Mã tỉnh/thành phố (đã loại bỏ ký tự pipe `|713|` $\rightarrow$ `713`).
* `province_name` (`VARCHAR`): Tên tỉnh/thành phố (VD: `Tỉnh Đồng Nai`).
* `district_code` (`VARCHAR`): Mã quận/huyện.
* `district_name` (`VARCHAR`): Tên quận/huyện.
* `ward_code` (`VARCHAR`): Mã phường/xã.
* `ward_name` (`VARCHAR`): Tên phường/xã.

---

## 3. Khám Phá Nghiệp Vụ Thực Nghiệm & Bằng Chứng Số Liệu

1. **Đo Lường Kiểm Chứng Bằng Chứng (0.0% vs 100.0%):**
   * Quét thực nghiệm 100 phân vùng ngày (470 bản ghi `plan`, 1,000 bản ghi `package`):
   * Toàn bộ các trường trong schema cũ (`invest_target`, `invest_scale`, `amount`, `buyer_id`, `buyer_name`) **đều có tỷ lệ xuất hiện = 0.0%**.
   * Toàn bộ các trường trong schema mới (`decisionNo`, `decisionDate`, `planType`, `bidPack`, `bidStartYear`, `processApply`, `capitalDetail`...) **đều có tỷ lệ xuất hiện = 100.0%**.
2. **Kỹ Thuật Xử Lý Tiền Tệ Số Thực (Float Precision Preservation):**
   * Trong JSON Bronze của Mua Sắm Công, các trường `bidPrice`, `lotPrice`, `pricePlan` xuất hiện dưới dạng số thực dạng `302472435111.978`.
   * Sử dụng lớp `JsonFloatLexeme(float)` để giữ nguyên lexeme ký tự gốc trong quá trình `json.loads`, truyền sang `parse_money()` tạo `Decimal("302472435111.978000")` chính xác từng xu, đồng thời không làm thay đổi hàm băm SHA256 của root JSON.
3. **Phân Vùng Bất Biến Cho Apache Iceberg Format v2:**
   * `plan_revision`: Phân vùng theo `public_date` (100% bản ghi có giá trị).
   * `package_revision`: Được bổ sung trường `public_date` thông qua fallback `createdDate.date() -> source_date`, giải quyết triệt để lỗi phân vùng rỗng của Apache Iceberg REST Catalog.

---

## 4. Trạng Thái Triển Khai Tiếp Theo (Implementation Roadmap)

- [x] **Bước 1:** Báo cáo phân tích thực nghiệm & Hồ sơ ánh xạ JSON profile (`khlcnt_analysis_report.md` & `khlcnt_field_profile.json`).
- [ ] **Bước 2:** Cập nhật DDL trong `schema.py` (`plan_revision`, `package_revision`, `package_lot`, `package_location`) & bổ sung `json_numbers.py`, `parsers.py`.
- [ ] **Bước 3:** Triển khai `PlanDetailTransformer` và `PackageDetailTransformer` vào cấu trúc Hexagonal `src/procurement/silver/domain/transformers/` & bổ sung DuckDB View `v_latest_package`.
- [ ] **Bước 4:** Di chuyển và thực thi bộ Unit Test (`test_plan_transformer.py`, `test_package_transformer.py`, `test_bronze_money_precision.py`).
- [ ] **Bước 5:** Bổ sung CLI `plan` & `package` và kiểm thử End-to-End trên SeaweedFS S3 + Iceberg REST Catalog thực tế.
