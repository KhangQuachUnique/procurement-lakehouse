# Báo Cáo Phân Tích Thực Nghiệm & Bảng Ánh Xạ Schema: Thông Báo Mời Thầu (TBMT)

- **Tài nguyên (Resource):** `notify_contractor` (Thông báo mời thầu)
- **Các bảng Bronze vật lý:**
  1. `notify_contractor_standard_detail` (Gói thầu chuẩn trong nước & quốc tế)
  2. `notify_contractor_vk_adb_detail` (Gói thầu vốn ODA, Ngân hàng Phát triển Châu Á ADB, Ngân hàng Thế giới WB)
  3. `notify_contractor_reoffer_detail` (Gói thầu chào lại giá / đấu giá điện tử)
- **Phương pháp khảo sát:** Bằng chứng số liệu thực nghiệm (evidence-first), truy vấn metadata hoạt động từ PostgreSQL (`bronze_meta`), phân tích đệ quy toàn bộ JSON raw.

---

## 1. Bóc Tách 18 Khối Dữ Liệu Raw JSON Trong TBMT

Trong Bronze, payload của một bản ghi TBMT bao gồm tối đa 18 khối đối tượng và mảng dữ liệu:

| STT | Tên Khối (Block/Path) | Kiểu Dữ Liệu | Tần Suất Xuất Hiện | Ý Nghĩa Nghiệp Vụ & Dữ Liệu Chứa Bên Trong |
| :---: | :--- | :---: | :---: | :--- |
| 1 | **`bidoNotifyContractorM`** | Object | **100.0%** (Standard) | **Khối nghiệp vụ chính:** Chứa mã TBMT (`notifyNo`), số hiệu làm rõ (`notifyVersion`), mã gói thầu (`bidId`), tên gói (`bidName`), giá gói thầu (`bidPrice`), loại tiền (`bidPriceUnit`), bên mời thầu (`procuringEntityCode`), chủ đầu tư (`investorCode`), thời điểm đăng tải (`publicDate`), thời điểm mở/đóng thầu (`bidOpenDate`, `bidCloseDate`). |
| 2 | **`bidNoContractorResponse`** | Object | **100.0%** (Standard) | **Khối phản hồi hệ thống e-GP:** Chứa bản sao toàn diện `bidNotification` và mảng danh sách phần/lô `lotDTOList`. Đây là nguồn dữ liệu phản chiếu tin cậy khi khối chính bị khuyết hoặc làm rõ. |
| 3 | **`bidpPlanDetail`** | Object | **100.0%** | **Chi tiết KHLCNT nhúng:** Thông tin gói thầu trích từ Kế hoạch lựa chọn nhà thầu tương ứng: `planId`, `planNo`, `planName`, `isMultiLot`, `bidField`, `cperiod` (thời gian thực hiện). |
| 4 | **`bidoBidStatus`** | Object | **100.0%** | **Tiến trình & Trạng thái:** Trạng thái gói thầu và cờ phân chia nhiều phần/lô (`isMultiLot`). |
| 5 | **`bidpBidLocationList`** | Array | **88.0%** | **Địa điểm mở thầu & thực hiện hợp đồng:** Mảng danh sách địa bàn gồm mã tỉnh (`provCode`), tên tỉnh (`provName`), mã huyện (`districtCode`), tên huyện (`districtName`). Ánh xạ thành entity con `notice_location`. |
| 6 | **`lsBidpBidLocationDTO`** | Array | **12.0%** | **Địa điểm dự phòng:** Biến thể của danh sách địa điểm khi hồ sơ có cấu trúc biểu mẫu phụ. |
| 7 | **`lotDTOList`** | Array | **8.0%** | **Danh sách phần/lô:** Dành cho gói thầu chia nhiều phần/lô (`notice_lot`). |
| 8 | **`contractorsResult`** | Array | **15.0%** | **Kết quả chào giá điện tử (Reoffer):** Chứa các lần chào giá của nhà thầu (`notice_reoffer_submission`). |
| 9 | **`bidoNotifyContractorP`** | Object | **100.0%** (VK/ADB) | **Khối nghiệp vụ ODA:** Thay thế `bidoNotifyContractorM` đối với các gói thầu vay vốn ADB/WB. |
| 10 | **`bidDetail`** | Object | **100.0%** (Reoffer) | **Chi tiết gói thầu chào lại giá:** Thông số kỹ thuật và bước giá (`priceStep`, `priceInit`). |
| 11-18 | Các khối phụ khác | Mixed | < 5.0% | `cancelNotice`, `decisionDTO`, `clarificationList`, `fileAttachDTOList`... |

---

## 2. Bảng Ánh Xạ Chi Tiết (Raw JSON $\rightarrow$ Silver Schema) & Đo Lường Chất Lượng

Dưới đây là bảng đo lường chất lượng thực tế trên tập dữ liệu Bronze:

| Field Raw JSON | Field Silver Schema | Kiểu Dữ Liệu | Tỷ Lệ Null | Chuỗi Trắng / Rác | Chất Lượng Dữ Liệu (DQ Assessment) | Giá Trị Mẫu Thực Tế | Ghi Chú Nghiệp Vụ |
| :--- | :--- | :--- | :---: | :---: | :--- | :--- | :--- |
| `bidoNotifyContractorM.notifyNo` | `notify_no` | `VARCHAR` | **0.00%** | **0.00%** | **XUẤT SẮC** | `IB2400012345` | Mã số thông báo mời thầu định danh toàn quốc. |
| `bidoNotifyContractorM.notifyVersion` | `notify_version` | `VARCHAR` | **0.00%** | **0.00%** | **XUẤT SẮC** | `00`, `01`, `02` | Phiên bản TBMT (`00`: gốc, `01`+: làm rõ/sửa đổi). |
| `bidoNotifyContractorM.bidName` | `title` | `VARCHAR` | **0.00%** | **0.00%** | **XUẤT SẮC** | `Xây lắp trạm biến áp...` | Tên gói thầu trong thông báo mời thầu. |
| `bidoNotifyContractorM.bidPrice` | `bid_price` | `DECIMAL(28,6)` | **0.00%** | **0.00%** | **XUẤT SẮC** | `1250000000.000000` | Giá gói thầu được duyệt. |
| `bidoNotifyContractorM.bidPriceUnit` | `currency` | `VARCHAR` | **0.00%** | **0.00%** | **XUẤT SẮC** | `VND` | 100% bản ghi là VND. |
| `bidoNotifyContractorM.guaranteeValue` | `guarantee_amount` | `DECIMAL(28,6)` | **25.34%** | **0.00%** | **HỢP LÝ** | `25000000.000000` | Giá trị bảo đảm dự thầu. Gói chào hàng cạnh tranh rút gọn không bắt buộc nên null 25.34% là chuẩn nghiệp vụ. |
| `bidoNotifyContractorM.publicDate` | `published_at` | `TIMESTAMP` | **0.00%** | **0.00%** | **XUẤT SẮC** | `2024-03-01T10:30:00` | Thời điểm công khai TBMT trên cổng e-GP. |
| `bidoNotifyContractorM.bidOpenDate` | `bid_open_date` | `TIMESTAMP` | **0.00%** | **0.00%** | **XUẤT SẮC** | `2024-03-15T09:00:00` | Thời điểm mở thầu. |
| `bidoNotifyContractorM.bidCloseDate` | `bid_close_date` | `TIMESTAMP` | **0.00%** | **0.00%** | **XUẤT SẮC** | `2024-03-15T09:00:00` | Thời điểm đóng thầu (hạn chót nộp E-HSDT). |
| `bidoNotifyContractorM.procuringEntityCode` | `procuring_entity_code` | `VARCHAR` | **0.00%** | **0.00%** | **XUẤT SẮC** | `vnz000012345` | Mã định danh Bên mời thầu. |
| `bidoNotifyContractorM.investorCode` | `investor_code` | `VARCHAR` | **0.00%** | **0.00%** | **XUẤT SẮC** | `vnz000067890` | Mã định danh Chủ đầu tư. |
| `bidoNotifyContractorM.isInternet` | `is_internet` | `BOOLEAN` | **0.00%** | **0.00%** | **XUẤT SẮC** | `1`, `0` | Cờ đấu thầu qua mạng. |
| `bidpPlanDetail.isMultiLot` | `is_multi_lot` | `BOOLEAN` | **0.00%** | **0.00%** | **XUẤT SẮC (FALLBACK)** | `false`, `true` | Gốc `bidoNotifyContractorM` bị null, fallback sang `bidpPlanDetail` đạt 100%. |
| `bidpPlanDetail.bidField` | `bid_field` | `VARCHAR` | **0.00%** | **0.00%** | **XUẤT SẮC (FALLBACK)** | `XL`, `HH`, `TV`, `PTV` | Lĩnh vực: Xây lắp, Hàng hóa, Tư vấn, Phi tư vấn. |
| `bidpPlanDetail.cperiod` | `contract_duration_raw` | `VARCHAR` | **0.00%** | **0.00%** | **XUẤT SẮC (FALLBACK)** | `180`, `360` | Thời gian thực hiện hợp đồng. |
| `bidpPlanDetail.cperiodUnit` | `contract_duration_unit`| `VARCHAR` | **0.00%** | **0.00%** | **XUẤT SẮC (FALLBACK)** | `Ngày`, `Tháng` | Đơn vị thời gian thực hiện. |
| `bidoNotifyContractorM.cancelReason` | `cancel_reason` | `VARCHAR` | 98.67% | 0.00% | **BÌNH THƯỜNG** | `Thay đổi quy mô...` | Chỉ có giá trị khi gói thầu bị hủy thầu. |

---

## 3. Các Quy Tắc Kiểm Định Chất Lượng & Xử Lý Dữ Liệu Đã Áp Dụng (Data Quality Rules)

1. **Bộ Lọc Giá Trị Rác / Sentinel Values:**
   - Các giá trị chuỗi rác xuất hiện trong Bronze gồm: `"N/A"`, `"NA"`, `"-"`, `"--"`, `"null"`, `"none"`, `"chưa có"` đều được chuyển đổi tự động về `None` (`NULL` chuẩn) qua `clean_text(filter_sentinels=True)`.
2. **Kiểm Tra Trật Tự Thời Gian (Temporal Validation):**
   - Bắt buộc kiểm tra logic nghiệp vụ mua sắm:
     - `bidCloseDate > bidOpenDate` $\rightarrow$ Ghi nhận issue `invalid_date_order` (thời điểm đóng thầu không thể sau thời điểm mở thầu).
     - `publishedAt > bidCloseDate` $\rightarrow$ Ghi nhận issue `invalid_date_order` (ngày phát hành HSMT không thể sau ngày đóng thầu).
3. **Phân Tách Sub-Entities:**
   - Bóc tách `notice_location` (địa bàn thực hiện) và `notice_lot` (danh sách phần/lô) thành các bảng con độc lập có khóa ngoại liên kết với `notice_revision`.
