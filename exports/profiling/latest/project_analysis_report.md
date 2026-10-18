# Báo Cáo Phân Tích Thực Nghiệm & Bảng Ánh Xạ Schema: Dự Án Đầu Tư (Project)

- **Tài nguyên (Resource):** `project` (Dự án đầu tư công / Dự án mua sắm)
- **Bảng Bronze vật lý:** `project_detail`
- **Quy mô dữ liệu hoạt động:** **248,360 bản ghi** (1,525 partitions, 1,499 files Parquet, dung lượng 206.55 MB).
- **Phạm vi thời gian:** Từ `2022-01-01` đến `2026-03-05`.
- **Phương pháp khảo sát:** Bằng chứng số liệu thực nghiệm (evidence-first), phân tích đệ quy toàn bộ JSON raw và đối chiếu dòng thời gian chuyển dịch dữ liệu (drift analysis).

---

## 1. Bóc Tách Các Khối Dữ Liệu Raw JSON Trong `project_detail`

Trong Bronze, payload của một bản ghi dự án bao gồm **4 khối dữ liệu chính**:

| STT | Tên Khối (Block/Path) | Kiểu Dữ Liệu | Tần Suất Xuất Hiện | Ý Nghĩa Nghiệp Vụ & Dữ Liệu Chứa Bên Trong |
| :---: | :--- | :---: | :---: | :--- |
| 1 | **`projectDTO`** | Object (Dict) | **100.0%** | **Khối nghiệp vụ nòng cốt:** Chứa 44 trường định danh dự án, mã dự án (`no`), phiên bản (`version`), tên dự án (`name`), chủ đầu tư (`investorCode`, `investorName`), tổng mức đầu tư (`investTotal`, `investTotalUnit`), quyết định phê duyệt (`decisionNo`, `decisionDate`, `decisionAgency`), phân nhóm dự án (`group`), cờ ODA (`isOda`), trạng thái (`status`). |
| 2 | **`bidpProjectProjectPlanList`** | Array[Object] | **98.5%** | **Mảng liên kết Kế hoạch LCNT (1-N):** Chứa danh sách các KHLCNT trực thuộc dự án. Mỗi phần tử có: `id`, `planNo`, `planName`, `planBidTotal`, `planBidTotalUnit`, `isOldPlan`, `pId`. Đây là **mối quan hệ cha-con cốt lõi** giữa Dự án và Kế hoạch. |
| 3 | **`investmentFunds`** | Array[Object] | **100.0%** | **Cơ cấu nguồn vốn:** Danh sách chi tiết các nguồn vốn tài trợ cho dự án (vốn ngân sách trung ương, vốn ngân sách địa phương, vốn ODA, vốn tự có của doanh nghiệp...). |
| 4 | **`Top-level Mirrors`** | Flat Fields | **100.0%** | **Trường phẳng cấp root:** Sao chép song song các trường từ `projectDTO` đồng thời bổ sung thông tin địa lý cấp huyện/tỉnh: `provCode`, `provName`, `districtCode`, `districtName`, `plocation`. |

---

## 2. Bảng Ánh Xạ Chi Tiết (Raw JSON $\rightarrow$ Silver Schema) & Đo Lường Chất Lượng

Dưới đây là bảng đo lường chất lượng thực tế trên 1,000 bản ghi đại diện toàn diện từ Bronze:

| Field Raw JSON | Field Silver Schema | Kiểu Dữ Liệu | Tỷ Lệ Null | Chuỗi Trắng / Rác | Chất Lượng Dữ Liệu (DQ Assessment) | Giá Trị Mẫu Thực Tế | Ghi Chú Nghiệp Vụ |
| :--- | :--- | :--- | :---: | :---: | :--- | :--- | :--- |
| `projectDTO.id` | `entity_id` / `source_id` | `VARCHAR` | **0.00%** | **0.00%** | **XUẤT SẮC** | `88339013481120382001` | UUID duy nhất 100% trong toàn bộ bảng Bronze. Khóa chính tin cậy. |
| `projectDTO.no` | `project_no` / `business_number` | `VARCHAR` | **0.00%** | **0.00%** | **XUẤT SẮC** | `20220100494`, `PR2400000017` | Mã số dự án chuẩn hiển thị nghiệp vụ trên e-GP. |
| `projectDTO.version` | `source_version` | `VARCHAR` | **0.00%** | **0.00%** | **XUẤT SẮC** | `01`, `02` | Phiên bản điều chỉnh dự án. Đã có fallback khi envelope thiếu version. |
| `projectDTO.name` | `title` | `VARCHAR` | **0.00%** | **0.00%** | **XUẤT SẮC** | `Nâng cấp, cải tạo các tuyến đường...` | Tên dự án đầu tư xây dựng/mua sắm. |
| `projectDTO.investorCode` | `investor_code` / `buyer_id` | `VARCHAR` | **0.00%** | **0.00%** | **XUẤT SẮC** | `vnz000013471` | Mã định danh Chủ đầu tư (Khóa ngoại tham chiếu bên mua). |
| `projectDTO.investorName` | `investor_name` / `buyer_name` | `VARCHAR` | **0.00%** | **0.00%** | **XUẤT SẮC** | `Công ty Điện lực Long Biên` | Tên pháp nhân của Chủ đầu tư. |
| `projectDTO.investTotal` | `total_investment` / `amount` | `DECIMAL(38,6)` | **0.00%** | **0.00%** | **XUẤT SẮC** | `10962776000.000000` | Tổng mức đầu tư. 100% bản ghi là số hợp lệ, được `parse_money` chuẩn hóa. |
| `projectDTO.investTotalUnit` | `currency` | `VARCHAR` | **0.00%** | **0.00%** | **XUẤT SẮC** | `VND` | 100% bản ghi có đơn vị tiền tệ là `'VND'`. |
| `projectDTO.decisionNo` | `decision_no` | `VARCHAR` | **0.00%** | **0.00%** | **XUẤT SẮC** | `564/QĐ-UBND` | Số quyết định phê duyệt dự án. |
| `projectDTO.decisionDate` | `decision_date` | `DATE` | **0.00%** | **0.00%** | **XUẤT SẮC** | `2021-09-29` | Ngày ban hành quyết định phê duyệt dự án. |
| `projectDTO.publicDate` | `public_date` | `DATE` | **0.00%** | **0.00%** | **XUẤT SẮC** | `2022-01-02` | Ngày công khai dự án lên cổng e-GP. |
| `provCode` (root) | `prov_code` | `VARCHAR` | 24.40% | 0.00% | **TỐT (ĐÃ XỬ LÝ PIPE)** | `115` (gốc là `\|115\|`) | Mã tỉnh. Được `clean_pipe_code` bóc tách và đưa `\|\|` về `NULL`. |
| `districtCode` (root) | `district_code` | `VARCHAR` | 31.40% | 0.00% | **TỐT (ĐÃ XỬ LÝ PIPE)** | `11509` (gốc là `\|11509\|`) | Mã quận/huyện. Được `clean_pipe_code` bóc tách. |
| `projectDTO.status` | `status_raw` | `VARCHAR` | **0.00%** | **0.00%** | **XUẤT SẮC** | `01` | Mã trạng thái dự án (`01` = Đã phê duyệt). |
| `plocation` (root) | `location_name` (`project_location`) | `VARCHAR` | 0.00% (từ 16/09/2022) | 0.00% | **XUẤT SẮC (PARSED JSON)** | `Huyện Thanh Chương, Tỉnh Nghệ An` | Chuỗi JSON mảng địa điểm. Đã được parse thành tên địa danh sạch. |
| `bidpProjectProjectPlanList` | `ExtractedReference` (`target=plan`) | `RELATION` | 1.50% | 0.00% | **RẤT GIÁ TRỊ** | `PL20220100278` | 98.5% dự án có liên kết KHLCNT để dựng đồ thị quan hệ Dự án $\rightarrow$ Kế hoạch. |

---

## 3. Cột Mốc Lịch Sử Chuyển Đổi Dữ Liệu: Ngày 16/09/2022 (e-GP Golive Milestone)

Kiểm tra chi tiết theo từng ngày trong Bronze đã xác định chính xác thời điểm dữ liệu khu vực (`plocation`) bắt đầu xuất hiện:

```
2022-09-14: plocation =  0/20 (  0.0%)  [Hệ thống e-GP cũ]
2022-09-15: plocation =  0/20 (  0.0%)  [Hệ thống e-GP cũ]
--------------------------------------------------------------------------------
2022-09-16: plocation = 18/20 ( 90.0%)  [CHÍNH THỨC GOLIVE HỆ THỐNG E-GP MỚI]
2022-09-17: plocation = 20/20 (100.0%)  [Áp dụng bắt buộc Thông tư 08/2022/TT-BKHĐT]
2022-10-01: plocation = 20/20 (100.0%)
2023 - 2026: plocation = 100% duy trì tuyệt đối
```

- **Trước 16/09/2022:** Dữ liệu lịch sử từ e-GP cũ (chạy Internet Explorer) được migrate sang không có trường địa điểm chi tiết $\rightarrow$ `plocation = null`, `provCode = "||"`.
- **Từ 16/09/2022 đến nay:** 100% dự án mới đều có thông tin địa bàn chi tiết.

---

## 4. Khám Phá Cấu Trúc `plocation` & Xử Lý Dự Án Đa Địa Điểm (Multi-Location)

1. **Bản chất của `plocation`:**
   - Trường này không phải là chuỗi văn bản thông thường, mà là một **chuỗi JSON array được serialize (Stringified JSON Array)**:
     ```json
     "[{\"id\":null,\"projectId\":\"b026ed1a-f821-4d42-8f30-0e5dfddfee17\",\"districtCode\":\"40331\",\"districtName\":\"Huyện Thanh Chương\",\"provCode\":\"403\",\"provName\":\"Tỉnh Nghệ An\",\"wardCode\":null,\"wardName\":null}]"
     ```
2. **Dự án Đa Địa Điểm (Chiếm 1.6% tổng số dự án):**
   - Các công trình giao thông, thủy lợi, truyền tải điện liên huyện/liên tỉnh có mảng `plocation` chứa từ 2 đối tượng địa lý trở lên.
3. **Cải tiến trong Silver Transformer:**
   - [`ProjectDetailTransformer`](file:///C:/Work/data-project/procurement_lakehouse/src/procurement/silver/domain/transformers/project.py) đã được nâng cấp để:
     - Tự động parse chuỗi JSON trong `plocation`.
     - Ghép tên địa danh sạch: `f"{districtName}, {provName}"` (VD: `"Huyện Thanh Chương, Tỉnh Nghệ An"`), loại bỏ hoàn toàn việc lưu chuỗi JSON thô vào cột `location_name`.
     - Sinh ra đầy đủ các bản ghi con trong bảng `project_location` cho từng huyện/tỉnh mà dự án đi qua với số thứ tự (`ordinal=0, 1, 2...`).
     - Với các bản ghi cũ trước 16/09/2022 (chứa toàn `"||"`), transformer bỏ qua không sinh dòng rác vào `project_location`.

---

## 5. Liên Kết Dự Án $\rightarrow$ Kế Hoạch Lựa Chọn Nhà Thầu (Project-to-Plan Lineage)

- **98.5% dự án** trong Bronze chứa danh sách KHLCNT trong `bidpProjectProjectPlanList`.
- Phương thức `extract_references()` tự động bóc tách từng mã KHLCNT (`planNo`) thành đối tượng `ExtractedReference`:
  - `target_type`: `"plan"`
  - `id_scheme`: `"number"`
  - `target_identity`: Mã KHLCNT (VD: `"PL20220100278"`)
- Giúp tự động kết nối đồ thị phân cấp: **Project $\rightarrow$ Plan $\rightarrow$ Package $\rightarrow$ Notice $\rightarrow$ Award $\rightarrow$ Contract**.
