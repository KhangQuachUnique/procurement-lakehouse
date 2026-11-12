# HƯỚNG DẪN KẾT NỐI POWER BI & MÔ HÌNH HÓA DỮ LIỆU
## BỘ DỮ LIỆU GOLD LAKEHOUSE - MUA SẮM CÔNG (VIETNAM PUBLIC PROCUREMENT)

---

## 1. TỔNG QUAN BỘ DỮ LIỆU

Bộ dữ liệu này là **Tầng Gold (Dimensional Layer)** đã được làm sạch, chuẩn hóa và tổng hợp từ Hệ thống Mạng đấu thầu Quốc gia (VNEPS / Muasamcong). Dữ liệu được thiết kế theo chuẩn **Star Schema (Mô hình hình sao)** phục vụ trực tiếp cho Báo cáo quản trị, BI Dashboard và Phân tích kinh doanh.

* **Tổng số bảng:** 17 bảng (9 Dimensions + 8 Facts).
* **Tổng số dòng:** **4,354,496 dòng**.
* **Định dạng sẵn có trong thư mục `export/`:**
  1. `export/parquet/`: Gồm 17 file `.parquet` (Power BI đọc trực tiếp cực nhanh, tối ưu bộ nhớ).
  2. `export/duckdb/gold_warehouse.duckdb`: 1 file cơ sở dữ liệu DuckDB duy nhất (dành cho ai dùng Python, DBeaver, Metabase, Streamlit).

---

## 2. HƯỚNG DẪN IMPORT NHANH VÀO POWER BI DESKTOP (3 BƯỚC)

Power BI Desktop từ phiên bản 2021 trở đi đã tích hợp sẵn **Native Parquet Connector**, bạn **không cần cài thêm bất kỳ driver nào**.

### Cách 1: Nạp trực tiếp từng file Parquet (Khuyên dùng)
1. Mở **Power BI Desktop**.
2. Trên thanh Ribbon, chọn **Get Data** $\rightarrow$ chọn **More...** $\rightarrow$ gõ tìm **Parquet** $\rightarrow$ nhấn **Connect**.
3. Chọn đường dẫn tới file parquet trong thư mục `export/parquet/` (ví dụ `fact_savings_daily.parquet`) $\rightarrow$ chọn **Load**.
4. Lặp lại với các bảng Facts và Dimensions cần dùng (hoặc xem Cách 2 để load hàng loạt).

### Cách 2: Nạp hàng loạt tất cả file trong 1 bước (Load Folder)
1. Chọn **Get Data** $\rightarrow$ chọn **Folder** $\rightarrow$ nhấn **Connect**.
2. Dán đường dẫn thư mục: `...\export\parquet` $\rightarrow$ nhấn **OK**.
3. Nhấn **Transform Data** (để vào Power Query):
   * Với mỗi file, bạn chỉ cần bấm vào chữ `Binary` ở cột `Content` của dòng tương ứng, Power BI sẽ tự mở ra bảng dữ liệu chuẩn.

---

## 3. THIẾT KẾ MỐI QUAN HỆ (DATA MODELING / STAR SCHEMA)

Trong giao diện **Model view** (tab thứ 3 bên thanh công cụ trái của Power BI), hãy kéo thả các khóa ngoại (Foreign Keys) từ bảng **Fact** nối với khóa chính (Primary Key) của bảng **Dimension**. 

Tất cả quan hệ đều là **1 - Many (1 đến nhiều)**, hướng lọc **Single (Một chiều từ Dimension sang Fact)**:

```
                  ┌──────────────────────┐
                  │       dim_date       │
                  │ (PK: date_key)       │
                  └──────────┬───────────┘
                             │ 1
                             │ * (report_date)
  ┌──────────────────────────┼──────────────────────────┐
  │                          │                          │
┌─┴────────────────────┐   ┌─┴────────────────────┐   ┌─┴────────────────────┐
│ fact_savings_daily   │   │fact_bidding_compet...│   │  fact_demand_daily   │
└─┬────────────────────┘   └─┬────────────────────┘   └─┬────────────────────┘
  │ * (geography_key)        │ * (geography_key)        │ * (geography_key)
  │ 1                        │ 1                        │ 1
┌─┴──────────────────────────┴──────────────────────────┴────────────────────┐
│                               dim_geography                                │
│                           (PK: geography_key)                              │
└────────────────────────────────────────────────────────────────────────────┘
```

### Bảng tra cứu các khóa kết nối (Relationship Mapping):

| Từ bảng Fact | Cột Foreign Key | Đến bảng Dimension | Cột Primary Key | Ý nghĩa |
| :--- | :--- | :--- | :--- | :--- |
| **Mọi bảng Fact** | `report_date` | `dim_date` | `date_key` | Lọc theo Năm, Quý, Tháng, Tuần, Thứ |
| **Mọi bảng Fact** (trừ duration, status) | `geography_key` | `dim_geography` | `geography_key` | Lọc theo 63 Tỉnh/Thành, Vùng kinh tế |
| **Mọi bảng Fact** | `sector_key` | `dim_sector` | `sector_key` | Lọc theo Lĩnh vực (Xây lắp, Hàng hóa, Tư vấn...) |
| **Mọi bảng Fact** (trừ demand, status) | `method_key` | `dim_procurement_method` | `method_key` | Hình thức (Đấu thầu rộng rãi, Chỉ định thầu, Qua mạng...) |
| `fact_savings_daily`, `fact_demand_daily`, `fact_bidding_competition_daily` | `value_band_key` | `dim_value_band` | `band_key` | Phân nhóm quy mô gói thầu (Nhỏ, Vừa, Lớn) |
| `fact_savings_daily`, `fact_award_daily`, `fact_demand_daily`, `fact_supplier_daily` | `currency_key` | `dim_currency` | `currency_key` | Đơn vị tiền tệ (VND, USD...) |
| `fact_duration_daily` | `metric_key` | `dim_duration_metric` | `metric_key` | Loại chỉ số chu trình thời gian |
| `fact_supplier_daily` | `party_key` | `dim_party` | `party_key` | Tên/Mã cơ quan, Chủ đầu tư, Nhà thầu |
| `fact_status_daily` | `status_key` | `dim_progress_status` | `status_key` | Trạng thái tiến độ thực hiện |

---

## 4. TỪ ĐIỂN DỮ LIỆU TÓM TẮT (DATA DICTIONARY)

### A. Các bảng Fact (Số liệu đo lường)

1. **`fact_savings_daily` (Hiệu quả tiết kiệm & Hợp đồng - 207,680 dòng):**
   * `verified_baseline_sum`: Tổng giá gói thầu dự toán được duyệt (VNĐ).
   * `verified_award_sum`: Tổng giá trúng thầu (VNĐ).
   * `verified_saving_sum`: Tiết kiệm dự toán = `verified_baseline_sum - verified_award_sum`.
   * `contract_sample_count`: Số gói đã ký hợp đồng thực tế.
   * `contract_amount_sum`: Tổng giá trị hợp đồng đã ký kết (VNĐ).
   * `contract_saving_sum`: Tiết kiệm hợp đồng thực tế = `verified_baseline_sum - contract_amount_sum`.

2. **`fact_bidding_competition_daily` (Mở thầu & Mức độ cạnh tranh - Mới):**
   * `opening_count`: Số biên bản mở thầu.
   * `total_bidders`: Tổng số lượt nhà thầu nộp hồ sơ dự thầu.
   * `single_bidder_count`: Số gói chỉ có **duy nhất 1 nhà thầu** tham gia (Chỉ số cảnh báo rủi ro cạnh tranh thấp).
   * `multi_bidder_count`: Số gói có $\ge 2$ nhà thầu cạnh tranh.
   * `tech_passed_count` / `tech_failed_count`: Số nhà thầu đạt / không đạt kỹ thuật.
   * `discounted_bidder_count`: Số nhà thầu có thư giảm giá.
   * `total_bid_amount`: Tổng giá dự thầu ban đầu.
   * `total_discount_amount`: Tổng số tiền giảm giá.
   * `total_final_bid_amount`: Giá dự thầu sau giảm giá.

3. **`fact_demand_daily` (Nhu cầu mua sắm):**
   * `package_count`: Số lượng gói thầu trong kế hoạch.
   * `planned_amount_sum`: Tổng ngân sách dự toán kế hoạch mua sắm.

4. **`fact_notice_daily` (Thông báo mời thầu):**
   * `notice_count`: Số lượng thông báo mời thầu phát hành.
   * `cancelled_notice_count`: Số lượng thông báo mời thầu bị hủy.

5. **`fact_award_daily` (Kết quả trúng thầu):**
   * `award_scope_count`: Số lượt trúng thầu.
   * `award_amount_sum`: Tổng giá trị trúng thầu được phê duyệt.

6. **`fact_duration_daily` (Thời gian & Điểm nghẽn chu trình):**
   * `duration_sum` / `sample_count`: Tổng thời gian và số mẫu đo lường.
   * Nối với `dim_duration_metric` để lọc các chỉ số:
     * `plan_to_notice`: Thời gian từ phê duyệt kế hoạch đến khi đăng thông báo mời thầu.
     * `close_to_open_hours`: Thời gian từ đóng thầu đến mở thầu (giờ).
     * `open_to_award_days`: Thời gian từ mở thầu đến khi có kết quả trúng thầu (ngày).
     * `planned_to_actual_delay_days`: Số ngày trễ tiến độ so với kế hoạch ban đầu.

7. **`fact_supplier_daily` (Phân bổ theo đối tượng / Chủ đầu tư):**
   * `party_key`: Khóa định danh đơn vị (nối `dim_party` để lấy tên đầy đủ, mã số thuế).
   * `award_amount_sum`: Giá trị gói thầu được phân bổ.

---

## 5. CÔNG THỨC DAX GỢI Ý (COPY & PASTE DÙNG NGAY)

Tạo một bảng rỗng tên là `_Measures` và thêm các công thức đo lường sau:

```dax
// 1. Tổng tiền tiết kiệm dự toán (Tỷ VNĐ)
Tong_Tiet_Kiem_TyVND = 
DIVIDE(SUM(fact_savings_daily[verified_saving_sum]), 1000000000, 0)

// 2. Tỷ lệ tiết kiệm đấu thầu (%)
Ty_Le_Tiet_Kiem = 
DIVIDE(
    SUM(fact_savings_daily[verified_saving_sum]),
    SUM(fact_savings_daily[verified_baseline_sum]),
    0
)

// 3. Tỷ lệ gói thầu 1 nhà thầu duy nhất (Rủi ro cạnh tranh thấp)
Ty_Le_1_Nha_Thau = 
DIVIDE(
    SUM(fact_bidding_competition_daily[single_bidder_count]),
    SUM(fact_bidding_competition_daily[opening_count]),
    0
)

// 4. Số nhà thầu bình quân trên 1 gói mở thầu
Nha_Thau_Binh_Quan = 
DIVIDE(
    SUM(fact_bidding_competition_daily[total_bidders]),
    SUM(fact_bidding_competition_daily[opening_count]),
    0
)

// 5. Tỷ lệ tiết kiệm thực tế theo Hợp đồng (%)
Ty_Le_Tiet_Kiem_Hop_Dong = 
DIVIDE(
    SUM(fact_savings_daily[contract_saving_sum]),
    SUM(fact_savings_daily[verified_baseline_sum]),
    0
)

// 6. Thời gian xử lý trung bình (Ngày)
Thoi_Gian_Trung_Binh_Ngay = 
DIVIDE(
    SUM(fact_duration_daily[duration_sum]),
    SUM(fact_duration_daily[sample_count]),
    0
)
```

---

## 6. GỢI Ý CẤU TRÚC BÁO CÁO DASHBOARD (4 TRANG HOÀN CHỈNH)

### Trang 1: Tổng quan Thị trường Mua sắm Công (Executive Overview)
* **Card KPI hàng trên cùng:**
  * Tổng giá trị kế hoạch (`planned_amount_sum`)
  * Tổng giá trị trúng thầu (`award_amount_sum`)
  * Tổng tiền tiết kiệm ngân sách (`Tong_Tiet_Kiem_TyVND`)
  * Tỷ lệ tiết kiệm bình quân (`Ty_Le_Tiet_Kiem`)
* **Biểu đồ cột (Bar Chart):** Cơ cấu giá trị trúng thầu theo Lĩnh vực (`dim_sector[sector_name]`).
* **Bản đồ / Treemap:** Phân bố giá trị gói thầu theo 63 Tỉnh/Thành (`dim_geography[province_name]`).
* **Biểu đồ đường (Line Chart):** Xu hướng mua sắm theo thời gian (`dim_date[year_month]`).

### Trang 2: Phân tích Tiết kiệm Ngân sách & Giá trị Hợp đồng (Savings & Contracts)
* **Waterfall Chart (Biểu đồ thác nước):** Từ `Giá dự toán` $\rightarrow$ `Giảm giá trúng thầu` $\rightarrow$ `Giá trị hợp đồng thực tế`.
* **Scatter Plot (Ma trận phân tán):** Trục X = Tỷ lệ tiết kiệm, Trục Y = Quy mô ngân sách, Chi tiết = Tỉnh thành / Lĩnh vực (Giúp phát hiện đơn vị nào tiết kiệm cao nhất, đơn vị nào tiết kiệm sát trần 0%).
* **Bảng chi tiết (Matrix Table):** So sánh giá trị dự toán, giá trúng thầu, giá hợp đồng theo từng phương thức đấu thầu (`dim_procurement_method[method_category]`).

### Trang 3: Mở thầu & Mức độ Cạnh tranh (Bidding Competition & Risk)
* **Donut Chart:** Tỷ lệ gói 1 nhà thầu duy nhất (`single_bidder_count`) vs Gói nhiều nhà thầu (`multi_bidder_count`).
* **Column Chart:** Số lượng nhà thầu bình quân tham gia theo từng Lĩnh vực (Hàng hóa, Xây lắp, Thuốc...).
* **Thống kê giảm giá:** Tổng giá trị giảm giá qua các đợt mở thầu (`total_discount_amount`).
* **Tỷ lệ đạt kỹ thuật:** Tỷ lệ hồ sơ đạt/không đạt kỹ thuật (`tech_passed_count` vs `tech_failed_count`).

### Trang 4: Hiệu quả Chu trình & Điểm nghẽn Thời gian (Procurement Bottlenecks)
* **Bar Chart:** Thời gian trung bình từ Mở thầu đến khi có Quyết định trúng thầu theo Tỉnh/Lĩnh vực.
* **Line Chart:** Số ngày trễ tiến độ so với kế hoạch (`planned_to_actual_delay_days`) theo từng tháng.
* **Thời gian mở thầu:** Thời gian từ đóng thầu đến mở thầu (`close_to_open_hours`) - Đánh giá tính minh bạch mở thầu online.

---

## 7. MỞ NHANH BẰNG PYTHON / PANDAS / STREAMLIT (NẾU CẦN)

Nếu muốn phân tích sâu bằng Python hoặc Streamlit, bạn chỉ cần 3 dòng code để đọc file DuckDB mà không cần cài thêm database server:

```python
import duckdb

# Kết nối trực tiếp vào file DuckDB đã đóng gói
con = duckdb.connect("export/duckdb/gold_warehouse.duckdb", read_only=True)

# Lấy dữ liệu ra Pandas DataFrame
df_savings = con.execute("""
    SELECT 
        d.year_month,
        s.sector_name,
        SUM(f.verified_saving_sum) / 1e9 AS saving_billion_vnd,
        AVG(f.verified_saving_sum / NULLIF(f.verified_baseline_sum, 0)) * 100 AS saving_rate_pct
    FROM fact_savings_daily f
    JOIN dim_date d ON f.report_date = d.date_key
    JOIN dim_sector s ON f.sector_key = s.sector_key
    GROUP BY d.year_month, s.sector_name
    ORDER BY d.year_month DESC
""").df()

print(df_savings.head(10))
```

---
*Gói dữ liệu được trích xuất tự động từ Procurement Lakehouse Gold Layer.*
