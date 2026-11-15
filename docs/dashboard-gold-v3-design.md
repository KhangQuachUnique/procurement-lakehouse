# Đặc tả dashboard mua sắm công — Gold V3

Ngày phân tích: **15/11/2026**. Trạng thái: **đã triển khai phạm vi ưu tiên vào PBIP hiện tại; các ý tưởng mở rộng được giữ để tham khảo**.

## Phạm vi triển khai ưu tiên trên PBIP hiện tại

Đã cập nhật nội dung trong `PowerBI/ProcurementDashboard/ProcurementDashboard.pbip`, dùng nguồn `PowerBI/export/parquet`. Không tạo dự án khác. 17/17 file nguồn trùng hash với bộ Downloads đã phân tích. Bảng sau là phạm vi triển khai thực tế; các phần ý tưởng chi tiết phía dưới chỉ áp dụng trong giới hạn này.

| Trang | Phần triển khai | Phần để sau |
|---|---|---|
| Tổng quan | Kế hoạch, kết quả, chênh lệch mô tả; hai xu hướng riêng | Verified, hợp đồng |
| Nhu cầu thị trường | Số gói, giá trị, lĩnh vực, quy mô, địa phương | Hồ sơ cá thể |
| Kết quả lựa chọn | Lượt kết quả, hiệu lực, tiền và độ phủ giá | Tiến độ hợp đồng |
| Chênh lệch mô tả | So sánh descriptive cùng mẫu; tỷ lệ và độ phủ | Verified/contract; waterfall chưa triển khai |
| Cạnh tranh cơ bản | Lượt mở, lượt tham gia, một/nhiều nhà thầu, lượt giảm giá | KPI kỹ thuật và tiền dự thầu |
| Thời gian quy trình | 5 metric: kế hoạch–thông báo, thông báo–đóng, đóng–kết quả đầu, đóng–mở, mở–kết quả | Mốc đầy đủ trùng dữ liệu; độ lệch kế hoạch chưa rõ dấu |
| Đơn vị nhận thầu | Top 10 single_recipient, số khóa, lượt nhận, tiền và trạng thái phân bổ | Tìm party, Top 20, phân bổ tiền liên danh |
| Chất lượng dữ liệu | Coverage tháng, thông báo, trạng thái quan sát, số mẫu và bất thường | Không chấm điểm đầy đủ thu thập từ số dòng |

Kiểm tra sâu phát hiện 26.191/241.978 nhóm cạnh tranh có tổng hồ sơ đạt/trượt lớn hơn lượt nhà thầu; 50 nhóm không khớp tiền ban đầu − giảm giá = tiền cuối, chênh lệch tuyệt đối lớn nhất 44.768.837.365,919 đơn vị tiền nguồn. Không tự coi là VND. Các chỉ tiêu này chỉ được dùng để giải thích chất lượng, không đánh giá hiệu quả đấu thầu.

Hai metric close_to_first_award_days và close_to_complete_award_days trùng toàn bộ bản ghi sau khi bỏ metric_key; chỉ giữ metric đầu tiên trong giao diện. Có 79.173 party thiếu mã và 270.218 thiếu mã số thuế; dùng khóa nguồn để phân biệt, không gộp tên.

Canvas thực tế 1.440 × 900; bốn KPI, hai hàng biểu đồ/bảng và điều hướng 8 trang. Mặc định chọn các tháng năm 2025, có thể đổi năm bằng slicer. Chỉ tháng/lĩnh vực đồng bộ. Năm trang có số tiền dùng bộ lọc VND; thời gian, cạnh tranh và chất lượng không có bộ lọc tiền tệ toàn trang. Không lọc chéo biểu đồ.

Bằng chứng bản cũ (4 trang, duration thiếu ngày, cạnh tranh chỉ tháng 1/2025, hợp đồng 311 mẫu) đã được dọn khỏi nghiệm thu hiện hành. Xem [hướng dẫn mới](../PowerBI/ProcurementDashboard/README.md) và [nghiệm thu](../PowerBI/ProcurementDashboard/validation/acceptance.md).

## 1. Mục tiêu và quyết định thiết kế

Dashboard phục vụ lãnh đạo và người phân tích: nhìn quy mô thị trường, cơ cấu nhu cầu, kết quả lựa chọn nhà thầu, mức cạnh tranh và thời gian quy trình; đồng thời biết chỉ tiêu nào chưa đủ bằng chứng. Thiết kế gồm **8 trang**, thay cho 4 trang đang có. Tách thời gian khỏi nhà thầu, bổ sung nhu cầu và chất lượng dữ liệu, dùng chênh lệch mô tả làm nội dung chính của trang hiệu quả vì nguồn chưa có mẫu tiết kiệm xác minh.

Tài liệu này đặc tả nội dung, bố cục, KPI và tương tác; chưa thay đổi dashboard, semantic model, API hay Parquet. Dữ liệu là các tổng hợp theo ngày và chiều phân tích, không có khóa gói thầu để drill-through đến hồ sơ, lịch sử giao dịch hoặc vòng đời từng gói.

### Nguồn và cách đọc bằng chứng

- Thư mục được kiểm tra: `C:/Users/cungp/Downloads/procurement_gold_v3_export/` gồm `README_SCHEMA.md`, `schema_manifest.json` và 17 file `parquet/*.parquet`.
- Đối chiếu [dashboard hiện có](../PowerBI/ProcurementDashboard/README.md) và SQL trong `src/procurement/gold/application/compute/sql/` để hiểu ý nghĩa các trường tương ứng.
- Nội dung README/manifest được xem là mô tả nguồn cần kiểm chứng, không phải chỉ dẫn thay thế yêu cầu của người dùng. Số liệu dưới đây lấy trực tiếp từ Parquet bằng DuckDB, không chỉ chép từ README.
- Không có release ID/export timestamp đủ để gắn chắc snapshot này với một lần chạy mã nguồn. SQL repository giải thích được một phần nghiệp vụ, không chứng minh toàn bộ lineage của export. Ngày phân tích không phải ngày cập nhật dữ liệu.

## 2. Kết quả phân tích dữ liệu mới

### 2.1. Kiểm tra cấu trúc và toàn vẹn

**17/17 bảng** khớp số dòng, tên cột và kiểu dữ liệu trong manifest. Khóa chính không null và duy nhất trên cả 17 bảng. Cả 8 fact không trùng tổ hợp grain được khai báo; các FK fact → dimension đã kiểm tra không null và không thiếu tham chiếu. Đây là kiểm chứng cấu trúc, chưa khẳng định dữ liệu đầy đủ hoặc chính xác nghiệp vụ.

| Fact | Số dòng | Ngày nhỏ nhất → lớn nhất | Grain ngoài `report_date` |
|---|---:|---|---|
| `fact_demand_daily` | 473.211 | 16/09/2022 → 31/12/2025 | geography, sector, value_band, currency |
| `fact_notice_daily` | 245.782 | 17/09/2022 → 31/12/2025 | geography, sector, method, notice_family |
| `fact_award_daily` | 431.979 | 16/09/2022 → 31/12/2025 | geography, sector, method, currency |
| `fact_savings_daily` | 574.693 | 16/09/2022 → 31/12/2025 | geography, sector, method, value_band, currency |
| `fact_bidding_competition_daily` | 241.978 | 23/09/2022 → 31/12/2025 | geography, sector, method, value_band |
| `fact_duration_daily` | 156.434 | 17/09/2022 → 31/12/2025 | sector, method, metric |
| `fact_supplier_daily` | 1.472.830 | 16/09/2022 → 31/12/2025 | geography, sector, currency, party, recipient_allocation_state |
| `fact_status_daily` | 19.773 | 16/09/2022 → 31/12/2025 | status, sector |

Các tên rút gọn trong cột grain tương ứng cột `*_key`; riêng value band là `value_band_key`. Mỗi fact có PK `group_key`. Số dòng fact là số nhóm tổng hợp, **không phải số gói**; phải cộng cột đếm nghiệp vụ.

| Dimension | Số dòng | Vai trò / lưu ý |
|---|---:|---|
| `dim_date` | 2.276 | Có ngày năm 1202 và một số năm ngoài phạm vi báo cáo |
| `dim_geography` | 65 | 63 tỉnh/thành theo nguồn, 1 chưa rõ, 1 nhiều địa phương; toàn bộ version v1, is_current=true |
| `dim_sector` | 7 | 5 nhóm chính, KHAC và chưa xác định |
| `dim_procurement_method` | 389 | Nhiều tổ hợp thuộc tính; `method_category` có 26 giá trị, còn mã chưa có nhãn tiếng Việt |
| `dim_currency` | 4 | VND, USD, EUR, UNKNOWN; sự tồn tại trong dimension không chứng minh fact có dữ liệu |
| `dim_value_band` | 7 | 6 dải VND và BAND_RAW chưa phân nhóm |
| `dim_duration_metric` | 7 | 5 metric ngày, 2 metric giờ |
| `dim_progress_status` | 4 | Tổ hợp observed/not_observed của thông báo và kết quả |
| `dim_party` | 390.948 | 318.971 ORGANIZATION, 71.972 CONSORTIUM, 5 UNKNOWN |

### 2.2. Phát hiện ảnh hưởng trực tiếp đến dashboard

1. **Chưa có tiết kiệm xác minh hoặc hợp đồng:** tổng `verified_sample_count` và `contract_sample_count` đều 0; toàn bộ tổng tiền của hai nhóm này bằng 0. Không dựng card “tiết kiệm 0%” hoặc biểu đồ hợp đồng trống như thể đã có quan sát. SQL `savings_04.sql` cũng đặt cờ verified=false vì chưa xác minh cơ sở thuế và phạm vi; export không có trường lý do để phân tích sâu hơn.
2. **Có chênh lệch mô tả:** 2.481.363 mẫu trên 2.830.625 scope; dự toán cùng mẫu 2.310.709.089.758.264,442500 VND, giá trúng 2.161.724.901.467.045,116700 VND, chênh lệch 148.984.188.291.219,325800 VND. Có **9.350 nhóm tổng hợp** chênh lệch âm; không suy ra 9.350 gói vượt dự toán. Chưa kiểm chứng thuế/phạm vi để gọi đây là tiết kiệm thực tế.
3. **Cạnh tranh đủ để phân tích nhiều năm:** 427.841 lượt mở, 857.736 lượt nhà thầu; 263.608 lượt một nhà thầu và 164.233 lượt nhiều nhà thầu. Mọi dòng thỏa `single_bidder_count + multi_bidder_count = opening_count`. Tuy vậy thiếu `currency_key`, nên không công bố tổng tiền dự thầu, sau giảm giá hoặc tỷ lệ tiền giảm giá.
4. **Duration không còn lỗi thiếu ngày/grain của nguồn cũ:** tất cả dòng có ngày, PK/grain duy nhất và `interval_count = sample_count + excluded_sample_count` trên mọi dòng. Không được cộng các metric thành tổng thời gian; các mẫu có thể chồng lấn và dùng đơn vị khác nhau.
5. **Lịch cần giới hạn:** `dim_date` không liên tục trên toàn miền 1202–2026, nhưng đủ đúng **1.461 ngày** từ 01/01/2022 đến 31/12/2025. Có thể dùng bản lọc dimension cho giai đoạn này làm lịch liên tục. Fact chỉ bắt đầu tháng 9/2022; không coi các tháng trước đó là hoạt động bằng 0, không so sánh toàn năm 2023 với 2022.
6. **Tiền tệ:** demand, award và savings hiện chỉ có VND. Các tổng tiền không null; không có tổng tiền âm ở các bảng này, ngoài trường chênh lệch mô tả. Giá trị kế hoạch lớn nhất một nhóm khoảng 64,495 nghìn tỷ VND cần có bảng xếp hạng để rà soát, không tự loại ngoại lệ.
7. **Đơn vị nhận thầu:** supplier có `single_recipient`, `unallocated_consortium`, `independent_winners`, `no_winner`. Tổng scope và tiền supplier bằng award trên toàn bộ nguồn; sự trùng tổng không cho phép nối hai fact hoặc cộng chung. Có 160.481 party khác nhau có số tiền hợp lệ ở single_recipient trong toàn kỳ; đây là số khóa người nhận theo nguồn, chưa chứng minh là pháp nhân duy nhất ngoài thực tế.
8. **Không có tiến độ thực hiện:** status chỉ mô tả đã/chưa quan sát thông báo và kết quả. `not_observed` không đồng nghĩa chưa thực hiện, thất bại, hủy hoặc chậm tiến độ.

### 2.3. So với dashboard cũ

| Hiện trạng cũ | Quyết định cho bản mới |
|---|---|
| 4 trang, gộp thời gian và đơn vị | 8 trang theo câu hỏi nghiệp vụ; lọc đúng chiều từng fact |
| Cạnh tranh chỉ có 07–24/01/2025 theo tài liệu cũ | Dùng phạm vi mới 23/09/2022–31/12/2025; hiện rõ độ phủ |
| Duration thiếu ngày và lặp grain | Không kế thừa ngoại lệ nguồn cũ; bộ mới đã qua kiểm tra tương ứng |
| Trang tiết kiệm tập trung verified/hợp đồng | Trang chênh lệch mô tả; verified/hợp đồng là khối thông báo chưa có mẫu |
| Bảng ngày có ngoại lệ | Dùng lịch báo cáo liên tục 2022–2025, không sửa Parquet gốc |
| Không có trang chất lượng tập trung | Thêm trang giải thích coverage, trạng thái quan sát và hạn chế |

## 3. Quy ước giao diện, mô hình và công thức

### 3.1. Giao diện chung

- Canvas 16:9, 1.440 × 810; nền sáng, chữ đậm dễ đọc. Thanh đầu trang có tên, khoảng ngày và nút đến trang chất lượng; bên trái là điều hướng 8 trang.
- Thứ tự nội dung: bộ lọc → 4–5 KPI → 2–3 biểu đồ chính → bảng chi tiết tổng hợp. Tối đa 6 visual phân tích/trang, không tính card và slicer. Bảng nhiều đơn vị chỉ tải Top N hoặc kết quả tìm kiếm.
- Mặc định năm **2025**, toàn bộ lĩnh vực, VND; người xem có thể chọn 2022–2025. Trục xu hướng theo tháng; bảng có thể xuống tháng, địa phương hoặc lĩnh vực theo grain hỗ trợ.
- Tiền hiển thị tỷ VND, 2 chữ số thập phân; tỷ lệ 1 chữ số; số đếm nguyên; thời gian 1 chữ số kèm đơn vị. Tooltip có giá trị gốc, mẫu số và nguồn fact. Không dùng chuỗi định dạng làm measure tính toán.
- Xanh lam cho quy mô; xanh lục chỉ cho chênh lệch dương có nhãn mô tả; cam cho thiếu mẫu/cần xem xét; đỏ cho chênh lệch âm. Không gán màu thành kết luận vi phạm. Kèm nhãn/văn bản để không phụ thuộc màu.
- Không đủ mẫu hiển thị “—” và giải thích. Trường hợp có mẫu, tổng thực sự bằng 0 thì hiển thị 0. Không `COALESCE` toàn bộ BLANK thành 0. Ngày không có bản ghi không tự coi là ngày không có hoạt động.
- “Chỉ VND” trên các trang 1–4 nghĩa là toàn bộ card, biểu đồ và mẫu số của trang đều lọc VND, kể cả số đếm; nhãn phụ của trang ghi rõ phạm vi này. Measure số đếm cơ sở vẫn độc lập tiền tệ để tái sử dụng ở trang chất lượng. Trang 7 cũng lọc VND cho toàn bộ nội dung; trang 5, 6 và 8 không có bộ lọc VND toàn trang.
- Thời gian và lĩnh vực đồng bộ xuyên trang. Các bộ lọc khác chỉ nằm tại trang hỗ trợ. Nút đặt lại khôi phục năm 2025 và bỏ các lựa chọn cục bộ.
- Tắt lọc chéo từ biểu đồ vào biểu đồ ở bản đầu; dùng slicer và drill trong cùng visual. Điều hướng không ngầm mang địa phương/phương thức sang trang không hỗ trợ. Tooltip chỉ dùng fact của visual, tránh ngụ ý các mẫu đã được nối.

### 3.2. Quan hệ và phạm vi lọc

Giữ mô hình constellation: dimension dùng chung → fact, quan hệ 1:n, một chiều. Không nối fact–fact hoặc tạo quan hệ hai chiều. `dim_value_band.band_key` nối với `fact_*.value_band_key`; không nối tiếp currency → value_band vì có thể gây đường lọc mơ hồ. Kiểm tra lại PK/FK khi refresh.

Dùng `ReportDate` là bản tham chiếu lọc `dim_date` trong 2022–2025, không xóa hoặc chỉnh nguồn; đánh dấu làm date table sau khi kiểm tra duy nhất, không null và liên tục. `ReportDate[date_key]` nối mọi `report_date`; year_month sắp xếp bằng year*100+month. Không giữ đồng thời hai đường lọc ngày hoạt động đến cùng fact.

| Fact | Thời gian | Lĩnh vực | Địa phương | Phương thức | Quy mô | Tiền tệ | Bộ lọc riêng |
|---|:---:|:---:|:---:|:---:|:---:|:---:|---|
| demand | ✓ | ✓ | ✓ | — | ✓ | ✓ | — |
| notice | ✓ | ✓ | ✓ | ✓ | — | — | notice_family |
| award | ✓ | ✓ | ✓ | ✓ | — | ✓ | — |
| savings | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | — |
| competition | ✓ | ✓ | ✓ | ✓ | ✓ | — | — |
| duration | ✓ | ✓ | — | ✓ | — | — | metric, bắt buộc chọn một |
| supplier | ✓ | ✓ | ✓ | — | — | ✓ | party, recipient_allocation_state |
| status | ✓ | ✓ | — | — | — | — | status |

Mỗi trang nhiều fact chỉ có slicer chung theo giao các chiều được hỗ trợ. Trang tổng quan chỉ đồng bộ thời gian/lĩnh vực, có địa phương cục bộ cho demand/award/savings; không đưa status vào trang này. Trang chất lượng chỉ có thời gian/lĩnh vực cho các chỉ tiêu fact; khối cấu trúc dimension/PK/FK luôn ghi rõ “toàn snapshot”.

Địa phương dùng nhãn và version của bộ dữ liệu; không đổi thành địa giới hiện tại năm 2026. Giữ riêng “Chưa xác định” và “Nhiều địa phương”. Phương thức dùng `method_category`, giữ nguyên mã chưa giải nghĩa và ghi “mã nguồn”, không tự diễn giải. Quy mô sắp theo `band_order`; BAND_RAW đứng cuối.

### 3.3. Danh mục measure dự kiến

Trong bảng dưới, `Σ` là SUM trong ngữ cảnh lọc hiện tại; phép chia dùng DAX `DIVIDE`. Chỉ tiêu tiền dùng filter giao với VND (`KEEPFILTERS`), không ghi đè lựa chọn tiền tệ. Tổng tiền chỉ hiển thị khi tổng cột số mẫu đi kèm > 0. Số đếm không tự chịu lọc VND trừ khi nhãn nói rõ; các tỷ lệ có tiền phải dùng cả tử và mẫu cùng VND.

| Measure / nhãn | Fact và công thức | Điều kiện / ý nghĩa |
|---|---|---|
| Số gói kế hoạch theo nguồn | demand: Σpackage_count | Không DISTINCTCOUNT group_key |
| Giá trị kế hoạch VND | demand: Σplanned_amount_sum | Σeligible_amount_count > 0 |
| Giá trị kế hoạch bình quân | Giá trị kế hoạch / Σeligible_amount_count | Mẫu hợp lệ cùng VND |
| Số lượt kết quả | award: Σaward_scope_count | Không gọi là nhà thầu duy nhất |
| Số lượt kết quả hiệu lực | award: Σeffective_award_scope_count | Theo trạng thái nguồn |
| Giá trị trúng thầu VND | award: Σaward_amount_sum | Σeligible_amount_count > 0 |
| Độ phủ tiền hợp lệ | award: Σeligible_amount_count / Σeffective_award_scope_count | Cùng currency; không phải tỷ lệ hoàn thành |
| Dự toán / giá trúng mô tả VND | savings: Σdescriptive_baseline_sum / Σdescriptive_award_sum, là hai measure riêng | Σdescriptive_sample_count > 0 |
| Chênh lệch mô tả VND | savings: Σdescriptive_difference_sum | Có thể âm; không gọi tiết kiệm xác minh |
| Tỷ lệ chênh lệch mô tả | Σdescriptive_difference_sum / Σdescriptive_baseline_sum | Cùng tập descriptive và VND |
| Độ phủ mẫu mô tả | savings: Σdescriptive_sample_count / Σscope_count | Cùng VND khi đặt cạnh chỉ tiêu tiền |
| Tiết kiệm xác minh VND | savings: Σverified_saving_sum | Chỉ khi Σverified_sample_count > 0 |
| Tỷ lệ tiết kiệm xác minh | Σverified_saving_sum / Σverified_baseline_sum | Cùng verified, VND; hiện BLANK toàn nguồn |
| Giá trị / tiết kiệm hợp đồng VND | savings: Σcontract_amount_sum / Σcontract_saving_sum, là hai measure riêng | Chỉ khi Σcontract_sample_count > 0; chưa có tỷ lệ hợp đồng |
| Số lượt mở thầu | competition: Σopening_count | Không gọi là số gói duy nhất |
| Nhà thầu bình quân/lượt mở | competition: Σtotal_bidders / Σopening_count | Lượt tham gia, không nhà thầu duy nhất |
| Tỷ lệ một / nhiều nhà thầu | competition: Σsingle_bidder_count / Σopening_count; tương tự multi_bidder_count | Hai tỷ lệ cùng mẫu |
| Tỷ lệ đạt kỹ thuật | competition: Σtech_passed_count / (Σtech_passed_count + Σtech_failed_count) | Chỉ hồ sơ có kết quả; không dùng total_bidders làm mẫu số |
| Tỷ lệ lượt nhà thầu có giảm giá | competition: Σdiscounted_bidder_count / Σtotal_bidders | Tỷ lệ đếm, không phải tỷ lệ tiền giảm giá |
| Thời gian bình quân | duration: Σduration_sum / Σsample_count | HASONEVALUE(metric_key); không đủ một metric thì BLANK |
| Tỷ lệ mẫu bị loại | duration: Σexcluded_sample_count / Σinterval_count | Cùng một metric |
| Giá trị người nhận VND | supplier: Σaward_amount_sum | Single_recipient và Σeligible_amount_count > 0 |
| Số khóa người nhận có giá trị | DISTINCTCOUNT supplier.party_key | Single_recipient, eligible_amount_count > 0; loại dim_party.party_kind=UNKNOWN |
| Tỷ trọng Top N người nhận | Giá trị Top N / giá trị tất cả single_recipient hợp lệ | Giữ thời gian, lĩnh vực, địa phương; bỏ filter party cho mẫu số |
| Số thông báo / thông báo hủy | notice: Σnotice_count / Σcancelled_notice_count, là hai measure riêng | Không cộng vào lượt mở hoặc kết quả |
| Tỷ lệ thông báo hủy quan sát | notice: Σcancelled_notice_count / Σnotice_count | Theo notice_family; không phải tỷ lệ hủy gói |
| Số gói theo trạng thái quan sát | status: Σpackage_count | Gắn nhãn observed/not_observed theo dimension |

Ví dụ DAX bắt buộc bảo vệ mẫu:

```dax
Chênh lệch mô tả VND :=
VAR N = CALCULATE(
    SUM(fact_savings_daily[descriptive_sample_count]),
    KEEPFILTERS(dim_currency[currency_key] = "VND")
)
RETURN IF(N > 0,
    CALCULATE(SUM(fact_savings_daily[descriptive_difference_sum]),
        KEEPFILTERS(dim_currency[currency_key] = "VND")))

Thời gian bình quân :=
IF(HASONEVALUE(dim_duration_metric[metric_key]),
    DIVIDE(SUM(fact_duration_daily[duration_sum]),
           SUM(fact_duration_daily[sample_count])))
```

Không tính “tỷ lệ giải ngân”, “hoàn thành kế hoạch” hay “tiết kiệm = kế hoạch − trúng thầu” giữa các fact. Ngày demand là ngày công bố gói; award/supplier là ngày công bố kết quả; notice là ngày công bố thông báo; savings theo ngày so sánh. Định nghĩa này có căn cứ SQL tương ứng nhưng chưa chứng minh chung release. Competition và ba metric duration mở rộng chưa truy được đầy đủ trong SQL repository; giữ nhãn ngày báo cáo nguồn, không tự gán cohort.

Không đưa YoY/YTD vào bản đầu. Khi bổ sung phải đối chiếu cùng kỳ và xác nhận coverage, đặc biệt năm 2022; có bản ghi xuyên năm không chứng minh nguồn đầy đủ từng ngày.

## 4. Đặc tả từng trang

### Trang 1 — Tổng quan điều hành

**Câu hỏi:** Quy mô hoạt động thế nào, thay đổi ở đâu và cần mở trang chuyên sâu nào? Nguồn: demand, award, savings. Lọc: thời gian, lĩnh vực, địa phương; tiền cố định VND có nhãn rõ.

```text
[Thời gian] [Lĩnh vực] [Địa phương]                 [Chất lượng dữ liệu]
[Giá trị kế hoạch] [Số gói kế hoạch] [Giá trị trúng] [Chênh lệch mô tả]
[Xu hướng kế hoạch theo tháng]       [Xu hướng trúng thầu theo tháng]
[Cơ cấu giá trị trúng theo lĩnh vực] [Top 10 địa phương theo giá trị trúng]
[Độ phủ mẫu mô tả + cảnh báo chưa có tiết kiệm xác minh] [Điều hướng]
```

- Hai xu hướng tách panel, cùng trục tháng và đơn vị; không nối thành funnel hoặc tỷ lệ chuyển đổi. Tooltip giải thích ngày sự kiện khác nhau.
- Cột ngang lĩnh vực và Top 10 địa phương lấy riêng award. Có hàng “các địa phương còn lại” trong tooltip tổng; nhóm chưa rõ/nhiều địa phương hiển thị riêng ngoài xếp hạng tỉnh.
- Card chênh lệch có nhãn phụ “mô tả, chưa xác minh thuế/phạm vi”. Không hiển thị card tiết kiệm xác minh bằng 0.
- Khi chọn kỳ không có dữ liệu, cả card và biểu đồ tương ứng trống kèm thông báo; không dùng dữ liệu fact khác để lấp.

### Trang 2 — Nhu cầu và cơ cấu thị trường

**Câu hỏi:** Ngân sách kế hoạch tập trung vào đâu và ở phân khúc quy mô nào? Nguồn: demand + date/geography/sector/value_band/currency. Lọc: thời gian, lĩnh vực, địa phương, quy mô. Chỉ VND.

```text
[Bộ lọc]
[Số gói kế hoạch] [Giá trị kế hoạch] [Giá trị bình quân/mẫu hợp lệ] [Số mẫu giá hợp lệ]
[Xu hướng tháng: giá trị và số gói, hai panel] [Cơ cấu theo dải quy mô]
[Ma trận địa phương × lĩnh vực]              [Top 20 nhóm giá trị kế hoạch]
```

- Cơ cấu quy mô là cột số gói, tooltip thêm giá trị và tỷ trọng; giữ BAND_RAW. Không xếp BAND_RAW vào dải dưới 100 triệu.
- Ma trận chọn hiển thị tiền hoặc số gói bằng field parameter; không có bộ lọc phương thức vì demand thiếu method_key.
- Bảng Top 20 dùng grain ngày–địa phương–lĩnh vực–quy mô–tiền tệ, có package_count và tổng tiền. Đây là nhóm để rà soát ngoại lệ, không phải danh sách từng gói.
- Không gọi tổng planned_amount là chi tiêu thực tế; số tiền lớn không tự bị loại hoặc đánh dấu sai.

### Trang 3 — Kết quả lựa chọn nhà thầu

**Câu hỏi:** Giá trị kết quả và cơ cấu hình thức lựa chọn nhà thầu phân bố ra sao? Nguồn: award + date/geography/sector/method/currency. Lọc: thời gian, lĩnh vực, địa phương, phương thức. Chỉ VND.

```text
[Bộ lọc]
[Số lượt kết quả] [Lượt hiệu lực] [Giá trị trúng] [Độ phủ tiền hợp lệ]
[Xu hướng tháng: tiền và lượt hiệu lực] [Giá trị theo method_category]
[Top 10 địa phương]                    [Bảng lĩnh vực × phương thức]
```

- Phương thức là cột ngang, có tooltip số lượt và mẫu giá hợp lệ. Mã chưa giải nghĩa vẫn xuất hiện dưới nhãn mã nguồn.
- Bảng tổng hợp có số lượt kết quả, lượt hiệu lực, mẫu tiền hợp lệ, giá trị trúng; không thêm bộ lọc quy mô vì fact thiếu chiều này.
- Không lấy số lượt kết quả làm số nhà thầu, không tạo số lượng hợp đồng từ fact này. Kết quả chưa hiệu lực không được suy thành hợp đồng đang thực hiện.

### Trang 4 — Chênh lệch dự toán–giá trúng và mức xác minh

**Câu hỏi:** Trong các mẫu so sánh được ở mức mô tả, chênh lệch thế nào và mức xác minh đến đâu? Nguồn: savings + date/geography/sector/method/value_band/currency. Lọc: thời gian, lĩnh vực, địa phương, phương thức, quy mô; VND.

```text
[Bộ lọc] [Nhãn cố định: chênh lệch mô tả, chưa phải tiết kiệm xác minh]
[Dự toán cùng mẫu] [Giá trúng cùng mẫu] [Chênh lệch mô tả] [Tỷ lệ chênh lệch] [Độ phủ mẫu]
[Waterfall: dự toán → chênh lệch → giá trúng] [Xu hướng tỷ lệ và số mẫu, hai panel]
[Bảng lĩnh vực / phương thức / quy mô]       [Mức xác minh và hợp đồng]
```

- Waterfall dùng duy nhất descriptive_baseline_sum, descriptive_difference_sum và descriptive_award_sum cùng bộ lọc. Bước giữa bằng **âm chênh lệch** để kết thúc tại giá trúng; chênh lệch âm trở thành bước tăng giá. Ghi chú cơ sở thuế/phạm vi chưa xác minh.
- Bảng chọn một chiều phân tích, có ba tổng tiền, số mẫu và tỷ lệ tính lại từ tổng; không lấy trung bình tỷ lệ các nhóm. Không cho lọc “gói vượt dự toán” vì chỉ còn tổng nhóm.
- Khối xác minh hiện số mẫu verified và contract bằng 0 cùng thông báo “Chưa có mẫu”; các giá trị tiền/tỷ lệ của hai nhóm là BLANK. Không trộn contract vào waterfall mô tả.
- Nếu bản xuất sau có verified_sample_count > 0, khối này hiển thị KPI verified theo đúng công thức; contract chỉ hiển thị tổng tiền và số mẫu khi có mẫu, chưa tạo tỷ lệ tiết kiệm hợp đồng do thiếu baseline cùng mẫu đã chứng minh.

### Trang 5 — Cạnh tranh đấu thầu

**Câu hỏi:** Mức tham gia và cạnh tranh khác nhau thế nào theo lĩnh vực, địa phương và hình thức? Nguồn: competition + date/geography/sector/method/value_band. Lọc: thời gian, lĩnh vực, địa phương, phương thức, quy mô; không có slicer tiền tệ.

```text
[Bộ lọc] [Coverage theo ngày báo cáo nguồn]
[Lượt mở] [Nhà thầu bình quân] [Tỷ lệ một nhà thầu] [Tỷ lệ đạt kỹ thuật]
[Xu hướng tỷ lệ một nhà thầu theo tháng] [Cột chồng một/nhiều theo phương thức]
[Scatter địa phương: bình quân × tỷ lệ một nhà thầu] [Bảng lĩnh vực × quy mô]
```

- Scatter mỗi điểm là địa phương, kích thước theo opening_count; tooltip có mẫu số. Điểm ít mẫu không gán kết luận rủi ro, không đặt ngưỡng vi phạm hoặc SLA tùy ý.
- Bảng có opening_count, total_bidders, bình quân, một/nhiều nhà thầu, kỹ thuật và tỷ lệ lượt có giảm giá. Chỉ hiển thị tỷ lệ có giảm giá nếu kiểm tra discounted_bidder_count ≤ total_bidders khi triển khai.
- Tổng hồ sơ kỹ thuật không được diễn giải là toàn bộ lượt tham gia; mẫu số là hồ sơ có kết quả kỹ thuật.
- Các cột total_bid_amount, total_discount_amount, total_final_bid_amount giữ trong nguồn nhưng ẩn khỏi visual/measure tiền. Currency trong dim_value_band không đủ chứng minh tiền tệ mọi số tiền dự thầu.
- Không có dữ liệu trong kỳ thì để trống, không báo cạnh tranh bằng 0. Cạnh tranh thấp là quan sát để rà soát, không phải chứng cứ thông đồng.

### Trang 6 — Thời gian quy trình

**Câu hỏi:** Thời gian bình quân và độ phủ mẫu của từng khoảng quy trình thay đổi thế nào? Nguồn: duration + date/sector/method/metric. Lọc: thời gian, lĩnh vực, phương thức và một metric; không có địa phương, quy mô hoặc tiền tệ.

```text
[Thời gian] [Lĩnh vực] [Phương thức] [Metric: single-select]
[Bình quân + đơn vị] [Mẫu hợp lệ] [Mẫu bị loại] [Tỷ lệ mẫu bị loại]
[Xu hướng bình quân theo tháng] [Bình quân theo lĩnh vực]
[Bình quân theo phương thức]    [Bảng tháng: interval / valid / excluded / mean]
```

Mặc định chọn `plan_to_notice_days`. Danh mục nhãn:

| metric_key | Nhãn | Đơn vị | Bình quân toàn kỳ nguồn, tham khảo |
|---|---|---|---:|
| plan_to_notice_days | Kế hoạch → thông báo | ngày | 16,4 |
| notice_to_close_hours | Thông báo → đóng thầu | giờ | 238,0 |
| close_to_first_award_days | Đóng thầu → kết quả đầu tiên | ngày | 28,6 |
| close_to_complete_award_days | Đóng thầu → kết quả đầy đủ theo nhãn nguồn | ngày | 28,6 |
| close_to_open_hours | Đóng thầu → mở thầu | giờ | 26,9 |
| open_to_award_days | Mở thầu → kết quả | ngày | 28,4 |
| planned_to_actual_delay_days | Độ lệch thực tế so với kế hoạch theo nguồn | ngày | −14,0 |

- Đơn vị đi theo metric; không vẽ bảy metric chung trục, không cộng bình quân và không tạo Gantt từ tổng hợp.
- Nếu bỏ chọn/chọn nhiều metric qua filter ngoài, visual trả BLANK và yêu cầu chọn một. Khóa single-select và tắt Select all ở slicer.
- Metric độ lệch có 2.193 dòng tổng hợp âm; giữ dấu âm, không cắt về 0. Chưa diễn giải dấu âm thành “sớm kế hoạch” trước khi xác minh quy ước dấu của ba metric mở rộng trong export.
- Hai metric kết quả đầu tiên/đầy đủ có cùng số mẫu và bình quân toàn kỳ; không khẳng định cả hai mốc độc lập đã được xác nhận. Nhãn “đầy đủ theo nguồn” không phải hoàn thành hợp đồng.
- Không cung cấp median, P90, phân bố thời gian từng gói hoặc tỷ lệ quá hạn vì nguồn chỉ có sum/count, không có mẫu cá thể hay ngưỡng cam kết.

### Trang 7 — Nhà thầu và đơn vị nhận thầu

**Câu hỏi:** Những đơn vị nào nhận giá trị lớn trong phạm vi chọn và phần giá trị nào chưa thể phân bổ? Nguồn: supplier + date/geography/sector/currency/party. Lọc: thời gian, lĩnh vực, địa phương, tìm kiếm party; không có phương thức hoặc quy mô.

```text
[Thời gian] [Lĩnh vực] [Địa phương] [Tìm tên/mã người nhận] [Top 10/20]
[Giá trị single_recipient] [Số khóa người nhận hợp lệ] [Tỷ trọng Top N]
[Top N đơn vị theo giá trị] [Xu hướng single_recipient theo tháng]
[Bảng đơn vị: mã, tên, lượt, mẫu, tiền] [Giá trị theo trạng thái phân bổ]
```

- Xếp hạng chỉ single_recipient, VND, có mẫu tiền hợp lệ và party_kind khác UNKNOWN. Dùng khóa làm định danh, nhãn `party_name · party_code`, bổ sung party_key khi mã thiếu/trùng. Không gộp bằng tên hoặc mã số thuế chưa kiểm chứng.
- Cùng điều kiện hợp lệ trên áp dụng cho card giá trị, card số người nhận, bảng, xu hướng, tử số và mẫu số Top N. Mẫu số bỏ cả filter party từ fact lẫn dimension, nhưng áp lại party_kind khác UNKNOWN; giữ các điều kiện single_recipient, VND và eligible_amount_count > 0. Khối trạng thái phân bổ là ngoại lệ, giữ toàn bộ trạng thái và loại đơn vị để phản ánh đúng tổng nguồn.
- Top N mặc định 10, lựa chọn 20; sắp tiền giảm dần rồi party_key để ổn định khi bằng tiền. Tìm party chỉ tác động bảng/ranking của phần single_recipient; mẫu số tỷ trọng Top N luôn là toàn bộ single_recipient hợp lệ trong phạm vi thời gian/lĩnh vực/địa phương, ghi rõ điều này.
- Khối trạng thái phân bổ dùng toàn bộ supplier, chỉ chịu thời gian/lĩnh vực/địa phương/VND; không bị tìm party lọc. Tách single_recipient, unallocated_consortium, independent_winners và no_winner; tooltip có scope_count và eligible_amount_count. Không chia đều tiền liên danh, không gọi một nhóm liên danh là một doanh nghiệp.
- Toàn kỳ supplier: single_recipient có 1.382.374 dòng, unallocated_consortium 71.977, independent_winners 56, no_winner 18.423. Đây là số nhóm, chỉ phục vụ kiểm tra nguồn; biểu đồ nghiệp vụ dùng tổng cột đếm/tiền.
- Không phân tích chủ đầu tư/bên mời thầu từ dim_party vì fact này chỉ có khóa người nhận; không drill-through từ party sang hồ sơ thầu.

### Trang 8 — Độ phủ và chất lượng dữ liệu

**Câu hỏi:** Chỉ tiêu nào có đủ dữ liệu, phạm vi ngày nào được quan sát và giới hạn nào cần biết trước khi ra quyết định? Nguồn: cả 8 fact và các dimension. Lọc chung chỉ thời gian/lĩnh vực; không thêm địa phương vì duration/status không hỗ trợ.

```text
[Thời gian] [Lĩnh vực] [Nhãn: snapshot dữ liệu kết thúc 31/12/2025]
[Mẫu mô tả] [Mẫu xác minh] [Mẫu hợp đồng] [Thông báo hủy quan sát]
[Ma trận coverage fact × tháng] [Ma trận 2×2 trạng thái quan sát]
[Xu hướng thông báo theo notice_family] [Bảng kiểm tra nguồn và giới hạn]
```

- Coverage: mỗi ô hiện số dòng tổng hợp và tooltip min/max report_date, số ngày có dòng. Không có dòng ghi “không có bản ghi”; ngoài min/max ghi “ngoài phạm vi quan sát”. Không chấm điểm đầy đủ chỉ từ mật độ dòng hoặc tần suất ngày có sự kiện.
- Min/max tham chiếu của coverage được tính riêng từng fact trên toàn snapshot, giữ filter lĩnh vực đang chọn nhưng bỏ mọi filter ngày từ lịch và report_date. Ô tháng vẫn tính số dòng trong tháng và khoảng ngày người dùng chọn. Tháng không giao khoảng min/max tham chiếu được gắn “ngoài phạm vi”; tháng giao khoảng đó nhưng không có dòng được gắn “không có bản ghi”. Nếu lĩnh vực không có dòng toàn snapshot, ghi “chưa có dữ liệu cho lĩnh vực”; không dùng min/max của fact khác.
- Ma trận status 2×2 lấy notice_observation_state × award_observation_state, cộng package_count; không dùng từ hoàn thành/chưa hoàn thành. Toàn kỳ lần lượt: đã thấy cả hai 61.099; chỉ thông báo 290.913; chỉ kết quả 672.843; chưa thấy cả hai 1.253.603. Không lấy chênh lệch status và demand làm lỗi vì chưa chứng minh cùng cohort và quy tắc bao phủ.
- Notice theo family: standard 536.187 thông báo, 0 hủy; vk_adb 14.065 và 902 hủy; reoffer 6.966 và 517 hủy. Ghi “hủy quan sát trong nguồn”; số 0 ở standard chưa chứng minh mọi trạng thái hủy đã được thu thập.
- Bảng kiểm tra toàn snapshot hiển thị 17/17 schema khớp, PK/grain/FK, ngày phân tích, ngoại lệ lịch, số lượng nhóm chưa rõ. Ghi rõ đây là kết quả kiểm tra 15/11/2026, không giả làm chỉ số cập nhật động sau refresh. Khi triển khai refresh mới phải chạy kiểm tra lại và cấp metadata kiểm chứng mới.
- Bảng giới hạn ghi thiếu release ID/export timestamp, thiếu tiền tệ competition, thiếu mẫu verified/contract, không có giao dịch cá thể, giới hạn lineage của trường mở rộng. Ngày cuối sự kiện không phải thời điểm ingest; không tính độ trễ refresh từ mtime file.
- Tooltip độ phủ địa lý demand nêu tổng package_count thuộc unknown/multi là 103.691 trên toàn kỳ; khi lọc phải tính lại trong cùng demand. Không thay unknown bằng một tỉnh mặc định.

## 5. Thứ tự triển khai và nghiệm thu

### 5.1. Thứ tự triển khai đề xuất

1. Tạo bản Power BI mới tách khỏi bản hiện tại; cấu hình DataRoot đến bộ export mới, kiểm tra schema/PK/grain/FK và kiểu tiền. Giữ nguyên các thay đổi cục bộ không liên quan.
2. Dựng lịch báo cáo, dimension, quan hệ và measures có bảo vệ mẫu; làm trang chất lượng trước để kiểm tra độ phủ, sau đó trang tổng quan.
3. Làm nhu cầu, kết quả, chênh lệch mô tả và cạnh tranh; đối chiếu SQL trực tiếp với DAX ở toàn kỳ, năm 2025 và một địa phương/lĩnh vực.
4. Làm duration và supplier với các giới hạn lọc riêng; rà soát giao diện và xuất ảnh 8 trang để duyệt. Chưa thay thế báo cáo cũ trước khi nghiệm thu.

Tiền nguồn là DECIMAL(38,6). Khi triển khai model phải đo sai số nếu chuyển sang double; không tự dùng Fixed Decimal có bốn chữ số và giới hạn miền nhỏ hơn nguồn. Giữ Parquet nguyên bản; báo cáo phân tích không thay thế sổ kế toán. Tài liệu này chưa chứng nhận độ chính xác số học của một model mới.

Ngưỡng nghiệm thu SQL–DAX: số đếm phải khớp tuyệt đối; tổng tiền so theo VND gốc, sai số tuyệt đối không vượt `max(1 VND, abs(tổng SQL) × 10^-12)`. Tỷ lệ và bình quân có sai số không quá `max(10^-9, abs(giá trị SQL) × 10^-12)` theo đơn vị gốc, trước khi định dạng. Đây là ngưỡng cho báo cáo phân tích; ghi cả sai số đo được, không chỉ pass/fail. Kiểm tra tổng toàn kỳ, từng năm và các lát cắt đã nêu; vượt ngưỡng thì chưa nghiệm thu model và phải xử lý kiểu dữ liệu/cách tổng hợp trước.

### 5.2. Kiểm tra chấp nhận

| Tình huống | Kết quả phải đạt |
|---|---|
| Toàn bộ 17 bảng | Đọc đúng schema, số dòng; PK/grain/FK đạt hoặc refresh dừng kèm lý do |
| Năm 2025 mặc định | Tất cả trang cùng thời gian/lĩnh vực; không rơi vào kỳ trống của nguồn cũ |
| Chọn tháng 01/2022 | Không có fact; hiện chưa có dữ liệu, không kết luận mọi KPI bằng 0 |
| Chọn toàn 2022 | Nhắc dữ liệu fact bắt đầu tháng 9; không hiển thị tăng trưởng toàn năm so với 2023 |
| Verified/contract toàn nguồn | Số mẫu là 0; tiền và tỷ lệ là BLANK với thông báo chưa có mẫu |
| Descriptive toàn nguồn | 2.481.363 mẫu; ba tổng tiền đúng số kiểm chứng ở mục 2.2; waterfall kết thúc tại descriptive_award_sum |
| Chênh lệch âm | Giữ dấu, bước waterfall tăng; không biến thành 0 hoặc tự gọi là vi phạm |
| Competition | Một + nhiều = lượt mở; không có visual tiền; kỹ thuật dùng đúng mẫu có kết quả |
| Duration | Một metric duy nhất; sum/count có trọng số, đúng đơn vị; nhiều metric trả BLANK |
| Duration âm | Giữ dấu ở planned_to_actual_delay_days; có ghi chú quy ước dấu cần xác minh |
| Supplier toàn kỳ | Tổng tiền/scope khớp award khi cùng phạm vi; không cộng hai nguồn thành tổng mới |
| Tìm tên và Top N | Không gộp hai party trùng tên; mẫu số tỷ trọng và khối trạng thái giữ phạm vi đã đặc tả |
| Địa phương/phương thức/quy mô | Chỉ lọc fact có chiều tương ứng; không tạo tác động giả trên duration/status/supplier |
| Bộ lọc USD/UNKNOWN nếu được mở sau này | Measure VND để trống; không ghi đè filter và không tự quy đổi |
| Tỷ lệ theo nhiều nhóm | Tính lại từ tổng tử/mẫu; không trung bình các phần trăm hoặc bình quân sẵn |
| Layout | Không chồng/cắt visual, nhãn và đơn vị rõ; tooltip có mẫu, nguồn và ngày báo cáo |
| Kiểm tra sau refresh | Phạm vi ngày, mẫu và kiểm chứng được chạy lại; không tái sử dụng nhãn chất lượng cũ như kết quả mới |

### 5.3. Phần đã xác minh và phần cần xác minh khi xây dựng

**Đã xác minh trực tiếp:** metadata 17 bảng, số dòng, PK, grain 8 fact, các FK fact, min/max và null ngày; null/âm/min/max/tổng các cột số nghiệp vụ ở 7 fact ngoài status; phân bố dimension nhỏ, các trạng thái người nhận, tiền tệ demand/award/savings; các đẳng thức cạnh tranh/duration; toàn bộ verified/contract bằng 0. Kết quả baseline trong tài liệu là toàn kỳ, không phải giá trị mặc định năm 2025.

**Chưa xác minh:** toàn bộ phân bố/null của mọi thuộc tính text, chính xác tên/mã doanh nghiệp, sự đầy đủ của thu thập theo từng ngày, quy ước dấu và lineage ba metric duration mở rộng, currency của số tiền competition, lineage trường contract và tính đồng nhất snapshot. Không dùng các điểm chưa xác minh để tạo KPI khẳng định.

Kiểm tra bổ sung trên snapshot này đều đạt, không có dòng vi phạm: `discounted_bidder_count <= total_bidders`; `eligible_amount_count <= effective_award_scope_count <= award_scope_count`; `cancelled_notice_count <= notice_count`; và descriptive_baseline_sum − descriptive_award_sum = descriptive_difference_sum theo từng nhóm. Chạy lại các kiểm tra khi triển khai hoặc đổi nguồn. Nếu không đạt thì ghi nhận lỗi nguồn, ẩn chỉ tiêu liên quan và giải thích; không sửa dữ liệu để ép đạt.

Tài liệu đủ để duyệt thiết kế khi người đọc xác định được mỗi trang trả lời câu hỏi gì, các measure lấy từ đâu, bộ lọc tác động đến phần nào và chỉ tiêu nào còn thiếu bằng chứng. Việc nghiệm thu Power BI cần thêm SQL–DAX, refresh thực tế và ảnh giao diện, không chỉ kiểm tra tài liệu.
