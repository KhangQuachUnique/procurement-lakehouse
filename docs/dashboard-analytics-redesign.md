# Procurement Analytics Dashboard V4

## Phạm vi và cách đọc

Bản rà soát ngày 25/11/2026 sử dụng 17 Parquet ở PowerBI/export/parquet. Profile kiểm tra khóa, grain và quan hệ trên từng bảng. Bộ kiểm tra V4 so tên cột với schema Gold hiện tại. Tệp xuất thiếu manifest release nên chỉ xác nhận được cấu trúc và nội dung của các file đã đọc. Không đủ bằng chứng để gán một lần chạy Gold cho cả snapshot. Private business schema mô tả đầu vào tính toán, chưa chứng minh các bảng chi tiết vẫn truy vấn được.

## Quyết định thiết kế và chart-to-data mapping

| Câu hỏi | Quyết định với V3 | Visual và ngữ cảnh |
|---|---|---|
| Nhu cầu và giá trị trúng tập trung ở đâu? | MERGE ba trang Tổng quan, Nhu cầu, Kết quả. REMOVE card chênh lệch lặp. ADD hai tỷ trọng theo quy mô. | Đường tháng theo ngày công bố của từng fact, bar lĩnh vực theo tiền trúng, bảng quy mô theo demand và bảng địa phương theo award. Xếp hạng dùng tên, phiên bản và mã nguồn. Tổng giữ cả GEO_UNKNOWN và GEO_MULTI_UNALLOCATED. |
| Nhóm nào có mức tham gia thấp tương đối? | KEEP xu hướng và cỡ mẫu. MODIFY biểu đồ đếm thành bảng tỷ lệ có đối sánh. REMOVE chỉ số giảm giá. | Tháng, lĩnh vực, địa phương và phương thức lọc competition. Bảng lĩnh vực giữ số lượt, tỷ lệ và chênh lệch điểm phần trăm. Sắp theo số lượt để nhóm một quan sát không dẫn bảng. |
| Dự toán khác giá trúng bao nhiêu? | KEEP bốn KPI cùng mẫu, xu hướng tỷ lệ và bảng độ phủ. | Savings theo ngày công bố kết quả, VND. Bảng ngành và quy mô đưa số mẫu bên cạnh giá trị. Giữ dấu chênh lệch. |
| Khoảng thời gian nào dài? | KEEP bộ chọn năm khoảng và mẫu bị loại. | Duration theo tháng, lĩnh vực và phương thức. Ngày ghi nhận là mốc cuối của khoảng. Bộ chọn metric giới hạn phân tích vào một đơn vị. |
| Giá trị tập trung vào ai? | MODIFY mẫu người nhận, Top 10 và biểu đồ lĩnh vực. ADD độ phủ giá trị. | Supplier theo ngày công bố kết quả. Xếp hạng bằng khóa, hiển thị tên kèm khóa. Biểu đồ so CR10 của từng lĩnh vực. Bảng phân bổ cho biết phần tiền nằm ngoài tập có thể quy người nhận. |
| Phạm vi nào có quan sát? | KEEP trạng thái và họ thông báo. REMOVE card kỹ thuật. | Bảng theo tháng cho biết sự hiện diện của từng luồng được hiển thị. Trạng thái là ghi nhận tài liệu tại nguồn. Hủy quan sát lấy từ notice, không suy ra tỷ lệ gói thất bại. |

## Kết quả trên snapshot

Các kết quả dưới đây dùng toàn kỳ quan sát của từng fact. Truy vấn, số mẫu và kết quả cho mọi nhóm nằm trong [bằng chứng DuckDB](../PowerBI/ProcurementDashboard/validation/redesign_insights.json). Report tính lại theo bộ lọc.

| Câu hỏi | Kết quả | Giới hạn diễn giải |
|---|---|---|
| Nhóm quy mô nào chiếm nhiều giá trị? | 6.257 gói từ 100 tỷ VND trở lên chiếm 0,275% số gói VND và 46,44% giá trị kế hoạch VND. | Kế hoạch được công bố chưa chứng minh giải ngân. |
| Lĩnh vực nào thường chỉ có một nhà thầu? | Xây lắp 70,22% trên 151.829 lượt mở, hàng hóa 52,64% trên 170.156 lượt. KHAC 100% trên đúng một lượt. | Cơ cấu phương thức có thể khác nhau. Định danh nguồn và số ghi nhận trong hồ sơ giới hạn cách hiểu. |
| Chênh lệch có giống nhau giữa các lĩnh vực? | Hàng hóa 8,31% trên 1.464.505 mẫu VND, xây lắp 4,14% trên 226.106 mẫu. | Tỷ lệ gia quyền theo dự toán, chưa đối chiếu thuế và phạm vi thực hiện. |
| Đóng thầu đến công bố kết quả đầu tiên mất bao lâu? | Bình quân 28,61 ngày trên 500.559 mẫu hợp lệ. | Mốc công bố khác ngày quyết định. Trung bình không mô tả phần đuôi phân phối. |
| Giá trị hàng hóa tập trung vào ai? | 10 khóa đứng đầu chiếm 12,72% giá trị trên 74.165 khóa, 1.453.793 mẫu giá. | Chỉ các dòng đủ điều kiện VND, single_recipient. Không phân bổ lại giá trị theo thành viên liên danh. |

## Hợp đồng chỉ tiêu

Mỗi dòng fact công bố là một tổ hợp ngày và các chiều trong [source_profile.json](../PowerBI/ProcurementDashboard/validation/source_profile.json), có group_key làm PK. Số lượt cộng cột đếm. Quan hệ một chiều đi từ dimension tới fact. Tháng và lĩnh vực đồng bộ. Geography dùng ở demand, award, savings, competition, supplier và notice. Method dùng ở award, savings, competition, duration và notice. Value band dùng ở demand, competition và savings. Party chỉ dùng ở supplier. Notice family là thuộc tính fact.

| KPI và đơn vị | Fact, ngày và điều kiện | Tử số / mẫu số và BLANK |
|---|---|---|
| Số gói, tiền kế hoạch (tỷ VND) | Demand, ngày công bố gói. Đếm mọi currency, tiền chỉ VND và có mẫu giá. | SUM(package_count), SUM(planned_amount_sum)/1e9. Tỷ trọng quy mô lấy cùng tập VND và tổng các nhóm đang chọn. |
| Lượt kết quả hiệu lực, tiền trúng (tỷ VND) | Award, ngày công bố kết quả. Count theo effective hoặc effective_completed. Tiền cộng trên mẫu giá hiệu lực VND. | SUM(effective_award_scope_count), SUM(award_amount_sum)/1e9 nếu có mẫu. Một gói có thể có nhiều phạm vi kết quả. |
| Lượt mở, nhà thầu/lượt và tỷ lệ một nhà thầu | Competition, ngày mở thực tế, fallback ngày công bố. Mỗi bản mở sau chọn phiên bản tính một lượt. | SUM(opening_count), SUM(total_bidders)/SUM(opening_count), SUM(single_bidder_count)/SUM(opening_count). Đối sánh trừ tỷ lệ gia quyền của các lĩnh vực đang chọn rồi nhân 100. |
| Dự toán, giá trúng, chênh lệch (tỷ VND), tỷ lệ và độ phủ | Savings theo ngày công bố kết quả. Cùng tập descriptive, cùng currency VND. | SUM(descriptive_baseline_sum), SUM(descriptive_award_sum), SUM(descriptive_difference_sum) đổi ra tỷ. Tỷ lệ chia tổng chênh lệch cho tổng baseline. Độ phủ chia descriptive_sample_count cho scope_count. |
| Thời gian bình quân (giờ hoặc ngày), mẫu và tỷ lệ loại | Duration, một metric, hai đầu mốc hợp lệ. | SUM(duration_sum)/SUM(sample_count). Tỷ lệ loại bằng SUM(excluded_sample_count)/SUM(interval_count). Nhiều hoặc không chọn metric trả BLANK. |
| Giá trị đơn, số khóa, mẫu giá, CR10, độ phủ | Supplier, ngày công bố kết quả, VND, single_recipient, eligible_amount_count>0 và party_kind khác UNKNOWN. | Giá trị chia 1e9. DISTINCTCOUNT(party_key). SUM(eligible_amount_count) giữ đúng phần đủ điều kiện trong 892 nhóm có eligibility hỗn hợp. CR10 chia tổng 10 khóa cho tổng cùng tập. Độ phủ chia giá trị đơn cho giá trị mọi allocation state có giá VND. |
| Thông báo, hủy quan sát và tỷ lệ | Notice, ngày công bố thông báo, theo họ nguồn. | SUM(cancelled_notice_count)/SUM(notice_count). |
| Gói theo trạng thái quan sát | Status, ngày cuối có bản ghi trong khoảng và lĩnh vực đã chọn. Bộ tổng hợp Gold hiện hành ghi observation_day vào report_date. | SUM(package_count) tại cùng ngày cuối cho mọi nhóm. Không cộng số gói qua nhiều snapshot. Tệp cũ thiếu release ID nên ngữ nghĩa lịch sử cần đọc thận trọng. |

Các phép chia dùng DIVIDE. Thiếu quan sát hoặc mẫu số bằng 0 cho BLANK. Số 0 có nghĩa khi có tập quan sát và tử số thực sự bằng 0. Kiểm tra số theo phạm vi và đơn vị trong [SQL](../PowerBI/ProcurementDashboard/validation/numeric_expected.json) và [DAX](../PowerBI/ProcurementDashboard/validation/dax_queries.json).

Competition chọn phiên bản theo notify_no hoặc entity_id. Số nhà thầu phân biệt theo mã, fallback tên rồi submission_id. Khi thiếu tổng từ submission, dùng tổng trên biên bản rồi mới về 0. Không tách được zero và unknown trong public fact. Các thống kê kỹ thuật và giảm giá đếm dòng submission, do đó tỷ lệ trên số định danh distinct là UNRELIABLE. Phương thức không kỳ vọng cạnh tranh cần xem riêng.

Duration dùng kế hoạch tham chiếu → thông báo đầu (ngày), công bố thông báo → đóng (giờ), đóng → công bố kết quả đầu (ngày), đóng → mở thực tế (giờ), mở → công bố kết quả đầy đủ theo trường nguồn (ngày). Metric đóng đến kết quả đầu còn loại trường hợp ngày quyết định có trước ngày đóng. Không cộng các khoảng để tính tổng thời gian.

## Nghiên cứu áp dụng

- APPLICABLE. Gắn câu hỏi thị trường với tử số, mẫu số, dữ liệu cần thiết và cỡ mẫu. [OCP market indicators](https://www.open-contracting.org/resources/indicators-to-diagnose-the-performance-of-a-procurement-market/)
- ADAPTED. Tách đo quy trình, cạnh tranh và cấu trúc thị trường. Đánh giá tuân thủ và mục tiêu chiến lược là UNSUPPORTED khi thiếu căn cứ, mục tiêu và outcome. [OECD Performance](https://www.oecd.org/en/publications/public-procurement-performance_0dde73f4-en.html) · [OECD Competition](https://www.oecd.org/en/publications/competition-in-public-procurement_fcc81703-en/full-report.html)
- ADAPTED. PPDS loại direct awards, lô chưa trao và lô thiếu số hồ sơ trong single bidding. V4 dùng lượt mở theo nguồn. Decision speed của EU và khoảng đóng đến công bố ở V4 có hai mốc kết thúc khác nhau. [PPDS dashboards](https://www.public-procurement-data-space.europa.eu/en/dashboards) · [Single bidder methodology](https://eproc.pages.code.europa.eu/ppds/pages/data-analytics/Single-bidder/)
- APPLICABLE. Dùng xu hướng và đối sánh trong phạm vi phân khúc. Tableau Story cần JavaScript, chưa kiểm tra được các thao tác trong story. [ECA SR 28/2023](https://www.eca.europa.eu/en/publications/SR-2023-28) · [Tableau](https://public.tableau.com/app/profile/gti1940/viz/eca_dashboard/Story)
- ADAPTED. Phân tích thị trường theo câu hỏi của người mua và người cung cấp. Chỉ những use case có grain tương ứng mới vào dashboard. Thuật ngữ Việt Nam theo cổng thống kê, công thức theo implementation Gold. [Prozorro BI](https://www.open-contracting.org/2024/12/04/how-to-make-better-public-procurement-decisions-with-business-intelligence-insights-from-ukraine/) · [Hệ thống mạng đấu thầu quốc gia](https://muasamcong.mof.gov.vn/vi/web/guest/statistical)
- APPLICABLE. Đường cho chuỗi thời gian, bar cho xếp hạng, đơn vị ngay trên visual và tooltip có cỡ mẫu. Đánh giá hiệu năng cần tách thời gian truy vấn và render. [Report design](https://learn.microsoft.com/en-us/power-bi/create-reports/desktop-tips-and-tricks-for-creating-reports) · [Dashboard design](https://learn.microsoft.com/en-us/power-bi/create-reports/service-dashboards-design-tips) · [Performance Analyzer](https://learn.microsoft.com/en-us/power-bi/create-reports/performance-analyzer)

## Backlog cần dữ liệu chi tiết

| Câu hỏi | Grain, khóa và relation tái sử dụng | Rủi ro và chi phí tương đối |
|---|---|---|
| Gói nào chuyển từ kế hoạch sang trúng? | Một package_key theo cohort công bố, nối demand với scope kết quả hiệu lực qua quan hệ vòng lựa chọn. | Nhiều lô, nhiều vòng và sửa đổi. Chi phí cao để chốt trạng thái và cửa sổ quan sát. |
| P50/P90 hoặc hiệu suất bên mời thầu? | Một vòng × metric, round_business_key, hai timestamp, cờ eligibility và khóa buyer/procurer từ fact_selection_round. | Bridge tài liệu nhân bản vòng. Chi phí trung bình cho projection, cao hơn cho đối sánh buyer qua thời kỳ. |
| Tỷ lệ thắng theo người dự? | Một party × vòng × lô, cần định danh dự và nhận, khóa phạm vi kết quả. | Hồ sơ dự trùng, liên danh và nhiều người thắng. Chi phí cao cho entity resolution. |
| Nguyên nhân hủy và thực hiện hợp đồng? | Một sự kiện hủy hoặc milestone hợp đồng có revision_id, thời điểm, lý do và khóa phạm vi. | Trùng bản sửa và một scope có nhiều hợp đồng. Chi phí cao cho dữ liệu mới và kiểm tra lịch sử. |

## Grain, khóa và thời gian quan sát

Mỗi contribution mang owner_key nội bộ. Khi cập nhật, aggregation thay toàn bộ contribution của owner bị ảnh hưởng, cộng lại theo grain rồi tính group_key từ grain. Riêng status giữ các ngày quan sát trước và thay snapshot của ngày đang chạy. Khi project dimension, các khóa đã được tham chiếu trước đây có thể tiếp tục tồn tại. Vì vậy khoảng min/max của dim_date rộng hơn khoảng hoạt động. Lịch nguồn không liên tục. V4 dùng month_start và tránh YoY trên lịch này.

| Fact | Grain thực tế sau group_key | Ngày đầu | Ngày cuối |
|---|---|---|---|
| fact_award_daily | report_date, geography_key, sector_key, method_key, currency_key | 2022-09-16 | 2025-12-31 |
| fact_bidding_competition_daily | report_date, geography_key, sector_key, method_key, value_band_key | 2022-09-23 | 2025-12-31 |
| fact_demand_daily | report_date, geography_key, sector_key, value_band_key, currency_key | 2022-09-16 | 2025-12-31 |
| fact_duration_daily | report_date, sector_key, method_key, metric_key | 2022-09-17 | 2025-12-31 |
| fact_notice_daily | report_date, geography_key, sector_key, method_key, notice_family | 2022-09-17 | 2025-12-31 |
| fact_savings_daily | report_date, geography_key, sector_key, method_key, value_band_key, currency_key | 2022-09-16 | 2025-12-31 |
| fact_status_daily | report_date, status_key, sector_key | 2022-09-16 | 2025-12-31 |
| fact_supplier_daily | report_date, geography_key, sector_key, currency_key, party_key, recipient_allocation_state | 2022-09-16 | 2025-12-31 |

Các ngày 1202–2026 trong dimension gồm ngày ngoài phạm vi tám fact. Trên slicer một tháng không có sự kiện vẫn có thể chọn để kiểm tra tình trạng thiếu quan sát. Không dùng ngày lớn nhất của lịch làm freshness. Chính sách Geo trong Gold hiện tại có hiệu lực theo thời kỳ, trong khi snapshot xuất chỉ ghi phiên bản nguồn. Không có crosswalk để quy về địa giới hiện hành.

## Đọc số âm và mẫu nhỏ

Có 9.350 nhóm ngày VND có tổng chênh lệch âm, bao gồm 35.277 mẫu so sánh trong các nhóm đó. Một nhóm có thể chứa cả mẫu âm và dương, nên 35.277 chưa xác định được số lần giá trúng cao hơn dự toán. Các biểu đồ giữ nguyên cả hai dấu. Bảng cạnh tranh ưu tiên số lượt quan sát và không cắt nhóm theo một ngưỡng tùy ý.

## Phụ thuộc và tái sinh

SQL tính đóng góp và business relations nằm trong src/procurement/gold/application/compute. Public schema nằm ở src/procurement/gold/domain/schema.json. Bộ sinh dùng schema Arrow của file thật để tạo Power Query và TMDL. Các relationship, danh sách measure, công thức, format và cột có trong model_inventory.json. Báo cáo và generator sử dụng chung định danh trang. Để rollback, lấy lại cả generator, report, semantic model và validation của cùng phiên bản trong Git.

OCP giới hạn mẫu số single bidding vào các tender kỳ vọng cạnh tranh. Export chỉ giữ số định danh trên bản mở nên V4 công bố rõ mẫu số mọi lượt mở trong phương thức đang chọn. Ghép giá trị award vào từng opening để tính tỷ trọng tiền theo số hồ sơ là UNSUPPORTED. [Hướng dẫn công thức và trường OCDS](https://docs.google.com/document/d/1vSJk9-qWSTQEx9ZZc7BUhQZMHvTRcyDYVS2sl8HB__k/edit)

PPDS decision speed liên kết hạn nhận hồ sơ trong CN với ngày ký kết trong CAN, giữ lô đã trao có thời gian dương và tối đa 730 ngày. Phép tính duration của Gold dùng giới hạn và mốc thời gian riêng. Chưa thể tái tạo bộ loại trừ PPDS từ tổng ngày và số mẫu. [PPDS Decision speed](https://eproc.pages.code.europa.eu/ppds/pages/data-analytics/Decision-speed/)

## Inventory visual V3 đối chiếu với V4

Bảng dưới kiểm kê các card, đường, bar và bảng nghiệp vụ từ PBIR V3. Các textbox phạm vi, tiêu đề, thanh điều hướng và slicer tháng/lĩnh vực dùng chung được MODIFY đồng loạt cho sáu trang và mặc định toàn kỳ. Từng slicer cục bộ được giữ tại trang có đầy đủ quan hệ, trừ method/band của hai trang thị trường đã gộp.

| Trang V3 | KPI / visual | Quyết định |
|---|---|---|
| awards | Lượt kết quả | REMOVE khỏi trang thị trường |
| awards | Độ phủ giá trúng | REMOVE khỏi trang thị trường |
| awards | Lượt kết quả hiệu lực theo lĩnh vực | MERGE vào trang thị trường |
| awards | Giá trị theo phương thức · tỷ VND | MERGE vào trang thị trường |
| awards | Kết quả theo địa phương | MERGE vào trang thị trường |
| awards | Trúng thầu · tỷ VND | REMOVE khỏi trang thị trường |
| awards | Lượt kết quả hiệu lực | MERGE vào trang thị trường |
| awards | Giá trị trúng thầu theo tháng · tỷ VND | MERGE vào trang thị trường |
| competition | Cạnh tranh theo quy mô | KEEP |
| competition | Tỷ lệ một nhà thầu | KEEP |
| competition | Tỷ lệ một nhà thầu theo tháng | MODIFY → Tỷ lệ một nhà thầu theo tháng |
| competition | Nhà thầu bình quân | KEEP |
| competition | Lượt mở thầu | KEEP |
| competition | Tỷ lệ lượt có giảm giá | REMOVE khỏi trang |
| competition | Cạnh tranh theo phương thức | KEEP |
| competition | Lượt mở một / nhiều nhà thầu theo lĩnh vực | MODIFY → Một nhà thầu theo lĩnh vực · đối sánh với tập chọn |
| demand | Địa phương · số gói và giá trị kế hoạch | MERGE vào trang thị trường |
| demand | Mẫu giá kế hoạch | REMOVE khỏi trang thị trường |
| demand | Số gói kế hoạch | MERGE vào trang thị trường |
| demand | Kế hoạch bình quân · tỷ VND | REMOVE khỏi trang thị trường |
| demand | Kế hoạch · tỷ VND | REMOVE khỏi trang thị trường |
| demand | Giá trị kế hoạch theo tháng · tỷ VND | MERGE vào trang thị trường |
| demand | Giá trị kế hoạch theo lĩnh vực · tỷ VND | MERGE vào trang thị trường |
| demand | Số gói kế hoạch theo quy mô | MERGE vào trang thị trường |
| duration | Mẫu hợp lệ và bị loại theo tháng | KEEP |
| duration | Tỷ lệ mẫu bị loại | KEEP |
| duration | Thời gian theo phương thức | KEEP |
| duration | Mẫu thời gian bị loại | KEEP |
| duration | Bình quân theo tháng · đơn vị trên card | MODIFY → Bình quân theo tháng · đơn vị trên card |
| duration | Bình quân theo lĩnh vực | MODIFY → Bình quân theo lĩnh vực |
| duration | Thời gian bình quân | KEEP |
| duration | Mẫu thời gian | KEEP |
| overview | Giá trị trúng thầu theo lĩnh vực · tỷ VND | MODIFY → Giá trị trúng theo lĩnh vực · tỷ VND |
| overview | Giá trị trúng thầu theo tháng · tỷ VND | MERGE vào trang thị trường |
| overview | Kế hoạch · tỷ VND | MODIFY → Số gói kế hoạch |
| overview | Top 10 nhóm địa phương theo nguồn · tỷ VND | MERGE vào trang thị trường |
| overview | Trúng thầu · tỷ VND | MODIFY → Lượt kết quả hiệu lực |
| overview | Số gói kế hoạch | MODIFY → Kế hoạch · tỷ VND |
| overview | Giá trị kế hoạch theo tháng · tỷ VND | MERGE vào trang thị trường |
| overview | Chênh lệch mô tả · tỷ VND | MODIFY → Trúng thầu · tỷ VND |
| quality | Mẫu mô tả | KEEP |
| quality | Mẫu xác minh | KEEP |
| quality | Thông báo theo họ nguồn · hủy quan sát | KEEP |
| quality | Mẫu hợp đồng | KEEP |
| quality | Đã / chưa quan sát thông báo và kết quả | MODIFY → Trạng thái tại ngày cuối có bản ghi trong tập chọn |
| quality | Nhóm kỹ thuật cần xác minh | MODIFY → Độ phủ mẫu mô tả |
| quality | Số nhóm có bản ghi theo tháng · không phải số giao dịch | MODIFY → Luồng sự kiện có quan sát theo tháng |
| savings | Chênh lệch theo lĩnh vực | KEEP |
| savings | Tỷ lệ chênh lệch mô tả theo tháng | MODIFY → Tỷ lệ chênh lệch mô tả theo tháng |
| savings | Mẫu và chênh lệch theo quy mô | KEEP |
| savings | Dự toán và giá trúng cùng mẫu · tỷ VND | MODIFY → Dự toán và giá trúng cùng mẫu · tỷ VND |
| savings | Dự toán mô tả · tỷ VND | KEEP |
| savings | Chênh lệch mô tả · tỷ VND | KEEP |
| savings | Giá trúng mô tả · tỷ VND | KEEP |
| savings | Tỷ lệ chênh lệch mô tả | KEEP |
| suppliers | Top 10 khóa người nhận theo giá trị · tỷ VND | MODIFY → Top 10 khóa người nhận theo giá trị · tỷ VND |
| suppliers | Toàn bộ trạng thái phân bổ theo nguồn | KEEP |
| suppliers | Số khóa người nhận | KEEP |
| suppliers | Lượt nhận đơn | MODIFY → Độ phủ giá trị người nhận |
| suppliers | Người nhận · tỷ VND | KEEP |
| suppliers | Tỷ trọng Top 10 người nhận | KEEP |
| suppliers | Single recipient theo tháng · tỷ VND | REMOVE khỏi trang |

Các visual mới hoặc thay câu hỏi được mô tả trong bảng quyết định thiết kế phía trên.

## Business relations và ranh giới publish

| Relation nội bộ | Đơn vị phân tích trong release | Vai trò trong public |
|---|---|---|
| fact_procurement_demand | package_key | Một contribution kế hoạch và trạng thái quan sát. |
| fact_tender_notice | notice_business_key, liên kết package_key, scope_key và round_business_key | Công bố theo họ thông báo, trạng thái hủy. |
| fact_selection_round | round_business_key | Có thể tạo nhiều contribution duration, một cho từng metric có mốc báo cáo. |
| fact_award | award_business_key tại scope_key | Cùng kết quả tạo các lát cắt award và supplier. Ghép hai fact sẽ nhân tiền. |
| fact_savings | comparison_scope_key | Chọn baseline và award cùng phạm vi trước khi kiểm tra eligibility. |
| Bản mở trong competition.py | Bản đại diện theo notify_no / entity_id sau xếp phiên bản | Mỗi lần mở có trọng số 1. Dữ liệu hồ sơ dự được gom trước khi chiếu theo ngày. |

Bộ publish đưa candidate qua publish_verified, activate và các view công bố. Snapshot Parquet chỉ chứa public facts và dimensions. Nguồn hiện hành có thể sinh party_kind UNKNOWN_GROUP cho bucket báo cáo, trong khi dim_party của export có ORGANIZATION, CONSORTIUM và UNKNOWN. Vì vậy phép đối chiếu cột và FK chỉ chứng minh tương thích cấu trúc. Khi thay export cần chạy lại profile, đặc biệt kiểm tra bucket, ngày status, phiên bản địa lý và giá trị band.

Quý IV/2025 có 236.442 gói kế hoạch VND với tổng 637.974,56 tỷ VND. Nhóm lớn nhất ngày 17/12/2025, lĩnh vực hỗn hợp, GEO_HNI, có đúng một gói và một mẫu tiền, giá trị 64.495,067 tỷ VND, khoảng 10,1% tổng quý. Kiểm tra grain và số mẫu cho thấy giá trị này đã có trong Parquet trước khi vào Power BI. Muốn xác minh nội dung hồ sơ gốc cần package_key và revision_id. Báo cáo giữ giá trị trong tổng và bảng quy mô. [Truy vấn largest_planned_groups và quarterly_market](../PowerBI/ProcurementDashboard/validation/redesign_insights.json)

Đối với descriptive, SQL Gold hiện hành yêu cầu award effective có giá đủ điều kiện, baseline dương, hai currency xác định và bằng nhau, ngày baseline không sau ngày công bố kết quả, kết quả nằm trong kỳ. Khi gói có lô, scope cấp gói bị loại để tránh đếm hai lần. Gold giới hạn award ở tối đa hai lần baseline trước khi tổng hợp. Đây là điều kiện tính mẫu trong nguồn, không phải quy tắc loại outlier bổ sung tại Power BI.

## Lựa chọn biểu đồ và phạm vi nghiệm thu

Cạnh tranh và chênh lệch dùng bảng quy mô kết hợp slicer lĩnh vực. Bảy nhóm lĩnh vực × bảy band, mỗi ô cần cả tỷ lệ và số mẫu, sẽ yêu cầu 14 cột số trên nửa canvas. Bảng hiện tại giữ số mẫu cạnh tỷ lệ và cho phép chọn một hoặc nhiều lĩnh vực. Khi chọn nhiều lĩnh vực, tỷ lệ được tính lại từ các tổng. Người xem cần đổi slicer để đối chiếu hai lĩnh vực theo cùng một band.

Trang thị trường dùng đường tháng để giữ biến động cuối quý nhìn thấy được. Kết quả năm và quý có trong bộ DuckDB. Visual xu hướng hiện tại chưa có nút đổi grain năm/quý/tháng. Trang người nhận ưu tiên CR10 theo lĩnh vực vì câu hỏi là mức độ tập trung khác nhau ở đâu. Trace Performance Analyzer và kiểm tra nhóm cho thấy việc xếp hạng lại nhiều lần trên chiều party lớn có chi phí đo được. Báo cáo chưa cung cấp đường CR10 theo quý, HHI hoặc win rate.
