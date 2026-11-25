# Mua sắm công Việt Nam · Tổng hợp dashboard và insight

**Trong các gói kế hoạch VND được ghi nhận, cứ khoảng 365 gói mới có một gói từ 100 tỷ đồng trở lên, nhưng nhóm này chiếm 46,44% giá trị.** Vì vậy, một quý có ít gói hơn vẫn có thể có tổng giá trị cao hơn. Trang thị trường đặt tỷ trọng số gói cạnh tỷ trọng giá trị để người xem nhận ra sự khác biệt này.

Phạm vi phân tích là 17 bảng Parquet của dự án, rà soát ngày 25/11/2026. Dữ liệu kế hoạch và kết quả trải từ 16/09/2022 đến 31/12/2025, lượt mở thầu bắt đầu ngày 23/09/2022. Các con số dưới đây dùng toàn kỳ quan sát trừ khi ghi rõ quý. Trên dashboard, số liệu được tính lại theo bộ lọc. [Mở dashboard](../PowerBI/ProcurementDashboard/ProcurementDashboard.pbip) · [SQL và kết quả theo phân khúc](../PowerBI/ProcurementDashboard/validation/redesign_insights.json)

## Quy mô, biến động và địa phương

Snapshot ghi nhận **2.278.458 gói kế hoạch** trên mọi loại tiền tệ. Giá trị kế hoạch thuộc tập VND đạt **5.305.409 tỷ đồng**, còn giá trị trúng thuộc tập VND đạt **2.162.473 tỷ đồng**. Hai luồng dùng ngày công bố riêng. Muốn tính tỷ lệ gói kế hoạch đi đến kết quả cần theo dõi cùng tập gói qua thời gian. [Đối chiếu SQL toàn kỳ](../PowerBI/ProcurementDashboard/validation/numeric_expected.json)

Quý IV/2025 có **236.442 gói VND**, giảm khoảng **13,0%** so với 271.793 gói cùng quý năm 2024. Trong khi đó, giá trị kế hoạch tăng **29,3%**, từ 493.312 lên 637.975 tỷ đồng. Riêng ngày 17/12/2025, một gói hỗn hợp được nguồn gán cho Hà Nội có giá kế hoạch 64.495 tỷ đồng, tương đương **10,1% tổng quý**. DuckDB xác nhận nhóm này có một gói và một mẫu giá. Việc xác minh nội dung hồ sơ cần khóa gói và phiên bản tài liệu gốc. [Truy vấn quý và nhóm giá trị lớn nhất](../PowerBI/ProcurementDashboard/validation/redesign_insights.json)

Khoảng **9,77% giá trị trúng VND** thuộc nhóm nhiều địa phương chưa phân bổ hoặc địa phương chưa xác định. Khi đọc bảng xếp hạng tỉnh, tổng các tỉnh xác định vì thế nhỏ hơn tổng thị trường. Tên và phiên bản địa phương lấy theo nguồn tại kỳ quan sát. [Truy vấn phân bổ địa lý](../PowerBI/ProcurementDashboard/validation/redesign_insights.json)

## Cạnh tranh

Toàn kỳ có **427.841 lượt mở**, trung bình **2,00 nhà thầu/lượt**. Có **61,61%** lượt mở ghi nhận một nhà thầu. Bảng dưới cho thấy cả mức chênh lệch lẫn số quan sát đứng sau tỷ lệ. [Kết quả theo lĩnh vực](../PowerBI/ProcurementDashboard/validation/redesign_insights.json)

| Lĩnh vực | Lượt có một nhà thầu | Tổng lượt mở | Tỷ lệ |
|---|---:|---:|---:|
| Tư vấn | 26.866 | 37.054 | 72,50% |
| Xây lắp | 106.619 | 151.829 | 70,22% |
| Phi tư vấn | 39.885 | 67.601 | 59,00% |
| Hàng hóa | 89.574 | 170.156 | 52,64% |

Xây lắp cao hơn hàng hóa **17,58 điểm phần trăm**. Để xem chênh lệch còn lớn đến đâu trong điều kiện gần nhau hơn, chọn cùng tháng và phương thức trên trang **Cạnh tranh cơ bản**, rồi đọc lại tỷ lệ cùng số lượt. Nhóm KHAC đạt 100% từ đúng một lượt mở. Các tỷ lệ này mô tả mức tham gia theo biên bản nguồn, chưa đủ để suy ra nguyên nhân hoặc hành vi vi phạm.

## Chênh lệch dự toán và giá trúng

Trên **2.481.363 mẫu so sánh VND**, tổng dự toán là 2.310.709 tỷ đồng. Giá trúng trên chính tập đó thấp hơn **148.984 tỷ đồng**, tương ứng **6,45%** tổng dự toán. Hàng hóa có tỷ lệ 8,31% trên 1.464.505 mẫu, so với 4,14% trên 226.106 mẫu xây lắp. Tỷ lệ toàn kỳ được gia quyền theo giá trị dự toán nên các gói lớn ảnh hưởng nhiều hơn đến kết quả. [SQL và số mẫu](../PowerBI/ProcurementDashboard/validation/redesign_insights.json) · [Tổng đã đối chiếu](../PowerBI/ProcurementDashboard/validation/numeric_validation.json)

Có **9.350 nhóm ngày** có tổng chênh lệch âm. Một nhóm ngày có thể chứa nhiều gói với chênh lệch trái dấu, nên dữ liệu công bố chưa cho biết có bao nhiêu gói vượt dự toán. Hai chỉ tiêu mẫu tiết kiệm đã xác minh và mẫu hợp đồng đều bằng 0 trong snapshot. Kết luận phù hợp ở đây là mức chênh giá trên tập đủ điều kiện.

## Thời gian lựa chọn

| Khoảng quan sát | Bình quân | Mẫu hợp lệ | Mẫu bị loại |
|---|---:|---:|---:|
| Kế hoạch tham chiếu → thông báo đầu tiên | 16,40 ngày | 526.293 | 30.925 |
| Công bố thông báo → đóng thầu | 237,98 giờ | 441.564 | 11 |
| Đóng thầu → mở thực tế | 26,91 giờ | 427.840 | 1 |
| Đóng thầu → công bố kết quả đầu tiên | 28,61 ngày | 500.559 | 5.892 |
| Mở thực tế → công bố kết quả đầy đủ theo nguồn | 28,36 ngày | 490.115 | 16.336 |

**Đóng thầu đến công bố kết quả đầu tiên** là khoảng đáng xem khi phân tích thời gian chờ kết quả. Mốc cuối là ngày công bố. Hai khoảng bắt đầu từ đóng thầu và từ mở thầu dùng tập mẫu và điểm cuối khác nhau, nên lấy hiệu hai số bình quân sẽ không cho ra thời gian đóng đến mở. Trên trang **Thời gian quy trình**, chọn một khoảng rồi đối sánh phương thức trong cùng lĩnh vực. [Kết quả và điều kiện mẫu](../PowerBI/ProcurementDashboard/validation/redesign_insights.json)

## Mức tập trung ở người nhận

**97,05% giá trị trúng VND** thuộc tập đủ điều kiện quy về một khóa người nhận theo nguồn. Trong tập đó có **160.481 khóa** và **10 khóa dẫn đầu chiếm 5,90%** giá trị. Khi tính lại Top 10 riêng trong từng lĩnh vực, mức tập trung khác nhau đáng kể. [Tổng theo tập hợp lệ](../PowerBI/ProcurementDashboard/validation/numeric_expected.json) · [CR10 từng lĩnh vực](../PowerBI/ProcurementDashboard/validation/redesign_insights.json)

| Lĩnh vực | Khóa người nhận | Mẫu giá | Tỷ trọng của 10 khóa đứng đầu |
|---|---:|---:|---:|
| Hỗn hợp | 3.686 | 5.183 | 46,64% |
| Hàng hóa | 74.165 | 1.453.793 | 12,72% |
| Xây lắp | 37.995 | 225.497 | 5,66% |

Nhóm hỗn hợp có ít khóa hơn và một phần lớn giá trị thuộc về 10 khóa đầu. Khi chọn một lĩnh vực, bảng xếp hạng cho biết cụ thể khóa nào đóng góp vào CR10. Khóa nguồn có thể đại diện cho một liên danh. Dữ liệu thiếu định danh pháp nhân đã chuẩn hóa và số lượt tham dự theo người nhận để đánh giá thị phần pháp nhân hoặc tỷ lệ thắng.

## Độ tin cậy và cách kiểm tra lại

Trang **Chất lượng dữ liệu** cho biết tháng nào có quan sát ở từng luồng. Ô trống có thể do thiếu sự kiện quan sát hoặc thiếu mẫu đủ điều kiện. Năm 2022 chỉ có dữ liệu từ tháng 9. Export thiếu release ID để chứng minh cả 17 bảng được xuất trong cùng một lần chạy.

Các phép kiểm tra Desktop đã lưu gồm 225 truy vấn DAX, 100 đối chiếu số với SQL, kiểm tra thứ tự Top 10 và sáu tình huống tương tác. Để tái lập phân tích theo snapshot mới, chạy *profile_redesign.py* từ thư mục tools của dashboard rồi đối chiếu các bảng kết quả với bộ lọc tương ứng. [Bằng chứng nghiệm thu](../PowerBI/ProcurementDashboard/validation/acceptance.md) · [Từ điển KPI và thiết kế](dashboard-analytics-redesign.md) · [Hướng dẫn refresh](../PowerBI/ProcurementDashboard/README.md)
