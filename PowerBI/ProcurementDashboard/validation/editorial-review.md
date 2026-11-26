# Procurement Analytics · Nghiệm thu giao diện Enterprise Light

Dự án bắt đầu từ workspace sạch tại `ca22fb692ab26f6adc7f1b3391e214b233009dea`, sau baseline được chỉ định. Số liệu trong ảnh tham chiếu chỉ phục vụ thiết kế. Mô hình dùng 8 fact, 9 dimension và 37 relationship hiện hữu.

| Trang | Thay đổi giao diện | Ảnh trước | Ảnh sau |
|---|---|---|---|
| Tổng quan | Xu hướng ở vị trí chính, donut có nhãn tỷ trọng và tổng tiền trong tiêu đề. Panel tính tỷ trọng số gói và giá trị cho nhóm từ 100 tỷ VND. | [Trước](screenshots/ui-redesign-before/final-overview.png) | [Sau](screenshots/final-overview.png) |
| Cạnh tranh | Tỷ lệ một nhà thầu là KPI chính. Lĩnh vực xếp theo lượt mở. Heatmap rộng hơn. Insight so sánh biên độ giữa các lĩnh vực có từ 30 lượt mở. | [Trước](screenshots/ui-redesign-before/final-competition.png) | [Sau](screenshots/final-competition.png) |
| Dự toán và giá trúng | Đổi tên trang, ưu tiên đường tỷ lệ, giữ cột giá trị và dấu chênh lệch. Panel cho biết độ phủ và số mẫu. | [Trước](screenshots/ui-redesign-before/final-savings.png) | [Sau](screenshots/final-savings.png) |
| Thời gian | Bộ chọn metric nằm cùng nhóm bộ lọc ở đầu trang. Tiêu đề, trục và card cập nhật giờ/ngày. Insight hiển thị tỷ lệ bị loại và mẫu hợp lệ. | [Trước](screenshots/ui-redesign-before/final-duration.png) | [Sau](screenshots/final-duration.png) |
| Nhà cung cấp | Bảng dài chiếm phần lớn chiều cao, có thứ hạng, tên, thanh giá trị và cột định danh riêng. Tổng hợp theo party_key. Panel đặt CR10 cạnh độ phủ phân bổ. | [Trước](screenshots/ui-redesign-before/final-suppliers.png) | [Sau](screenshots/final-suppliers.png) |
| Chất lượng | Matrix lấy trạng thái có bản ghi để tô màu từng nguồn. Hai card tiền thiếu phân bổ, thanh độ phủ, điều kiện mẫu và giới hạn nghiệp vụ có khu vực riêng. | [Trước](screenshots/ui-redesign-before/final-quality.png) | [Sau](screenshots/final-quality.png) |

## Quyết định theo visual

**Giữ và đặt lại vị trí** cho đường xu hướng, donut, biểu đồ xếp hạng, thanh chồng, heatmap và bảng Top 10. **Định kiểu lại** card, bộ lọc, tiêu đề, nhãn, bảng và chart container trên canvas 1600 × 900. **Thay thế** thanh ngang bằng page navigator dọc có trạng thái đang chọn. **Loại bỏ** chú thích toàn kỳ trên trang Tổng quan và các footer dài. Sidebar dùng phạm vi ngày quan sát của bộ fact hiện tại. Khi đổi bộ nguồn cần rà lại nhãn phạm vi này.

Vòng render đầu phát hiện logo bị ngắt dòng và lỗi định dạng thẻ biên độ cạnh tranh. Các vòng sau sửa logo, thẻ biên độ, bổ sung icon điều hướng, hạng nhà cung cấp và tiêu đề có đơn vị động. [Ảnh vòng đầu](screenshots/editorial-review-1/)

## Measure trình bày

20 measure mới gồm hai tỷ trọng nhóm từ 100 tỷ, biên độ cạnh tranh tính theo điểm phần trăm, tám measure màu nền theo fact, tên và hạng người nhận, ba tiêu đề thời gian, một tiêu đề donut và ba phiên bản định dạng cho card. Measure màu heatmap cạnh tranh có trước được đổi sang thang xanh dương. [Định nghĩa DAX](model_inventory.json)

## Bằng chứng của bản cuối

- 186 tệp JSON và 172 visual đạt kiểm tra schema, tham chiếu trường, role, biên canvas và overlap. Trình phân tích TMDL của Desktop đọc được 154 measure. [PBIR](static_validation.json) · [TMDL](tmdl_validation.json)
- Full refresh trên Parquet thật đưa 18 partition về Ready. Toàn bộ 225 tập kết quả DAX khớp phiên bản trước trong dung sai. 100 phép đối chiếu số SQL–DAX và hai bộ đối chiếu định danh/giá trị Top N đạt. [Refresh](engine_state.json) · [Hồi quy](editorial-regression.json) · [Đối chiếu số](numeric_validation.json) · [Top N](group_validation.json)
- Kiểm tra theo toàn kỳ, Hàng hóa, tháng trống, metric giờ và tập Hàng hóa + Xây lắp. Benchmark gia quyền hai lĩnh vực khớp SQL ở mức 60,93234%. Trong tháng trống, insight trả BLANK. [Kết quả các truy vấn](editorial-insights.json)
- Tám kiểm tra giao diện đạt, gồm Ctrl+click sidebar, bộ lọc lĩnh vực, insight trên tập lọc, đồng bộ liên trang, metric giờ, tháng không có quan sát và khôi phục toàn kỳ. Tooltip đã được hover trong Desktop. [Tương tác](interaction_validation.json) · [Tooltip](screenshots/tooltip-competition.png)
- Sinh lại hai lần vào thư mục độc lập, mỗi lần 210 tệp giống từng byte với bản bàn giao. Sáu tab được xác nhận trong Desktop trước khi chụp. [Ảnh và thứ tự trang](page_capture_validation.json)

## Giới hạn hiển thị

Các card tiền giữ giá trị đến hai chữ số thập phân theo tỷ VND để đọc và đối chiếu trực tiếp. Tiêu đề donut rút gọn tổng sang nghìn tỷ. Bảng nhà cung cấp giữ khóa riêng để phân biệt các đơn vị trùng tên. Matrix và danh sách phương thức có thể cần cuộn theo số nhóm. Người dùng Desktop chuyển trang bằng Ctrl+click trong sidebar. Ở chế độ chỉnh sửa, Power BI điều khiển việc hiển thị tooltip và nhãn phần trăm tùy diện tích thực tế.

## Hiệu năng trên Desktop

Thời gian từ lúc visual đầu tiên bắt đầu đến lúc visual cuối cùng hoàn tất khi chuyển trang ở phạm vi toàn kỳ. Hai lượt ghi có tập trang giao nhau. Bảng dùng lượt thứ hai khi có sự kiện tương ứng. Các số đo phụ thuộc cache và tải máy. [Lượt thứ nhất](performance_editorial.json) · [Lượt thứ hai](performance_editorial_all.json)

| Trang | Thời gian (ms) |
|---|---:|
| Tổng quan | 1.070 |
| Cạnh tranh | 799 |
| Dự toán và giá trúng | 767 |
| Thời gian | 928 |
| Nhà cung cấp | 1.277 |
| Chất lượng | 1.116 |
