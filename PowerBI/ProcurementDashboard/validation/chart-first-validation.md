# Nghiệm thu dashboard ưu tiên biểu đồ · 26/11/2026

Cả sáu trang dùng lại 8 fact, 9 dimension và 37 relationship. Trong 13 bảng thống kê ban đầu, 10 bảng chuyển thành biểu đồ, hai bảng thành matrix và một bảng xếp hạng nhận thanh dữ liệu trong ô.

| Trang | Thay đổi chính | Trước | Sau |
|---|---|---|---|
| Tổng quan | Thanh nhóm so sánh tỷ trọng số gói và giá trị. Top 5 địa phương xếp theo giá trúng VND, loại khóa chưa xác định và nhiều địa phương trước xếp hạng. | [Ảnh](screenshots/chart-first-before/final-overview.png) | [Ảnh](screenshots/final-overview.png) |
| Cạnh tranh | Thanh theo lĩnh vực có đối sánh gia quyền, xếp hạng phương thức và heatmap quy mô × lĩnh vực. | [Ảnh](screenshots/chart-first-before/final-competition.png) | [Ảnh](screenshots/final-competition.png) |
| Chênh lệch | Cột thể hiện giá trị theo tháng, đường thể hiện tỷ lệ. Thanh lĩnh vực giữ dấu và thanh 100% phân rã điều kiện mẫu. | [Ảnh](screenshots/chart-first-before/final-savings.png) | [Ảnh](screenshots/final-savings.png) |
| Thời gian | Thanh xếp hạng phương thức, cột chồng hợp lệ và bị loại. Trục thời gian bình quân dùng định dạng giờ/ngày theo metric. | [Ảnh](screenshots/chart-first-before/final-duration.png) | [Ảnh](screenshots/final-duration.png) |
| Người nhận | Top 10 dùng bảng có thanh dữ liệu, tên đầy đủ và khóa. Thanh trạng thái dùng giá trị đủ điều kiện VND. | [Ảnh](screenshots/chart-first-before/final-suppliers.png) | [Ảnh](screenshots/final-suppliers.png) |
| Chất lượng | Matrix tháng × 8 nguồn, thanh độ phủ và thanh chồng điều kiện so sánh. Hai card lượng hóa giá trị thiếu phân bổ theo địa phương và người nhận. | [Ảnh](screenshots/chart-first-before/final-quality.png) | [Ảnh](screenshots/final-quality.png) |

## Measure trình bày

Bảy measure mới gồm **Đối sánh tập chọn**, **Màu cạnh tranh**, **Lưu ý cỡ mẫu**, **Mẫu ngoài so sánh**, **Khóa người nhận**, **Giá trúng thiếu địa phương** và **Giá chưa phân bổ**. Generator sinh thêm bốn measure định dạng có tiền tố **Hiển thị ·** cho đối sánh, mẫu ngoài so sánh và hai giá trị thiếu phân bổ. Ngưỡng dưới 30 lượt mở là quy ước cảnh báo khi đọc tỷ lệ. DAX của các measure có trước khớp với inventory gốc.

## Kiểm chứng

- [PBIR và theme](static_validation.json) đã kiểm tra 109 tệp JSON, 95 visual, hình học, liên kết trường và role. [Trình phân tích TMDL của Desktop](tmdl_validation.json) đọc được 134 measure.
- [Refresh toàn bộ](engine_state.json) từ Parquet thật đưa 18 partition về Ready. [Đối chiếu số](numeric_validation.json) gồm 100 phép so sánh SQL–DAX. [Đối chiếu nhóm](group_validation.json) kiểm tra danh tính và giá trị Top 10 nhà cung cấp, địa phương. Năm địa phương hiển thị trên trang khớp năm dòng đầu kết quả toàn kỳ.
- [Hồi quy và tái sinh](chart_first_regression.json) kiểm tra 225 tập kết quả với phiên bản trước và sinh lại 133 tệp vào thư mục tạm. Không phát hiện sai khác ngoài dung sai số học. Đối sánh Hàng hóa và Xây lắp cho cùng benchmark 60,93234% trên hai hàng, khớp SQL gộp tử và mẫu.
- [Sáu kiểm tra tương tác](interaction_validation.json) bao gồm chọn lĩnh vực, đồng bộ qua trang, chọn giờ, tháng trống và khôi phục toàn kỳ. [Tooltip đã render](screenshots/tooltip-competition.png) hiển thị nhóm KHAC có 1/1 lượt mở, tỷ lệ 100% và cảnh báo ít quan sát. Kiểm tra hover thực tế lấy mẫu trên biểu đồ cạnh tranh.
- [Ảnh vòng đầu](screenshots/chart-first-review-1/) cho thấy tên người nhận bị cắt và cột heatmap quá rộng. Vòng sau đổi bảng xếp hạng sang tên xuống dòng có thanh dữ liệu, đổi chiều heatmap, rút gọn tên tám luồng và bổ sung cảnh báo mẫu. Ảnh sau ở bảng trên được chụp trực tiếp từ Desktop sau khi xác nhận đúng tab. Ảnh trước lấy từ lần nghiệm thu bản gốc.
- [Performance Analyzer bản trước](performance_v4.json) và [bản cuối](performance_chart_first_final.json) dùng lượt chuyển qua sáu trang ở phạm vi toàn kỳ. Bảng dưới đo từ lúc visual đầu bắt đầu tới khi visual cuối hoàn tất trong cùng sự kiện đổi trang.

| Trang | Trước (ms) | Sau (ms) |
|---|---:|---:|
| Tổng quan | 833 | 544 |
| Cạnh tranh | 434 | 436 |
| Chênh lệch | 615 | 500 |
| Thời gian | 796 | 596 |
| Người nhận | 1.826 | 1.535 |
| Chất lượng | 776 | 509 |

## Giới hạn

Thời gian trên là một lượt quan sát mỗi trang ở hai phiên Desktop. Trạng thái cache và tải nền có thể khác nhau. Một số trục phân loại cần cuộn. Matrix chất lượng biểu thị số nhóm tổng hợp có bản ghi từng tháng. Đánh giá độ đầy đủ của thu thập cần một nguồn đối chiếu độc lập. Insight 0,27% số gói và 46,44% giá trị kế hoạch mô tả toàn kỳ của bản nguồn được kiểm tra. Khi thay bộ Parquet, cần đối chiếu lại chú thích toàn kỳ này. Các biểu đồ tùy chọn về phương thức ở tổng quan, heatmap chênh lệch và xu hướng CR10 được lược để dành chỗ cho các so sánh chính.
