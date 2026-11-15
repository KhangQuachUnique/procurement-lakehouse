# Bàn giao dashboard Gold V3

Ngày kiểm tra: **15/11/2026**. Đã sửa trực tiếp dự án `ProcurementDashboard.pbip` hiện có, thay bốn trang cũ bằng **tám trang dùng nguồn mới**, mở và refresh bằng Power BI Desktop 2.154.956.0. Không publish lên dịch vụ ngoài.

## Các trang đã triển khai

1. [Tổng quan](screenshots/final-overview.png).
2. [Nhu cầu thị trường](screenshots/final-demand.png).
3. [Kết quả lựa chọn](screenshots/final-awards.png).
4. [Chênh lệch mô tả](screenshots/final-savings.png).
5. [Cạnh tranh cơ bản](screenshots/final-competition.png).
6. [Thời gian quy trình](screenshots/final-duration.png).
7. [Đơn vị nhận thầu](screenshots/final-suppliers.png).
8. [Chất lượng dữ liệu](screenshots/final-quality.png).

Model có 17 bảng nguồn, `_Measures` với 116 measures gồm measures số và measures hiển thị, 37 quan hệ một chiều. Lịch model giới hạn 1.461 ngày 2022–2025; không sửa Parquet. Mặc định chọn 12 tháng năm 2025 qua slicer, không khóa năm bằng page filter. Tháng/lĩnh vực cấu hình đồng bộ; các slicer khác chỉ xuất hiện ở trang hỗ trợ. Tắt lọc chéo từ biểu đồ để không suy diễn quan hệ giữa các fact.

## Bằng chứng kiểm tra

| Kiểm tra | Kết quả |
|---|---|
| Nguồn | 17 bảng, PK/grain/FK không có lỗi cấu trúc; xem [source_profile.json](source_profile.json) |
| Toàn vẹn Parquet | 17 hash không đổi so với lúc khảo sát; [source_integrity.json](source_integrity.json) |
| Static | 8 trang, 131 visual containers; JSON schema, bindings và bố cục đạt; [static_validation.json](static_validation.json) |
| TMDL | Parser thật Desktop đọc model thành công; [tmdl_validation.json](tmdl_validation.json) |
| Refresh và mở lại | 18 partition Ready; [engine_state.json](engine_state.json) |
| DAX | 203 truy vấn không lỗi; 85 kết quả scalar/rowcount khớp SQL theo ngưỡng trong đặc tả; [numeric_validation.json](numeric_validation.json) |
| Top 10 | Danh tính và giá trị Top 10 địa phương/người nhận khớp SQL; [group_validation.json](group_validation.json) |
| Giao diện | Đã mở, chụp và xem cả 8 trang; UI Automation xác nhận tab được chọn; [page_capture_validation.json](page_capture_validation.json) |
| Slicer và đồng bộ | Sáu kiểm tra giao diện khớp SQL, mặc định đã khôi phục; [interaction_validation.json](interaction_validation.json) |
| Tổng hợp | [completion_audit.json](completion_audit.json) |

Đối chiếu số học gồm toàn kỳ, năm 2025, tháng 01/2025–Hàng hóa, năm 2023 cho Top 10, năm metric thời gian và các trường hợp BLANK, nhiều/không có metric, ngoại tệ giao với measure VND. Các kiểm tra này không thay thế việc thử mọi tổ hợp slicer trên giao diện.

## Những phần đã chủ động loại khỏi bản này

- Tiết kiệm xác minh và tiền hợp đồng: nguồn mới không có mẫu, không dựng KPI bằng 0 để thay cho thiếu dữ liệu.
- KPI kỹ thuật và tiền cạnh tranh: có bất thường tổng hợp và thiếu thông tin tiền tệ. Trang chất lượng giải thích giới hạn.
- Metric kết quả đầy đủ trùng dữ liệu và độ lệch kế hoạch chưa rõ dấu: không đưa vào bộ chọn thời gian. Chỉ dùng năm metric còn lại.
- Không chia đều tiền liên danh, không tra cứu hồ sơ gói cá thể và không diễn giải trạng thái quan sát thành tiến độ hợp đồng.

## Giới hạn kiểm chứng

- Đã kiểm tra trực tiếp sáu tình huống: tổng toàn kỳ, lọc Hàng hóa, giữ lĩnh vực khi chuyển sang trang cạnh tranh, đổi metric giờ, tháng 01/2022 không có dữ liệu, khôi phục đủ 12 tháng năm 2025. Card hiển thị khớp SQL; [interaction_validation.json](interaction_validation.json). Đây là các ca đại diện, chưa kiểm thử mọi tổ hợp slicer.
- Tiêu đề cột nghiệp vụ và nhãn trạng thái phân bổ đã chuyển sang tiếng Việt. Card dùng vi-VN; trục/bảng native trong Desktop hiện vẫn theo locale tiếng Anh. Mã họ thông báo và mã phương thức chưa có giải nghĩa đáng tin cậy được giữ nguyên theo nguồn. Không khẳng định toàn bộ giá trị nguồn đã Việt hóa.

- Chưa kiểm tra mọi tooltip, tên đơn vị dài và cỡ màn hình. Một số bảng cuộn để xem hết dữ liệu; Top 10 người nhận hiển thị đủ trong khung đã xem.
- Model dùng double để phân tích, Parquet giữ DECIMAL gốc. Nguồn chưa có release ID/thời điểm xuất đủ chứng minh snapshot chung, và các ngày có bản ghi không chứng minh thu thập đầy đủ.

Nội dung/bằng chứng bản cũ đã được dọn khỏi thư mục nghiệm thu hiện hành. Bản sao trung gian trước khi thay đổi cũng đã được dọn sau khi kiểm tra bản mới. Dữ liệu mới không bị xóa hoặc thay đổi.

## Dọn thư mục PowerBI

Đã xóa 38 mục không còn dùng (khoảng 13,25 MB): tám script khảo sát/fixture cũ, cache Python, fixture nhiều part, ảnh mở/refresh trung gian, tài liệu nghiên cứu không được bộ kiểm tra tham chiếu và bản sao PBIP trước lần thay mới. Giữ dữ liệu nguồn, dự án hiện tại, công cụ sinh/kiểm tra đang dùng, JSON schema chính thức và ảnh nghiệm thu cuối. Cache dữ liệu Desktop hiện tại được giữ để mở nhanh trên máy này; không theo Git.
