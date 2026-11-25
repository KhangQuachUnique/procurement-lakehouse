# Nghiệm thu Procurement Analytics V4 · 25/11/2026

| Hạng mục | Kết quả | Bằng chứng |
|---|---|---|
| Snapshot hiện tại | 17 bảng khớp tên cột Gold, profile toàn bộ PK, grain, FK. Không có lỗi ở các phép kiểm tra này. | [Source profile](source_profile.json), [insight và SQL](redesign_insights.json) |
| Tái sinh, schema và binding | Sinh hai lần liên tiếp, SHA-256 của cả 128 file định nghĩa trùng nhau. 110 file qua kiểm tra, 96 visual trên 6 trang, không có lỗi geometry hoặc binding được phát hiện. | [Static](static_validation.json), [inventory](report_inventory.json) |
| Semantic model | Parser TOM của Desktop đọc được 18 bảng, 37 relationships. Đã refresh toàn bộ dữ liệu và mở lại PBIP. Bản cuối đã refresh lại toàn bộ. Engine xác nhận 18 partition Ready. | [TMDL](tmdl_validation.json), [engine](engine_state.json) |
| Giá trị | 225 truy vấn engine, 100 phép đối chiếu số độc lập với SQL, gồm cả BLANK, ngoại tệ, một/nhiều metric, quý, ngành, phương thức, địa phương, band và mẫu số benchmark. | [Numeric](numeric_validation.json) |
| Xếp hạng | SQL và DAX trùng khóa hiển thị, giá trị và thứ tự cho hai bảng Top 10 kiểm thử. Xếp hạng người nhận dùng party_key. | [Groups](group_validation.json) |
| Tương tác Desktop | Toàn kỳ, lọc ngành, đồng bộ ngành, chọn metric giờ, tháng trống, reset. | [Interaction](interaction_validation.json) |
| Giao diện | Đã xem ảnh 6 trang. Phát hiện và sửa lỗi tỷ trọng bằng 100% do cột sort của band. Ảnh sau sửa hiển thị 0,27% số gói và 46,44% giá trị ở nhóm từ 100 tỷ. | [Danh sách ảnh V4](page_capture_validation.json), [ảnh tổng quan](screenshots/final-overview.png) |

## Hiệu năng quan sát được

Performance Analyzer ghi thời gian thực tế trên cùng máy với cache import. Baseline dùng report và model V3 tại Git HEAD, tại cùng đường dẫn PBIP. V3 mở năm 2025 và V4 mở toàn kỳ. Số đo gồm cả thay đổi phạm vi mặc định và thiết kế. Chỉ đối chiếu trang có visual thực sự xuất hiện trong trace. [Trace V3](performance_v3.json) · [Trace V4](performance_v4.json)

| Trang | Thao tác | Visual V4 | V3 (ms) | V4 (ms) | Visual V4 có lifecycle dài nhất |
|---|---|---:|---:|---:|---|
| overview | Refresh | 16 | 387 | 504 | Giá trị trúng theo lĩnh vực · tỷ VND |
| overview | Refresh | 16 | 387 | 453 | Địa phương xác định theo giá trúng · tỷ VND |
| competition | ChangePage | 16 | — | 591 | Tỷ lệ một nhà thầu theo tháng |
| savings | ChangePage | 17 | — | 812 | Dự toán và giá trúng cùng mẫu · tỷ VND |
| duration | ChangePage | 17 | 751 | 1024 | Bình quân theo tháng · đơn vị trên card |
| suppliers | ChangePage | 15 | 847 | 2041 | Tỷ trọng Top 10 người nhận |
| quality | ChangePage | 15 | 685 | 965 | Trạng thái tại ngày cuối có bản ghi trong tập chọn |
| overview | ChangePage | 16 | 788 | 1099 | Hai luồng công bố độc lập theo tháng · tỷ VND |
| quality | ChangePage | 15 | 685 | 1271 | Trạng thái tại ngày cuối có bản ghi trong tập chọn |
| suppliers | ChangePage | 15 | 847 | 1176 | Tỷ trọng Top 10 trong từng lĩnh vực |
| overview | ChangePage | 16 | 788 | 1074 | Hai luồng công bố độc lập theo tháng · tỷ VND |
| overview | ChangeSlicer | 16 | — | 836 | Hai luồng công bố độc lập theo tháng · tỷ VND |
| overview | ChangeSlicer | 16 | — | 666 | Địa phương xác định theo giá trúng · tỷ VND |

Đơn vị ms đo từ thao tác tới visual cuối hoàn thành lifecycle đầu tiên của chính visual đó. Sự kiện mở lại dropdown sau khi trang đã render có cùng visualId và được loại khỏi phép đo tải trang. Dấu gạch ngang là không có phép đo tương ứng. Đây là một lượt trên mỗi thao tác, chưa có phân phối cold-cache hoặc phép thử tải đồng thời. Truy vấn CR10 chỉ xếp các khóa xuất hiện trong fact đủ điều kiện. Mỗi truy vấn engine có elapsed_ms riêng trong [DAX results](dax_results.json). Chưa kiểm tra hover từng tooltip và mọi tổ hợp slicer.

[Danh sách ảnh nghiệm thu sáu trang V4](page_capture_validation.json).
