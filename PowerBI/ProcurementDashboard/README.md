# Dashboard mua sắm công — Gold V3

Đã thay nội dung trong dự án [ProcurementDashboard.pbip](ProcurementDashboard.pbip) bằng 8 trang dùng dữ liệu mới ở `PowerBI/export/parquet`. Không tạo dự án PBIP khác. Xem [đặc tả](../../docs/dashboard-gold-v3-design.md) và [nghiệm thu](validation/acceptance.md).

## Các trang có thể làm từ dữ liệu thực tế

| Trang | Nội dung |
|---|---|
| Tổng quan | Kế hoạch, kết quả, chênh lệch mô tả, lĩnh vực và địa phương |
| Nhu cầu thị trường | Số gói, tiền kế hoạch và cơ cấu quy mô |
| Kết quả lựa chọn | Lượt kết quả, hiệu lực, tiền và độ phủ giá |
| Chênh lệch mô tả | Dự toán và giá trúng cùng mẫu descriptive; tỷ lệ và mẫu |
| Cạnh tranh cơ bản | Lượt mở, lượt tham gia, một/nhiều nhà thầu và lượt có giảm giá |
| Thời gian quy trình | Năm metric, bình quân có trọng số và mẫu bị loại |
| Đơn vị nhận thầu | Top 10 single_recipient, số khóa, lượt nhận và trạng thái phân bổ |
| Chất lượng dữ liệu | Coverage tháng, trạng thái quan sát, thông báo, mẫu và hạn chế |

## Mở và sử dụng

1. Mở PBIP bằng Power BI Desktop hỗ trợ PBIR/TMDL. Môi trường kiểm tra có bản 2.154.956.0.
2. Khi chuyển máy, sửa tham số `DataRoot` đến thư mục Parquet. Một bảng có thể là một file hoặc nhiều part trong thư mục cùng tên bảng.
3. Refresh nếu chưa có cache và Save. Dữ liệu `.pbi/` chỉ dùng cục bộ, không đưa vào Git.
4. Mặc định chọn 12 tháng năm 2025. Có thể bỏ chọn và xem các tháng 2022–2025. Tháng/lĩnh vực đồng bộ giữa các trang; slicer khác chỉ tại trang có chiều hỗ trợ.
5. Trong chế độ chỉnh sửa Desktop, dùng Ctrl+click Page Navigator. Biểu đồ không tự lọc chéo; dùng slicer để lọc chủ động.

Tiền là tỷ VND. Card dùng vi-VN; trục/bảng native có thể theo locale Desktop. Không đủ mẫu hiện dấu “—”, không tự thay bằng 0. Lịch model chỉ giữ 1.461 ngày 2022–2025; Parquet không bị sửa. Không cung cấp YoY/YTD vì fact 2022 chỉ bắt đầu từ tháng 9.

## Giới hạn đã áp dụng

- Verified và hợp đồng đều chưa có mẫu, không dựng KPI tiền/tỷ lệ. Descriptive chưa phải tiết kiệm được xác minh thuế/phạm vi.
- Bỏ KPI kỹ thuật vì 26.191 nhóm có tổng đạt/trượt lớn hơn lượt nhà thầu. Bỏ tiền cạnh tranh vì thiếu currency và 50 nhóm không khớp phép trừ giảm giá.
- Duration chỉ dùng plan_to_notice_days, notice_to_close_hours, close_to_first_award_days, close_to_open_hours và open_to_award_days. Bỏ mốc đầy đủ trùng dữ liệu và độ lệch kế hoạch chưa rõ dấu.
- Single_recipient có mẫu tiền hợp lệ và party_kind khác UNKNOWN là tập dùng cho xếp hạng; không chia tiền liên danh. Khóa nguồn phân biệt đơn vị trùng tên, chưa chứng minh pháp nhân duy nhất.
- Không có thời điểm xuất/release ID đủ chứng minh snapshot chung. Ngày cuối sự kiện không phải ngày ingest. Observed không phải tiến độ hợp đồng.
- Nguồn DECIMAL(38,6) giữ nguyên; model double phục vụ phân tích, không thay sổ kế toán. Sai số được đối chiếu SQL–DAX.

## Kiểm tra và sinh lại

Đóng cửa sổ dự án trước khi sinh lại vì generator thay toàn bộ các trang được quản lý. Ảnh tạm, fixture, script khảo sát và bản sao trước lần thay mới đã được dọn; chỉ giữ bộ sinh/kiểm tra đang dùng và bằng chứng nghiệm thu hiện hành.

```powershell
.\.venv\Scripts\python.exe -X utf8 PowerBI/ProcurementDashboard/tools/profile_sources.py
.\.venv\Scripts\python.exe -X utf8 PowerBI/ProcurementDashboard/tools/build_project.py
.\.venv\Scripts\python.exe -X utf8 PowerBI/ProcurementDashboard/tools/validate_project.py
powershell -NoProfile -ExecutionPolicy Bypass -File PowerBI/ProcurementDashboard/tools/validate_tmdl.ps1
.\.venv\Scripts\python.exe -X utf8 PowerBI/ProcurementDashboard/tools/prepare_numeric_checks.py
```

Sau khi mở Desktop, dùng cổng engine của đúng cửa sổ dự án:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File PowerBI/ProcurementDashboard/tools/engine_check.ps1 -Port <port> -Refresh -QueriesFile PowerBI/ProcurementDashboard/validation/dax_queries.json
.\.venv\Scripts\python.exe -X utf8 PowerBI/ProcurementDashboard/tools/compare_numeric.py
```

Không dùng lại cổng của phiên Desktop đã đóng. Static/TMDL không thay thế refresh, DAX và xem giao diện thật.

## Kiểm tra tương tác và giao diện

```powershell
.\.venv\Scripts\python.exe -X utf8 PowerBI/ProcurementDashboard/tools/verify_interactions.py --pid <PID-Desktop>
.\.venv\Scripts\python.exe -X utf8 PowerBI/ProcurementDashboard/tools/capture_pages.py --pid <PID-Desktop>
.\.venv\Scripts\python.exe -X utf8 PowerBI/ProcurementDashboard/tools/finalize_audit.py
```

Kiểm tra tương tác dùng chuột/bàn phím trên đúng cửa sổ dự án, đối chiếu card với SQL, rồi khôi phục năm 2025. Không chuyển sang ứng dụng khác trong lúc chạy. Bộ kiểm tra dừng nếu mất foreground. Thư mục `tools/` chỉ giữ công cụ xây dựng, xác minh và điều khiển Desktop đang dùng; `validation/` giữ bằng chứng cuối và schema cần để kiểm tra offline.
