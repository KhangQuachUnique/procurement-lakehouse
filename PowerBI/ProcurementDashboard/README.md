# Phân tích thị trường mua sắm công Việt Nam · V4

Mở [ProcurementDashboard.pbip](ProcurementDashboard.pbip) trong Power BI Desktop. Chọn trang theo câu hỏi về quy mô thị trường, cạnh tranh, chênh lệch dự toán–giá trúng, thời gian, người nhận hoặc độ phủ. [Tổng hợp dashboard và insight](../../docs/dashboard-insights.md) · [Thiết kế và từ điển KPI](../../docs/dashboard-analytics-redesign.md) · [Nghiệm thu V4](validation/acceptance.md).

## Sử dụng và refresh

1. Trong Transform data → Edit parameters, đặt DataRoot tới thư mục chứa 17 bảng Parquet nếu chuyển máy.
2. Chọn Refresh, chờ hoàn tất rồi Save. Cache .pbi chỉ dùng cục bộ.
3. Dùng tháng và lĩnh vực để giữ cùng phạm vi khi chuyển trang. Mặc định hiển thị toàn kỳ. Xóa lựa chọn trong dropdown để trở về toàn kỳ. Bộ lọc địa phương, phương thức và metric chỉ áp dụng tại trang tương ứng.
4. Trong Desktop dùng Ctrl+click trên thanh điều hướng hoặc chọn tab dưới canvas. Các biểu đồ chỉ nhận bộ lọc từ slicer.
5. Đọc cỡ mẫu bên cạnh tỷ lệ. Tiền biểu diễn theo tỷ VND. Trang thị trường đếm mọi tiền tệ ở card số lượng. Dấu gạch ngang trên card cho biết thiếu quan sát hoặc thiếu mẫu hợp lệ.

## Kiểm tra và sinh lại

Đóng cửa sổ dự án trước khi sinh lại vì generator thay toàn bộ các trang được quản lý. Ảnh tạm, fixture, script khảo sát và bản sao trước lần thay mới đã được dọn; chỉ giữ bộ sinh/kiểm tra đang dùng và bằng chứng nghiệm thu hiện hành.

```powershell
.\.venv\Scripts\python.exe -X utf8 PowerBI/ProcurementDashboard/tools/profile_sources.py
.\.venv\Scripts\python.exe -X utf8 PowerBI/ProcurementDashboard/tools/build_project.py
.\.venv\Scripts\python.exe -X utf8 PowerBI/ProcurementDashboard/tools/validate_project.py
powershell -NoProfile -ExecutionPolicy Bypass -File PowerBI/ProcurementDashboard/tools/validate_tmdl.ps1
.\.venv\Scripts\python.exe -X utf8 PowerBI/ProcurementDashboard/tools/prepare_numeric_checks.py
.\.venv\Scripts\python.exe -X utf8 PowerBI/ProcurementDashboard/tools/prepare_group_checks.py
```

Sau khi mở Desktop, dùng cổng engine của đúng cửa sổ dự án:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File PowerBI/ProcurementDashboard/tools/engine_check.ps1 -Port <port> -Refresh -QueriesFile PowerBI/ProcurementDashboard/validation/dax_queries.json
.\.venv\Scripts\python.exe -X utf8 PowerBI/ProcurementDashboard/tools/compare_numeric.py
.\.venv\Scripts\python.exe -X utf8 PowerBI/ProcurementDashboard/tools/compare_groups.py
```

Không dùng lại cổng của phiên Desktop đã đóng. Static/TMDL không thay thế refresh, DAX và xem giao diện thật.

## Kiểm tra tương tác và giao diện

```powershell
.\.venv\Scripts\python.exe -X utf8 PowerBI/ProcurementDashboard/tools/verify_interactions.py --pid <PID-Desktop>
.\.venv\Scripts\python.exe -X utf8 PowerBI/ProcurementDashboard/tools/capture_pages.py --pid <PID-Desktop>
.\.venv\Scripts\python.exe -X utf8 PowerBI/ProcurementDashboard/tools/finalize_audit.py
```

Kiểm tra tương tác dùng chuột/bàn phím trên đúng cửa sổ dự án, đối chiếu card với SQL, rồi khôi phục toàn kỳ. Không chuyển sang ứng dụng khác trong lúc chạy. Bộ kiểm tra dừng nếu mất foreground. Thư mục `tools/` chỉ giữ công cụ xây dựng, xác minh và điều khiển Desktop đang dùng; `validation/` giữ bằng chứng cuối và schema cần để kiểm tra offline.
