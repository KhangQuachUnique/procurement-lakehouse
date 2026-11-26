# Phân tích thị trường mua sắm công Việt Nam

Mở [ProcurementDashboard.pbip](ProcurementDashboard.pbip) trong Power BI Desktop. Báo cáo đọc 8 bảng fact và 9 bảng dimension từ Parquet. Hai bộ lọc **Tháng** và **Lĩnh vực** đồng bộ giữa sáu trang. Các bộ lọc địa phương, phương thức và chỉ số thời gian áp dụng trên trang có bộ lọc tương ứng.

## Đọc sáu trang

| Trang | Câu hỏi và cách đọc | Ảnh Desktop |
|---|---|---|
| Tổng quan thị trường | Biểu đồ đường theo dõi giá trị kế hoạch và giá trúng theo tháng. Vành khuyên cho biết lĩnh vực nào chiếm tỷ trọng lớn trong giá trị trúng. Hai biểu đồ thanh bên dưới so sánh tỷ trọng số gói với tỷ trọng giá kế hoạch theo quy mô và xếp hạng 5 địa phương theo giá trúng. | [Tổng quan](validation/screenshots/final-overview.png) |
| Mức độ cạnh tranh | Đường xu hướng và thanh theo phương thức cho thấy tỷ lệ một nhà thầu. Thanh theo lĩnh vực đối chiếu với tỷ lệ gia quyền của tập chọn. Heatmap phân theo quy mô và lĩnh vực. Tooltip hiển thị tử số, mẫu số và cảnh báo khi dưới 30 lượt mở. | [Cạnh tranh](validation/screenshots/final-competition.png) |
| Dự toán và giá trúng | Cột theo tháng và thanh theo lĩnh vực thể hiện dự toán trừ giá trúng trên cùng mẫu VND, kể cả giá trị âm. Đường biểu diễn tỷ lệ chênh lệch. Thanh chồng 100% cho biết bao nhiêu phần trăm phạm vi đủ điều kiện so sánh. | [Chênh lệch](validation/screenshots/final-savings.png) |
| Thời gian quy trình | Chọn một khoảng thời gian để xem bình quân theo tháng, lĩnh vực và phương thức. Đơn vị giờ hoặc ngày theo chỉ số đã chọn. Cột chồng phân biệt mẫu hợp lệ và mẫu bị loại. | [Thời gian](validation/screenshots/final-duration.png) |
| Thị trường nhà cung cấp | Bảng Top 10 dùng thanh dữ liệu để so sánh giá trị, đồng thời hiển thị tên đầy đủ và khóa người nhận. Hai biểu đồ bên phải cho thấy tỷ trọng Top 10 trong từng lĩnh vực và giá trị theo trạng thái phân bổ. | [Người nhận](validation/screenshots/final-suppliers.png) |
| Chất lượng dữ liệu | Matrix cho biết từng nguồn có bao nhiêu nhóm ghi nhận mỗi tháng. Thanh độ phủ và thanh chồng giúp đọc điều kiện mẫu. Hai card lượng hóa giá trúng thiếu địa phương xác định và giá trị ngoài trạng thái một người nhận. | [Chất lượng](validation/screenshots/final-quality.png) |

## Quy ước chỉ tiêu

- **Quy mô thị trường.** Card số lượng gồm mọi tiền tệ. Các chỉ tiêu giá trị dùng mẫu hợp lệ bằng VND và trình bày theo tỷ đồng. Ngày công bố kế hoạch và ngày công bố kết quả thuộc hai tập sự kiện riêng. Top địa phương loại các khóa chưa xác định và nhiều địa phương chưa phân bổ.
- **Cạnh tranh.** Tỷ lệ một nhà thầu bằng tổng lượt mở có một nhà thầu chia tổng lượt mở trong phạm vi lọc. Benchmark áp dụng cùng công thức trên tập lĩnh vực đang chọn. Khi rà soát nhóm có tỷ lệ cao, đọc thêm cỡ mẫu và hoàn cảnh đấu thầu. [Ví dụ tooltip và cỡ mẫu](validation/screenshots/tooltip-competition.png)
- **Chênh lệch.** Tỷ lệ bằng tổng chênh lệch chia tổng dự toán của cùng mẫu mô tả. Việc xác nhận tiết kiệm ngân sách cần bằng chứng bổ sung.
- **Thời gian.** Bình quân bằng tổng thời lượng chia tổng số mẫu hợp lệ của một metric. Nguồn tổng hợp hỗ trợ bình quân theo từng khoảng thời gian.
- **Người nhận.** CR10 bằng tổng giá trị của 10 khóa đứng đầu chia tổng giá trị đủ điều kiện trong tập chọn. Xếp hạng chỉ xét VND, mẫu có giá hợp lệ, một người nhận và định danh đã biết. Việc xếp theo khóa giữ riêng các đơn vị trùng tên.
- **Độ phủ và ô trống.** Mỗi tỷ lệ độ phủ có mẫu số riêng. Dấu gạch trên card hoặc ô trống biểu thị thiếu quan sát hoặc thiếu mẫu hợp lệ. Đánh giá tính đầy đủ của thu thập cần nguồn đối chiếu độc lập. [Khảo sát nguồn được sinh từ Parquet](validation/source_audit.md)

Vùng màu hổ phách cập nhật theo bộ lọc. Trên trang Tổng quan, vùng này so sánh tỷ trọng giá kế hoạch với tỷ trọng số gói của nhóm từ 100 tỷ VND. Dùng Ctrl+click trong sidebar khi mở bằng Desktop.

## Refresh và bằng chứng nghiệm thu

Khi chuyển máy, đặt tham số **DataRoot** trong **Transform data → Edit parameters** tới thư mục chứa 17 bảng Parquet. Chọn **Refresh**, chờ hoàn tất rồi lưu dự án.

Xem [báo cáo kiểm chứng thiết kế Enterprise Light](validation/editorial-review.md) để đối chiếu ảnh trước/sau, truy vấn hồi quy, tương tác và hiệu năng.

## Kiểm tra và sinh lại

Chạy từ thư mục gốc của repository. Lưu và đóng dự án trong Desktop trước khi chạy generator vì công cụ ghi lại toàn bộ định nghĩa các trang.

```powershell
.\.venv\Scripts\python.exe -X utf8 PowerBI/ProcurementDashboard/tools/profile_sources.py
.\.venv\Scripts\python.exe -X utf8 PowerBI/ProcurementDashboard/tools/build_project.py
.\.venv\Scripts\python.exe -X utf8 PowerBI/ProcurementDashboard/tools/validate_project.py
powershell -NoProfile -ExecutionPolicy Bypass -File PowerBI/ProcurementDashboard/tools/validate_tmdl.ps1
.\.venv\Scripts\python.exe -X utf8 PowerBI/ProcurementDashboard/tools/prepare_numeric_checks.py
.\.venv\Scripts\python.exe -X utf8 PowerBI/ProcurementDashboard/tools/prepare_group_checks.py
```

Mở lại PBIP trong Desktop và thay <port> bằng cổng Analysis Services của phiên vừa mở.

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
```

Kiểm tra tương tác dùng chuột/bàn phím trên đúng cửa sổ dự án, đối chiếu card với SQL, rồi khôi phục toàn kỳ. Không chuyển sang ứng dụng khác trong lúc chạy. Bộ kiểm tra dừng nếu mất foreground. Thư mục `tools/` chỉ giữ công cụ xây dựng, xác minh và điều khiển Desktop đang dùng; `validation/` giữ bằng chứng cuối và schema cần để kiểm tra offline.
