# Phân tích mua sắm công Việt Nam

Dự án Power BI cục bộ gồm 17 bảng Parquet, bảng `_Measures`, 37 quan hệ một chiều và bốn trang báo cáo. Mở [ProcurementDashboard.pbip](ProcurementDashboard.pbip) bằng Power BI Desktop. Không cần Power BI Service, DuckDB server hoặc custom visual để sử dụng báo cáo.

## Mở và refresh

1. Dùng Power BI Desktop hỗ trợ PBIP, TMDL và PBIR. Bản phát triển được kiểm tra với Desktop **2.154.956.0**; trạng thái kiểm chứng thực tế nằm trong `validation/`.
2. Mở `ProcurementDashboard.pbip`. Report tham chiếu semantic model bằng đường dẫn tương đối.
3. Nếu chuyển máy: vào **Transform data → Edit parameters**, đổi `DataRoot` thành thư mục chứa nguồn Parquet.
4. Chọn **Home → Refresh**. Lần đầu chưa có cache, report có thể hiện trống trước khi refresh hoàn tất. Chờ nạp cả bảng đơn vị có hơn hai triệu dòng.
5. Nhấn Save để lưu dữ liệu cache cục bộ. `.pbi/` là dữ liệu máy cá nhân, không đưa vào Git.

Nguồn mặc định: `D:/Project/procurement-lakehouse/PowerBI/export/parquet`.

Mỗi bảng logic có thể là `table_name.parquet` hoặc gồm nhiều file trong thư mục có tên `table_name` dưới `DataRoot`, bao gồm thư mục con. Hàm `ReadParquetTable` đọc riêng từng bảng, kiểm tra tên cột rồi nối các part cùng bảng. Không trộn schema của các fact/dimension. Công cụ khảo sát từ chối file không xác định được bảng hoặc layout mơ hồ; nếu nguồn dùng tên khác, cần cấu hình ánh xạ có căn cứ trước khi sinh lại. Schema và kiểu của các part được kiểm tra bởi công cụ khảo sát; Power Query gán kiểu tường minh và dừng khi chuyển kiểu lỗi.

## Đọc báo cáo

| Trang | Nội dung | Phạm vi |
|---|---|---|
| Tổng quan | Kế hoạch, trúng thầu, tiết kiệm, địa phương, trạng thái | Measures tiền chỉ VND; các fact theo ngày sự kiện riêng |
| Tiết kiệm và hợp đồng | Dự toán/giá trúng/tiết kiệm cùng tập verified; hợp đồng riêng | Không nối hợp đồng khác tập mẫu vào waterfall |
| Cạnh tranh đấu thầu | Mở thầu, lượt nhà thầu, một/nhiều nhà thầu, kỹ thuật | Export hiện có chỉ từ 07–24/01/2025 |
| Thời gian và đơn vị tham gia | Một metric thời gian và Top 10 đơn vị nhận đơn | Hai khu vực phân tích riêng; metric không lọc đơn vị |

Slicer tháng và lĩnh vực đồng bộ giữa các trang. Chọn tháng không có dữ liệu cạnh tranh sẽ cho kết quả trống, không phải bằng 0. Biểu đồ không tự lọc chéo nhau; slicer là cách lọc chủ động để tránh suy diễn quan hệ giữa các fact. Bộ chọn metric single-select chỉ tác động khu vực thời gian.

Tiền trên report dùng tỷ VNĐ, hai chữ số thập phân; số tiền chưa đủ mẫu hợp lệ để trống. Locale model là `vi-VN`; tùy thiết lập Desktop, cần kiểm tra dấu phân cách thực tế. Tháng và quý có khóa sắp xếp qua các năm. Địa phương giữ phiên bản lịch sử; không tự lọc `is_current` hoặc gộp toàn bộ thành 63 tỉnh/thành.

Card dùng các measures trình bày `Hiển thị · ...` để bảo đảm dấu phân cách `vi-VN` và dấu “—”. Measures số gốc được giữ riêng cho biểu đồ/tính toán. Trục và ô số native vẫn có thể theo locale của Desktop (máy kiểm tra dùng dấu phẩy hàng nghìn và dấu chấm thập phân); đơn vị được ghi rõ. Điều này được ghi trong nghiệm thu, không xem định dạng card là bằng chứng toàn bộ visual đã dùng cùng locale.

## Định nghĩa KPI

| Nhóm | Định nghĩa |
|---|---|
| Kế hoạch | Tổng `planned_amount_sum` có mẫu giá hợp lệ; số gói từ `package_count` |
| Trúng thầu | Tổng `award_amount_sum`; số lượt từ `award_scope_count`, không phải nhà thầu duy nhất |
| Tiết kiệm xác minh | Các tổng `verified_*`; tỷ lệ = tổng tiết kiệm / tổng dự toán, cùng mẫu xác minh và VND |
| Hợp đồng | Tổng các trường `contract_*` khi có `contract_sample_count`; chưa tạo tỷ lệ vì thiếu dự toán cùng mẫu được chứng minh |
| Nhà thầu bình quân | Tổng `total_bidders` / tổng `opening_count`; đây là lượt tham gia |
| Một nhà thầu | Tổng `single_bidder_count` / tổng `opening_count` |
| Đạt kỹ thuật | `tech_passed_count` / (`tech_passed_count` + `tech_failed_count`); chỉ hồ sơ có kết quả |
| Thời gian | `SUM(duration_sum)` / `SUM(sample_count)` cho đúng một metric và dòng có `report_date` |
| Mẫu bị loại có ngày | Tổng `excluded_sample_count` chỉ trên dòng có ngày và một metric |
| Đơn vị nhận đơn | Giá trị VND có mẫu hợp lệ, chỉ `single_recipient`; không chia tiền liên danh/nhiều người nhận |
| Trạng thái | Tổng `package_count` theo trạng thái quan sát thông báo/kết quả; không phải tiến độ thực hiện |

Các tỷ lệ dùng `DIVIDE` và để BLANK khi mẫu số bằng 0. Measures thời gian để BLANK nếu bỏ chọn hoặc có nhiều metric. Không tự quy đổi ngoại tệ. Bộ lọc USD/UNKNOWN giao với measure VND cho BLANK.

## Giới hạn nguồn và quyết định mô hình

- **Duration có lỗi grain:** 8.015 dòng thiếu `report_date`, chỉ có 835 `group_key` khác nhau; tổng mẫu hợp lệ và duration trên các dòng này bằng 0. Các dòng có ngày có khóa duy nhất. Toàn bộ dòng nguồn vẫn được import; không deduplicate, không gán ngày giả và không nối bằng `group_key`. Measures theo ngày loại tường minh nhóm thiếu ngày. Không trình bày tổng mẫu bị loại thiếu ngày như số mẫu duy nhất vì grain lặp chưa giải quyết được. Cần xuất lại nguồn duration với grain không mơ hồ để phục hồi chỉ số này. Bằng chứng trong `source_profile.json` và `source_issue_policy.json`; lỗi nguồn vẫn được ghi, không đổi thành “nguồn sạch”.
- **Provenance chưa đủ:** Parquet không có export manifest/release ID. `mtime` chỉ là thời gian filesystem; không chứng minh thời điểm xuất hay snapshot chung. Phạm vi ngày giữa các fact khác nhau và được giữ nguyên.
- **Bảng ngày không liên tục:** `dim_date` có 2.376 ngày, từ 18/10/1202 đến 02/04/2027; có ngày ngoại lệ năm 1202, 1984 và 1989. Không đánh dấu bảng ngày chuẩn khi không đáp ứng tính liên tục. Giữ nguyên các khóa để không làm mất liên kết nguồn; hierarchy và nhóm tháng vẫn dùng được. Chưa cung cấp YTD/YoY hoặc tự lấp ngày thiếu. Generator chỉ đánh dấu bảng ngày nếu kiểm tra thực tế thấy không null, duy nhất và liên tục.
- **Schema upstream khác export:** schema repository không chứa đầy đủ bảng cạnh tranh và các trường hợp đồng bổ sung của export hiện tại. Không dùng schema cũ để loại những trường nguồn hiện có hoặc khẳng định đã truy được toàn bộ lineage. Các trường bổ sung được giữ, nhưng không tự tạo tỷ lệ hợp đồng hay đơn vị tiền tệ cho bảng cạnh tranh thiếu `currency_key`.
- **Số tiền gần đúng:** nguồn là DECIMAL(38,6), model dùng binary64 (`double`). Không chọn Fixed Decimal vì giới hạn bốn chữ số thập phân và nguy cơ tràn tổng lớn. Giữ nguyên Parquet chính xác; report là công cụ phân tích, không phải sổ kế toán. `precision_assessment.json` ghi phạm vi và chênh lệch tổng sau ép kiểu; `numeric_validation.json` đối chiếu DAX với tổng SQL nguồn.
- **Đơn vị:** `recipient_allocation_state` phân biệt single recipient, liên danh chưa phân bổ, nhiều người nhận và không có người nhận. Chỉ Top 10 người nhận đơn; không suy diễn tỷ lệ chia tiền. `party_display` kèm khóa để tránh gộp hai đơn vị trùng tên.
- **Phạm vi lọc:** duration không có địa lý; supplier không có phương thức. Không tạo quan hệ suy diễn hoặc slicer chung cho các trường này.

Định dạng được chọn: PBIP 1.0, `definition.pbir` 4.0, `definition.pbism` 4.2, PBIR metadata 2.0.0, report schema 3.0.0, page 2.0.0, visualContainer 2.1.0 khi sinh và 2.9.0 cho một số visual sau khi Desktop lưu, TOM compatibility level 1601. Theme được sao chép vào RegisteredResources và được report tham chiếu.

## Chạy kiểm tra

Từ workspace, dùng Python có `duckdb`, `pyarrow`, `jsonschema`; kiểm tra giao diện cần thêm `pywinauto`, `pillow`, Windows và Desktop.

```powershell
.\.venv\Scripts\python.exe -X utf8 PowerBI\ProcurementDashboard\tools\profile_sources.py
.\.venv\Scripts\python.exe -X utf8 PowerBI\ProcurementDashboard\tools\prepare_numeric_checks.py
.\.venv\Scripts\python.exe -X utf8 PowerBI\ProcurementDashboard\tools\prepare_group_checks.py
.\.venv\Scripts\python.exe -X utf8 PowerBI\ProcurementDashboard\tools\assess_precision.py
.\.venv\Scripts\python.exe -X utf8 PowerBI\ProcurementDashboard\tools\validate_project.py
powershell -NoProfile -ExecutionPolicy Bypass -File PowerBI\ProcurementDashboard\tools\validate_tmdl.ps1
```

`profile_sources.py` trả mã lỗi nếu nguồn còn lỗi PK/grain/null FK; đây là phát hiện thực tế, không được bỏ qua. `source_issue_policy.json` mô tả ranh giới phân tích được bảo vệ và yêu cầu sửa nguồn. Không đổi mã lỗi profiler thành pass chỉ vì report có thể mở.

Khi Desktop đã refresh xong, lấy cổng engine của đúng dự án rồi chạy bộ DAX chỉ đọc:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File PowerBI\ProcurementDashboard\tools\engine_check.ps1 -Port <cong-engine> -QueriesFile PowerBI\ProcurementDashboard\validation\dax_queries.json
.\.venv\Scripts\python.exe -X utf8 PowerBI\ProcurementDashboard\tools\compare_numeric.py
.\.venv\Scripts\python.exe -X utf8 PowerBI\ProcurementDashboard\tools\compare_groups.py
```

Không tái sử dụng một cổng cũ sau khi đóng Desktop. `engine_check.ps1` chỉ làm mới dữ liệu nếu truyền `-Refresh`; mặc định kiểm tra trạng thái và chạy query đọc.

Sinh lại dự án chỉ sau khi đóng Desktop hoặc xử lý rõ thay đổi đang mở:

```powershell
.\.venv\Scripts\python.exe -X utf8 PowerBI\ProcurementDashboard\tools\build_project.py
```

Generator ghi lại các file do nó quản lý; không dùng sau khi chỉnh tay report nếu chưa lưu bản thay đổi cần giữ.

## Bằng chứng nghiệm thu

- `source_profile.json`, `source_files.json`: inventory, hash, metadata, PK/grain/FK, phạm vi ngày và loại dữ liệu.
- `source_issue_policy.json`: lỗi nguồn được giữ và ranh giới của measures duration.
- `precision_assessment.json`: phạm vi và giới hạn số thập phân.
- `static_validation.json`: schema JSON/theme, binding, bố cục, slicer bắt buộc và tương tác metric.
- `tmdl_validation.json`: parser TMDL thật của Desktop; không đồng nghĩa đã thực thi M/DAX.
- `numeric_expected.json`, `dax_queries.json`, `dax_results.json`, `numeric_validation.json`: SQL nguồn, DAX thực thi và đối chiếu.
- `engine_state.json`: trạng thái partition tại thời điểm kiểm tra; `NoData` không chứng minh refresh thành công.
- `screenshots/`: ảnh và UI Automation từ cửa sổ Desktop; các ảnh `initial-*`/`*-state` có thể là bằng chứng lỗi hoặc quá trình phát triển, không phải kết quả cuối.

Trạng thái cuối cùng được ghi trong `validation/acceptance.md`. Không gọi dashboard đã hoàn thiện trực quan chỉ dựa trên schema hoặc tọa độ.

Tham chiếu chính thức: [PBIP](https://learn.microsoft.com/en-us/power-bi/developer/projects/projects-overview), [report/PBIR](https://learn.microsoft.com/en-us/power-bi/developer/projects/projects-report), [semantic model](https://learn.microsoft.com/en-us/power-bi/developer/projects/projects-dataset). Schema Microsoft đã tải vào `validation/contracts/` để kiểm tra lại không cần mạng.
