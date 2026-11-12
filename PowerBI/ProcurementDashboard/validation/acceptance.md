# Biên bản kiểm tra bàn giao

Ngày kiểm tra: 12/11/2026. Dự án đã được sinh thực tế, mở, refresh và kiểm tra hiển thị bốn trang trong **Power BI Desktop 2.154.956.0**. Không publish hoặc deploy lên dịch vụ ngoài.

Đã bàn giao đủ PBIP, semantic model TMDL, report PBIR, theme đã tích hợp, README và công cụ kiểm tra. Model gồm 17 bảng nguồn được giữ nguyên chi tiết, `_Measures` với 65 measures và 37 quan hệ một chiều. Report có bốn trang, 63 visual containers gồm biểu đồ, bảng, KPI, slicer, chữ và điều hướng native.

## Bằng chứng đã thực hiện

| Kiểm tra | Kết quả / bằng chứng |
|---|---|
| Khảo sát toàn bộ nguồn | 17 bảng, 4.354.496 dòng; schema, PK/grain/FK, null, ngày, currency, metric và allocation state: [source_audit.md](source_audit.md), [source_profile.json](source_profile.json). Profiler vẫn trả lỗi cho grain/null ngày duration, không đổi lỗi nguồn thành pass. |
| Cấu trúc và bindings | 75 JSON theo schema Microsoft; bốn trang, 63 containers, bindings, đường dẫn, canvas, tương tác: [static_validation.json](static_validation.json). |
| TMDL | Parser thật `TmdlSerializer` của Desktop đọc thành công; đối chiếu inventory measures/columns/relationships: [tmdl_validation.json](tmdl_validation.json). |
| Refresh | Cả 18 partitions ở trạng thái Ready, không có lỗi: [engine_state.json](engine_state.json). Dữ liệu nguồn không bị sửa hoặc deduplicate. |
| DAX thực thi | 113 truy vấn chạy trên engine Desktop, không lỗi; 46 kết quả scalar/rowcount đối chiếu SQL độc lập trên Parquet: [numeric_validation.json](numeric_validation.json). Gồm toàn dữ liệu, lát cắt tháng 01/2025/lĩnh vực HH, VND/khác VND, BLANK, mẫu số 0, metric ngày/giờ, nhiều/không có metric. |
| Top 10 | Hai danh sách địa phương và đơn vị: đối chiếu cả danh tính và số tiền, tổng 20 dòng: [group_validation.json](group_validation.json). |
| Nhiều Parquet parts | Kiểm tra năm nhóm tình huống layout: [layout_validation.json](layout_validation.json). Power Query thực tế đọc hai parts dim_date lồng thư mục, đủ 2.376 ngày duy nhất; sau đó khôi phục DataRoot và refresh lại: [multipart_refresh_validation.json](multipart_refresh_validation.json). |
| Render và navigation | Đã xem cả bốn trang, dùng Page Navigator native để chuyển trang và xác minh active: [page_capture_validation.json](page_capture_validation.json). Ảnh cuối: [Tổng quan](screenshots/final-overview.png), [Tiết kiệm](screenshots/final-savings.png), [Cạnh tranh](screenshots/final-competition.png), [Thời gian](screenshots/final-duration.png). |
| Bộ lọc và trạng thái trống | Chọn HH tại Tổng quan làm thay đổi số liệu; chuyển trang Cạnh tranh vẫn giữ HH. Chọn tháng 1202-10 cho BLANK kèm thông báo không có dữ liệu, không dựng số giả. Sau kiểm tra đã bỏ chọn cả hai bộ lọc. Xem [HH](screenshots/ui-sector-hh-final.png), [đồng bộ và trống](screenshots/ui-empty-and-sync-final.png), [reset](screenshots/ui-sector-reset-final.png). |
| Metric thời gian | Thử metric giờ trong Desktop: KPI 223,0 giờ với 339.075 mẫu; bảng đơn vị giữ nguyên phạm vi. Xem [ảnh kiểm tra](screenshots/interaction-hours-real.png). Bản bàn giao mặc định metric ngày Kế hoạch → thông báo; measure vẫn bảo vệ khi nhiều/không có metric. |
| Toàn vẹn nguồn | SHA-256 trước/sau của cả 17 files: [source_integrity.json](source_integrity.json). |

## Giới hạn được giữ và công bố

- **Nguồn chưa sạch:** 8.015 dòng duration không có ngày và grain bị lặp; toàn bộ vẫn được import. Measures thời gian chỉ tính dòng có ngày và đúng một metric. Không cộng nhóm excluded thiếu ngày như số mẫu duy nhất. Cần re-export upstream để khắc phục; xem [source_issue_policy.json](source_issue_policy.json).
- `dim_date` không liên tục và có ngày năm 1202/1984/1989. Giữ nguyên khóa/hierarchy, không đánh dấu date table chuẩn và không tạo YTD/YoY khi chưa đủ điều kiện.
- Toàn bộ `verified_sample_count` hiện bằng 0. Các chỉ số tiết kiệm xác minh và biểu đồ phụ thuộc mẫu này để trống có giải thích. Hợp đồng là tập riêng (311 mẫu); không dựng tỷ lệ với dự toán không cùng mẫu. Trang tiết kiệm có vùng biểu đồ trống hợp lệ do nguồn, không phải lỗi binding.
- Không chứng minh được common export snapshot từ metadata. Schema upstream trong repository chưa mô tả đủ các trường mới của export; không tuyên bố đã xác minh toàn bộ lineage. Tiền cạnh tranh không hiển thị vì thiếu căn cứ đơn vị/currency.
- DECIMAL(38,6) được ánh xạ double sau khảo sát phạm vi. Kiểm tra tổng tiền cho phép sai số tuyệt đối **5 VNĐ**, tỷ lệ **1e-10**, duration **1e-9**; các đối chiếu đạt. Không dùng báo cáo thay sổ kế toán chính xác: [precision_assessment.json](precision_assessment.json).
- Card hiển thị đúng dấu phân cách `vi-VN`; trục/ô số native trên Desktop đang dùng locale tiếng Anh vẫn có dấu phẩy hàng nghìn, dấu chấm thập phân và nhãn tháng tiếng Anh. **Chưa đáp ứng đồng nhất vi-VN trên mọi visual trong môi trường hiện tại.** Cần xác nhận lại khi dùng Desktop/Windows locale Việt Nam; không coi format string là bảo đảm cho mọi máy.
- Đã xem render thực tế và thử các tương tác nêu trên. Chưa kiểm thử mọi tổ hợp slicer, mọi tooltip khi hover, mọi tên đơn vị dài hoặc mọi cỡ màn hình. Các kiểm tra DAX/static không thay thế những bước này.

Không gọi kết quả “đã kiểm chứng đầy đủ” hoặc nguồn dữ liệu không có lỗi. Bản bàn giao hoạt động trong Desktop đã kiểm tra, với các giới hạn dữ liệu và locale nêu rõ. [completion_audit.json](completion_audit.json) tổng hợp kiểm tra; [README](../README.md) hướng dẫn mở, đổi DataRoot và chạy lại.
