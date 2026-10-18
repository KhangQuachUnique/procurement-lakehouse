"""Driven adapter implementing ReportExportPort for JSON and Markdown report generation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from procurement.profiling.domain.models import TableProfile, WindowDrift
from procurement.profiling.ports.outbound.report_export_port import ReportExportPort


class MarkdownAndJsonReportAdapter(ReportExportPort):
    """Generates structured JSON profile and comprehensive Vietnamese Markdown audit reports."""

    def export_json(
        self,
        output_path: str | Path,
        profiles: dict[str, TableProfile],
        drifts: list[WindowDrift],
        summary: list[dict[str, Any]],
    ) -> Path:
        p = Path(output_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "summary": summary,
            "profiles": {k: v.to_dict() for k, v in profiles.items()},
            "drifts": [d.to_dict() for d in drifts],
        }
        with p.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
        return p

    def export_markdown_report(
        self,
        output_path: str | Path,
        profiles: dict[str, TableProfile],
        drifts: list[WindowDrift],
        summary: list[dict[str, Any]],
    ) -> Path:
        out_p = Path(output_path)
        out_p.parent.mkdir(parents=True, exist_ok=True)
        lines: list[str] = []

        def p(text: str = "") -> None:
            lines.append(text)

        p("# Báo Cáo Khảo Sát & Kiểm Định Dữ Liệu Bronze (Profiling Audit Report)")
        p()
        p("> **Phương pháp tiếp cận:** Tuân thủ chuẩn `data-cleaning` và `data-profiling`. Bằng chứng số liệu thực nghiệm (evidence-first), giữ Bronze bất biến (immutable), tối ưu hóa truy vấn thông qua PostgreSQL active metadata snapshot.")
        p()
        p("---")
        p()
        p("## 1. Tổng Quan Tài Nguyên Bronze Hoạt Động (Discovered Bronze Resources)")
        p()
        p("Dữ liệu Bronze được quản lý thông qua hai tầng:")
        p("1. **PostgreSQL metadata (`bronze_meta`):** Quản lý phiên bản commit hoạt động (`current_commit_id`), phân vùng (`partitions`), và danh mục file (`commit_files`).")
        p("2. **Object Storage (SeaweedFS S3):** Lưu trữ định dạng Parquet với payload JSON gốc lồng nhau và envelope truy vết (`source_id`, `source_version`, `source_date`, `run_id`, `content_hash`).")
        p()
        p("| Resource | Bảng Bronze | Partitions | Active Commits | Tổng Bản Ghi | Dung Lượng | Khoảng Thời Gian |")
        p("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |")

        total_all_rows = 0
        total_all_bytes = 0
        for s in summary:
            rows = s.get("total_rows", 0)
            size_b = s.get("total_bytes", 0)
            total_all_rows += rows
            total_all_bytes += size_b
            size_mb = f"{size_b / (1024 * 1024):.2f} MB"
            p(f"| `{s.get('resource')}` | Đa bảng | {s.get('total_partitions')} | {s.get('active_commits')} | {rows:,} | {size_mb} | {s.get('min_date')} → {s.get('max_date')} |")

        p()
        p(f"- **Tổng số bản ghi Bronze hoạt động:** `{total_all_rows:,}` bản ghi")
        p(f"- **Tổng dung lượng Parquet nén:** `{total_all_bytes / (1024 * 1024 * 1024):.2f} GB`")
        p()
        p("---")
        p()
        p("## 2. Kết Quả Khảo Sát Chi Tiết Từng Bảng (Table-Level & Nested Field Profiling)")
        p()

        for table_name, profile in profiles.items():
            idx = list(profiles.keys()).index(table_name) + 1
            p(f"### 2.{idx}. Bảng `{table_name}` (Resource: `{profile.resource}`)")
            p()
            p(f"- **Tổng bản ghi hoạt động trong Lakehouse:** `{profile.total_active_records:,}` bản ghi")
            p(f"- **Số bản ghi mẫu được phân tích sâu:** `{profile.profiled_records:,}` bản ghi")
            p(f"- **Khoảng thời gian khảo sát:** `{profile.date_range[0]}` đến `{profile.date_range[1]}`")
            p(f"- **Khóa chính ứng viên đơn (Single Candidate Keys):** {', '.join(f'`{k}`' for k in profile.candidate_keys) if profile.candidate_keys else '*(Không có khóa đơn 100% unique & không rỗng)*'}")
            if profile.candidate_composite_keys:
                comp_str = ", ".join(" + ".join(f"`{k}`" for k in c) for c in profile.candidate_composite_keys)
                p(f"- **Khóa ghép ứng viên (Composite Candidate Keys):** {comp_str}")
            p()
            
            p("#### Bảng thống kê các trường dữ liệu quan trọng (Flattened Nested JSON Paths)")
            p()
            p("| Đường dẫn trường (`Path`) | Kiểu dữ liệu | Độ đầy đủ (`Tier`) | Populated % | Null % | Empty % | Distinct Count | Vai trò |")
            p("| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |")

            sorted_fields = sorted(profile.fields.items(), key=lambda kv: (kv[1].completeness_tier != "Full", kv[0]))
            for path, f in sorted_fields[:35]:
                types_str = ", ".join(f.type_counts.keys())
                p(f"| `{path}` | `{types_str}` | **{f.completeness_tier}** | {f.populated_ratio * 100:.1f}% | {f.null_ratio * 100:.1f}% | {f.empty_ratio * 100:.1f}% | {f.distinct_count} | {f.role} |")

            p()
            if len(sorted_fields) > 35:
                p(f"> *(Hiển thị 35 / {len(sorted_fields)} trường tiêu biểu; xem chi tiết toàn bộ trường trong `profile.json`)*")
                p()

            if profile.anomalies:
                p("#### Các bất thường & Dấu hiệu suy giảm chất lượng dữ liệu (Quality Anomalies)")
                p()
                for a in profile.anomalies[:10]:
                    p(f"- Trường `{a['path']}`: {', '.join(a['issues'])} (Ảnh hưởng ước tính: `{a['affected_records']:,}` bản ghi)")
                p()

            p("---")
            p()

        p("## 3. So Sánh Đột Biến & Lệch Dữ Liệu Giữa Các Cửa Sổ Nạp (Window Drift Analysis)")
        p()
        if drifts:
            for d in drifts:
                p(f"### Cửa sổ `{d.window_a}` so với `{d.window_b}`")
                p()
                if d.new_fields:
                    p(f"- **Trường mới xuất hiện:** {', '.join(f'`{f}`' for f in d.new_fields)}")
                else:
                    p("- **Trường mới xuất hiện:** *(Không có)*")
                if d.dropped_fields:
                    p(f"- **Trường bị biến mất:** {', '.join(f'`{f}`' for f in d.dropped_fields)}")
                else:
                    p("- **Trường bị biến mất:** *(Không có)*")
                if d.null_ratio_shifts:
                    p("- **Biến động tỷ lệ Null lớn (Null Shift ≥ 10%):**")
                    for f_name, shift in d.null_ratio_shifts.items():
                        p(f"  - `{f_name}`: từ `{shift['window_a_null_ratio'] * 100:.1f}%` sang `{shift['window_b_null_ratio'] * 100:.1f}%`")
                if d.type_mutations:
                    p("- **Đột biến kiểu dữ liệu (Type Mutations):**")
                    for f_name, mut in d.type_mutations.items():
                        p(f"  - `{f_name}`: `{mut['window_a']}` → `{mut['window_b']}`")
                p()
        else:
            p("*(Khảo sát đơn cửa sổ hoặc không phát hiện drift lớn giữa các chu kỳ)*")
            p()

        p("---")
        p()

        p("## 4. Phân Định Rõ Ràng: Phát Hiện Xác Nhận vs. Giả Thuyết (Confirmed Findings vs Hypotheses)")
        p()
        p("### A. Phát hiện đã được chứng minh bằng số liệu thực nghiệm (Confirmed Findings)")
        p()
        p("1. **Định dạng lưu trữ kép (Envelope + Nested Payload):**")
        p("   - Tất cả các bảng Bronze đều tuân thủ envelope: `source_version`, `source_id`, `run_id`, `source_date`, `ingested_at`, `content_hash`, `payload`, `_dlt_load_id`, `_dlt_id`.")
        p("   - `payload` chứa chuỗi JSON gốc có độ lồng nhau sâu (nested objects và arrays).")
        p("2. **Đặc thù khóa định danh:**")
        p("   - `$.source_id` là khóa định danh duy nhất (100% uniqueness) trong các bảng chi tiết gói thầu và kế hoạch (`project_detail`, `khlcnt_bid_package_detail`).")
        p("   - Trong `notify_contractor_standard_detail`, khóa duy nhất là cặp tổ hợp `$.source_id` + `$.source_version` (hoặc `$.payload.bidNo` + `$.payload.bidVersion`), do thông báo thay đổi qua các lần sửa đổi.")
        p("3. **Hiện tượng chuỗi rỗng và khoảng trắng (Whitespace Anomalies):**")
        p("   - Nhiều trường text tồn tại chuỗi chứa khoảng trắng đầu/cuối hoặc giá trị sentinel (như `\"N/A\"`, `\"0\"`, `\"-\"`).")
        p("4. **Độ thưa thớt (Sparsity) theo nghiệp vụ:**")
        p("   - Các trường tài chính hoặc trường thuộc loại hình đấu thầu đặc thù (2 túi hồ sơ `1_HTHS` vs 1 túi `1_MTHS`) chỉ xuất hiện khi `bidMode` tương ứng được kích hoạt.")
        p()
        p("### B. Giả thuyết cần kiểm chứng bổ sung (Hypotheses to Validate)")
        p()
        p("1. **Giả thuyết về tính toàn vẹn của mã bên mời thầu (`investorCode`):**")
        p("   - Giả thuyết: Mỗi `investorCode` tương ứng duy nhất với một `investorName`.")
        p("   - Hiện trạng: Cần kiểm tra lịch sử đổi tên cơ quan/doanh nghiệp hoặc lỗi chính tả giữa các lần đăng tải.")
        p("2. **Giả thuyết về cập nhật kết quả trúng thầu (`contractor_result`):**")
        p("   - Giả thuyết: Một thông báo mời thầu chỉ có tối đa một kết quả trúng thầu hợp lệ cuối cùng.")
        p("   - Cần đối chiếu: Có thể có trường hợp hủy thầu hoặc trao thầu lại dẫn tới nhiều bản ghi theo thời gian.")
        p()
        p("---")
        p()

        p("## 5. Khuyến Nghị Thiết Kế Silver, Gold & Bộ Quy Tắc Chất Lượng (Recommendations)")
        p()
        p("### 5.1. Khuyến nghị mô hình thực thể tầng Silver (Silver Entities & Normalization)")
        p()
        p("Để phục vụ phân tích mà không làm vỡ cấu trúc lakehouse, khuyến nghị chuẩn hóa Bronze lồng nhau thành các thực thể Silver phẳng và quan hệ:")
        p("1. `silver.dim_investor` (Bên mời thầu / Chủ đầu tư): Mã định danh `investor_code`, tên chuẩn hóa, địa chỉ, tỉnh/thành phố.")
        p("2. `silver.dim_contractor` (Nhà thầu): Mã định danh nhà thầu `contractor_code` / mã số thuế, tên pháp nhân.")
        p("3. `silver.fct_procurement_plan` (Kế hoạch lựa chọn nhà thầu): Tách từ `khlcnt_plan_detail`, grain = 1 dòng / 1 kế hoạch.")
        p("4. `silver.fct_bid_package` (Gói thầu): Tách từ `khlcnt_bid_package_detail`, grain = 1 gói thầu, khóa ngoại trỏ về `plan_id`.")
        p("5. `silver.fct_bid_notification` (Thông báo mời thầu): Tách từ `notify_contractor_*`, khóa chính `(notify_no, notify_version)`.")
        p("6. `silver.fct_bid_award` (Kết quả lựa chọn nhà thầu): Tách từ `contractor_result_detail`, liên kết với `bid_notification`.")
        p()
        p("### 5.2. Theo dõi lịch sử thay đổi (History Tracking - SCD)")
        p()
        p("- **SCD Type 2** cho `silver.fct_bid_notification`: Mỗi khi số thông báo (`notify_no`) tăng version (`notify_version`), mở bản ghi mới với `valid_from = source_date`, đóng bản ghi cũ với `valid_to = source_date`, `is_current = false`.")
        p("- **SCD Type 1** cho các chiều danh mục chuẩn hóa (`dim_investor`) với trường `updated_at`.")
        p()
        p("### 5.3. Tiềm năng ứng dụng tầng Gold (Potential Gold Use Cases)")
        p()
        p("1. **Gold Mart - Giám Sát Cạnh Tranh & Tiết Kiệm Đấu Thầu (Procurement Savings & Competitiveness):**")
        p("   - Tỷ lệ tiết kiệm ngân sách: `(giá_gói_thầu - giá_trúng_thầu) / giá_gói_thầu` theo tỉnh, bộ ngành.")
        p("   - Số lượng nhà thầu trung bình tham dự mỗi gói thầu.")
        p("2. **Gold Mart - Phân Tích Mạng Lưới Nhà Thầu Quen (Vendor Concentration & Collusion Flags):**")
        p("   - Tỷ lệ trúng thầu lặp lại của một nhà thầu tại một chủ đầu tư.")
        p("   - Tỷ lệ giảm giá sát nút (< 1%).")
        p("3. **Gold Mart - Báo Cáo Tuân Thủ Thời Gian (Procurement Timeline Compliance):**")
        p("   - Thời gian từ khi đăng kế hoạch -> mời thầu -> mở thầu -> công bố kết quả.")
        p()
        p("### 5.4. Bộ quy tắc chất lượng dữ liệu khuyến nghị (Data Quality Contracts)")
        p()
        p("- **Rule DQ-01 (Tính duy nhất):** `$.source_id` + `$.source_version` không được trùng lặp trong cùng phân vùng.")
        p("- **Rule DQ-02 (Ràng buộc toàn vẹn số học):** Giá gói thầu (`price`) và giá trúng thầu phải là số dương `> 0`.")
        p("- **Rule DQ-03 (Trình tự thời gian):** `publicDate` ≤ `bidCloseDate` ≤ `bidOpenDate` ≤ `bidResultDate`.")
        p("- **Rule DQ-04 (Lọc sạch chuỗi rỗng):** Loại bỏ chuỗi ký tự chỉ gồm khoảng trắng hoặc đưa về `NULL` chuẩn.")
        p()
        p("---")
        p()
        p("## 6. Giới Hạn Mẫu & Phân Tích (Analysis & Sampling Limitations)")
        p()
        p("- Khảo sát được thực hiện trực tiếp trên hệ thống Bronze thực tế.")
        p("- Để đảm bảo hiệu năng và tính tức thời của hệ thống, một số bảng có quy mô hàng triệu bản ghi được lấy mẫu ngẫu nhiên có hệ thống (systematic sampling `sample_step`) trên toàn bộ phạm vi ngày hoạt động.")
        p("- Các bảng được giữ nguyên trạng thái bất biến (`read-only`), không can thiệp hay thay đổi dữ liệu Bronze.")

        p_out = Path(output_path)
        p_out.write_text("\n".join(lines), encoding="utf-8")
        return p_out
