"""Driven adapter implementing ReportExportPort for JSON and Markdown report generation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from procurement.profiling.domain.models import FieldMetrics, TableProfile, WindowDrift
from procurement.profiling.domain.silver_mapping import (
    SILVER_MAPPING_GROUPS,
    lookup_empirical_coverage,
)
from procurement.profiling.ports.outbound.report_export_port import ReportExportPort


def _sanitize_md_cell(value: Any, max_len: int = 45) -> str:
    """Escape markdown table delimiters and truncate overly long sample values."""
    if value is None:
        return "`null`"
    text = str(value).replace("\r", " ").replace("\n", " ").replace("|", "\\|").strip()
    if not text:
        return '`""`'
    if len(text) > max_len:
        text = text[: max_len - 3] + "..."
    return f"`{text}`"


def _format_top_samples(f: FieldMetrics) -> str:
    """Format top frequent values for a field row."""
    if not f.top_values:
        if "object" in f.type_counts:
            return "*(Nested Object)*"
        if "array" in f.type_counts:
            return "*(Nested Array)*"
        return "*(Null / Rỗng)*"

    samples: list[str] = []
    for item in f.top_values[:2]:
        if isinstance(item, (tuple, list)) and len(item) >= 2:
            raw_v, cnt = item[0], item[1]
        elif isinstance(item, dict):
            raw_v, cnt = item.get("value"), item.get("count", 0)
        else:
            raw_v, cnt = item, 1
        val_str = _sanitize_md_cell(raw_v, max_len=32)
        samples.append(f"{val_str} ({cnt})")
    return ", ".join(samples)


def _describe_field_issues(path: str, f: FieldMetrics) -> str:
    """Synthesize specific data quality issues for a single Bronze field."""
    issues: list[str] = []

    if f.null_ratio >= 0.999:
        issues.append("Null 100% (Trường rỗng/không sử dụng)")
    elif f.null_ratio >= 0.80:
        issues.append(f"Thưa thớt cao (Null {f.null_ratio * 100:.1f}%)")
    elif f.null_ratio >= 0.20:
        issues.append(f"Khuyết một phần (Null {f.null_ratio * 100:.1f}%)")

    if f.empty_ratio > 0:
        issues.append(f"Chuỗi/Mảng rỗng ({f.empty_ratio * 100:.1f}%)")

    non_null_types = [t for t in f.type_counts.keys() if t != "null"]
    if len(non_null_types) > 1:
        issues.append(f"Đa kiểu dữ liệu ({', '.join(non_null_types)})")

    for anom in f.anomalies:
        if anom.startswith("sentinel_value_detected:"):
            val = anom.split(":", 1)[1].strip()
            issues.append(f"Giá trị khuyết danh/Sentinel ({val})")
        elif anom == "whitespace_padding":
            padded = f.text_stats.whitespace_padded if f.text_stats else 0
            issues.append(f"Dư khoảng trắng đầu/cuối ({padded} bản ghi)")
        elif anom.startswith("unexpected_negatives:"):
            neg = anom.split(":", 1)[1].strip()
            issues.append(f"Số âm bất thường ({neg})")
        else:
            issues.append(anom)

    # Domain-specific structural patterns
    if path.endswith(("provCode", "districtCode", "wardCode")):
        for tv in f.top_values[:3]:
            v = str(tv[0] if isinstance(tv, (tuple, list)) else tv.get("value", ""))
            if "|" in v:
                issues.append("Mã địa giới bọc dấu pipe `\\|...\\|`")
                break

    if path.endswith(".plocation"):
        for tv in f.top_values[:2]:
            v = str(tv[0] if isinstance(tv, (tuple, list)) else tv.get("value", ""))
            if v.startswith("[{"):
                issues.append("JSON Array bị serialize thành String")
                break

    if not issues:
        return "Không (Clean)"
    return "; ".join(issues)


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

        p("# Báo Cáo Khảo Sát & Kiểm Định Dữ Liệu Bronze - Ánh Xạ Schema Silver (Profiling & Mapping Audit Report)")
        p()
        p(
            "> **Phương pháp tiếp cận:** Tuân thủ chuẩn `data-profiling` và `data-cleaning` (evidence-first). "
            "Báo cáo gồm 2 phần cốt lõi: **(I) Khảo sát đầy đủ 100% trường dữ liệu của từng bảng Bronze dưới dạng bảng kèm chẩn đoán vấn đề chất lượng cụ thể**, "
            "và **(II) Bảng ánh xạ chi tiết từng trường dữ liệu từ Bronze qua các bảng chuẩn hóa tại tầng Silver**."
        )
        p()
        p("---")
        p()

        # =====================================================================
        # PHẦN 1: TỔNG QUAN TÀI NGUYÊN BRONZE HOẠT ĐỘNG
        # =====================================================================
        p("## 1. Tổng Quan Tài Nguyên Bronze Hoạt Động (Discovered Bronze Resources)")
        p()
        p("Dữ liệu Bronze được quản lý đồng bộ qua 2 tầng:")
        p("1. **PostgreSQL Metadata (`bronze_meta`):** Quản lý phiên bản commit hiện hành (`current_commit_id`), phân vùng ngày (`partitions`), và danh mục file vật lý (`commit_files`).")
        p("2. **Object Storage (SeaweedFS S3):** Lưu trữ định dạng Parquet bất biến (immutable) chứa envelope truy vết (`source_id`, `source_version`, `source_date`, `run_id`, `ingested_at`, `content_hash`) và `payload` JSON gốc.")
        p()
        p("| STT | Resource | Các Bảng Bronze Vật Lý | Partitions | Active Commits | Tổng Bản Ghi | Dung Lượng Nén | Khoảng Thời Gian Dữ Liệu |")
        p("| :---: | :--- | :--- | ---: | ---: | ---: | ---: | :---: |")

        resource_tables_map: dict[str, list[str]] = {
            "project": ["`project_detail`"],
            "khlcnt": ["`khlcnt_plan_detail`", "`khlcnt_bid_package_detail`"],
            "notify_contractor": [
                "`notify_contractor_standard_detail`",
                "`notify_contractor_vk_adb_detail`",
                "`notify_contractor_reoffer_detail`",
            ],
            "contractor_result": ["`contractor_result_detail`"],
            "bid_opening": ["`bid_opening_detail`"],
        }

        total_all_rows = 0
        total_all_bytes = 0
        for idx, s in enumerate(summary, start=1):
            res_name = str(s.get("resource", ""))
            rows = int(s.get("total_rows", 0))
            size_b = int(s.get("total_bytes", 0))
            total_all_rows += rows
            total_all_bytes += size_b
            size_mb = f"{size_b / (1024 * 1024):,.2f} MB"
            tbl_list = ", ".join(resource_tables_map.get(res_name, ["Đa bảng"]))
            p(
                f"| {idx} | `{res_name}` | {tbl_list} | {s.get('total_partitions'):,} | "
                f"{s.get('active_commits'):,} | {rows:,} | {size_mb} | `{s.get('min_date')}` → `{s.get('max_date')}` |"
            )

        p()
        p(f"- **Tổng số bản ghi Bronze hoạt động:** `{total_all_rows:,}` bản ghi")
        p(f"- **Tổng dung lượng Parquet nén:** `{total_all_bytes / (1024 * 1024 * 1024):.2f} GB`")
        p()
        p("---")
        p()

        # =====================================================================
        # PHẦN 2: PROFILING ĐẦY ĐỦ TRƯỜNG DỮ LIỆU TỪNG BẢNG BRONZE & VẤN ĐỀ CỤ THỂ
        # =====================================================================
        p("## 2. Khảo Sát Đầy Đủ Trường Dữ Liệu Bronze & Chẩn Đoán Chất Lượng (Full Field-Level Bronze Profiling)")
        p()
        p(
            "Dưới đây là kết quả bóc tách đệ quy **toàn bộ 100% đường dẫn trường (flattened JSON paths)** của từng bảng vật lý trong tầng Bronze. "
            "Mọi trường dữ liệu đều được hiển thị đầy đủ trong bảng kèm theo thông số thống kê và các vấn đề chất lượng dữ liệu cụ thể."
        )
        p()

        for idx, (table_name, profile) in enumerate(profiles.items(), start=1):
            p(f"### 2.{idx}. Bảng `{table_name}` (Resource: `{profile.resource}`)")
            p()
            p(f"- **Tổng bản ghi hoạt động (Active Rows):** `{profile.total_active_records:,}` bản ghi (`{profile.total_active_files:,}` files Parquet)")
            p(f"- **Số bản ghi mẫu phân tích sâu (Sampled Records):** `{profile.profiled_records:,}` bản ghi")
            p(f"- **Khoảng thời gian mẫu khảo sát:** `{profile.date_range[0]}` → `{profile.date_range[1]}`")
            p(f"- **Tổng số trường phát hiện (Total Flattened Paths):** `{len(profile.fields):,}` trường")
            p(
                f"- **Khóa chính ứng viên đơn (Single Candidate Keys):** "
                f"{', '.join(f'`{k}`' for k in profile.candidate_keys) if profile.candidate_keys else '*(Không có khóa đơn 100% unique)*'}"
            )
            if profile.candidate_composite_keys:
                comp_str = ", ".join(
                    " + ".join(f"`{k}`" for k in c) for c in profile.candidate_composite_keys
                )
                p(f"- **Khóa ghép ứng viên (Composite Candidate Keys):** {comp_str}")
            p()

            # Bảng thống kê đầy đủ 100% trường
            p(f"#### 2.{idx}.1. Bảng Thống Kê Đầy Đủ {len(profile.fields)} Trường Dữ Liệu Của `{table_name}`")
            p()
            p("| STT | Đường dẫn trường (`Path`) | Kiểu dữ liệu (`Types`) | Độ đầy đủ (`Tier`) | Populated % | Null % | Empty % | Distinct | Vai trò (`Role`) | Giá trị mẫu (`Top Samples`) | Vấn đề cụ thể phát hiện (`Specific Issues`) |")
            p("| :---: | :--- | :--- | :---: | ---: | ---: | ---: | ---: | :---: | :--- | :--- |")

            # Sắp xếp theo thứ tự bảng chữ cái của Path để cấu trúc cha-con liền mạch dễ tra cứu
            sorted_fields = sorted(profile.fields.items(), key=lambda kv: kv[0])
            for f_idx, (path, f) in enumerate(sorted_fields, start=1):
                types_str = ", ".join(f.type_counts.keys())
                samples_str = _format_top_samples(f)
                issues_str = _describe_field_issues(path, f)
                p(
                    f"| {f_idx} | `{path}` | `{types_str}` | **{f.completeness_tier}** | "
                    f"{f.populated_ratio * 100:.1f}% | {f.null_ratio * 100:.1f}% | "
                    f"{f.empty_ratio * 100:.1f}% | {f.distinct_count:,} | {f.role} | "
                    f"{samples_str} | {issues_str} |"
                )
            p()

            # Tổng hợp các vấn đề chất lượng dữ liệu cụ thể của bảng
            p(f"#### 2.{idx}.2. Tổng Hợp Các Vấn Đề Chất Lượng Dữ Liệu Cụ Thể Trên Bảng `{table_name}`")
            p()

            null_100_fields = [k for k, v in sorted_fields if v.null_ratio >= 0.999]
            sparse_fields = [k for k, v in sorted_fields if 0.20 <= v.null_ratio < 0.999]
            empty_str_fields = [k for k, v in sorted_fields if v.empty_ratio > 0]
            multi_type_fields = [
                (k, [t for t in v.type_counts.keys() if t != "null"])
                for k, v in sorted_fields
                if len([t for t in v.type_counts.keys() if t != "null"]) > 1
            ]

            p("| Nhóm Vấn Đề Chất Lượng (Issue Category) | Số Trường Bị Ảnh Hưởng | Chi Tiết Trường & Bằng Chứng Số Liệu | Hướng Xử Lý Khi Sang Tầng Silver |")
            p("| :--- | :---: | :--- | :--- |")
            p(
                f"| **1. Trường rỗng hoàn toàn (`100% Null`)** | {len(null_100_fields)} | "
                f"{', '.join(f'`{k}`' for k in null_100_fields[:15]) + (f' *(và {len(null_100_fields) - 15} trường khác)*' if len(null_100_fields) > 15 else '') if null_100_fields else '*(Không có)*'} | "
                "Không đưa vào các cột nghiệp vụ chính của Silver; vẫn bảo toàn trong `root_json` / `raw_json`."
            )
            p(
                f"| **2. Trường khuyết/thưa thớt (`20% ≤ Null < 100%`)** | {len(sparse_fields)} | "
                f"{', '.join(f'`{k}` ({profile.fields[k].null_ratio * 100:.1f}%)' for k in sparse_fields[:12]) + (f' *(và {len(sparse_fields) - 12} trường khác)*' if len(sparse_fields) > 12 else '') if sparse_fields else '*(Không có)*'} | "
                "Cho phép `nullable=True` trên schema Silver; thiết lập chuỗi fallback đa tầng (VD: `isMultiLot`, `public_date`, `currency`)."
            )
            p(
                f"| **3. Chuỗi rỗng hoặc Mảng rỗng (`Empty > 0%`)** | {len(empty_str_fields)} | "
                f"{', '.join(f'`{k}` ({profile.fields[k].empty_ratio * 100:.1f}%)' for k in empty_str_fields[:12]) + (f' *(và {len(empty_str_fields) - 12} trường khác)*' if len(empty_str_fields) > 12 else '') if empty_str_fields else '*(Không có)*'} | "
                "Hàm `clean_text()` tự động chuẩn hóa chuỗi rỗng `\"\"` hoặc chỉ chứa khoảng trắng về `NULL`; mảng rỗng `[]` không sinh dòng bảng con."
            )
            p(
                f"| **4. Đa kiểu dữ liệu (`Mixed Types`)** | {len(multi_type_fields)} | "
                f"{', '.join(f'`{k}` ({'/'.join(t)})' for k, t in multi_type_fields[:10]) if multi_type_fields else '*(Không có)*'} | "
                "Ép kiểu tường minh qua bộ parser (`parse_money`, `parse_boolean`, `parse_date`, `parse_iso_datetime`); ghi nhận `quality_issue` nếu sai định dạng."
            )
            p()

            if profile.anomalies:
                p(f"**Danh sách chi tiết toàn bộ {len(profile.anomalies)} cảnh báo dị thường (Anomalies) trên các trường có dữ liệu:**")
                p()
                p("| STT | Trường Dữ Liệu (`Field Path`) | Loại Bất Thường Phát Hiện (`Detected Anomalies`) | Số Bản Ghi Ảnh Hưởng (Trên Mẫu) | Quy Tắc Làm Sạch Tại Silver |")
                p("| :---: | :--- | :--- | ---: | :--- |")
                for a_idx, a in enumerate(profile.anomalies, start=1):
                    a_path = a["path"]
                    a_issues = ", ".join(f"`{i}`" for i in a["issues"])
                    a_cnt = a["affected_records"]
                    if any("whitespace_padding" in i for i in a["issues"]):
                        fix_rule = "Cắt bỏ khoảng trắng thừa đầu/cuối bằng `clean_text()`"
                    elif any("sentinel_value_detected: '0'" in i for i in a["issues"]):
                        if any(flag in a_path for flag in ("isDomestic", "isInternet", "isMultiLot", "isPrequalification", "isMedicine", "isOda", "isOldPlan", "isConcentrateShopping", "isOnlineShopping")):
                            fix_rule = "Giá trị nhị phân hợp lệ (`0` = `False`) -> chuyển thành `BOOLEAN` qua `parse_boolean()`"
                        elif any(k in a_path for k in ("Month", "Year", "Quarter")):
                            fix_rule = "Giá trị sentinel `0` khi tính theo Quý -> chuyển thành `NULL` qua `_month()` / `_positive_year()`"
                        else:
                            fix_rule = "Kiểm tra ngữ cảnh nghiệp vụ; lọc sentinel hoặc giữ nguyên mã danh mục `'0'`"
                    elif any("sentinel_value_detected" in i for i in a["issues"]):
                        fix_rule = "Lọc các chuỗi khuyết danh (`N/A`, `NULL`, `NONE`, `-`) về `NULL`"
                    else:
                        fix_rule = "Kiểm tra ràng buộc miền giá trị và ghi nhận vào `silver.quality_issue`"
                    p(f"| {a_idx} | `{a_path}` | {a_issues} | `{a_cnt:,}` | {fix_rule} |")
                p()

            p("---")
            p()

        # =====================================================================
        # PHẦN 3: BẢNG MAPPING TRƯỜNG DỮ LIỆU QUA CÁC TABLE BÊN SILVER
        # =====================================================================
        p("## 3. Bảng Ánh Xạ Trường Dữ Liệu Từ Bronze Qua Các Bảng Tầng Silver (Bronze to Silver Schema Mapping)")
        p()
        p(
            "Phần này quy định **bảng ánh xạ chi tiết (Field-to-Column Lineage & Transformation Mapping)** từ các bảng vật lý tầng Bronze "
            "sang hệ thống 15 bảng chuẩn hóa quan hệ thuần túy (Pure Relational 1-Hop) tại tầng Silver (DuckDB / Apache Iceberg)."
        )
        p()

        p("### 3.0. Sơ Đồ Tổng Quan Ánh Xạ Thực Thể (Bronze Tables → Silver Tables Architecture)")
        p()
        p("| STT | Resource | Bảng Bronze Nguồn | Bảng Silver Thực Thể Chính (Tier 2 - Revision) | Bảng Silver Thực Thể Con (Tier 3 - 1:N Child) | Bảng Quản Trị & Audit (Tier 1) | Transformer Chịu Trách Nhiệm |")
        p("| :---: | :--- | :--- | :--- | :--- | :--- | :--- |")
        p("| 1 | `project` | `project_detail` | `silver.project_revision` | `silver.project_location` | `silver.observation`, `silver.quality_issue`, `silver.quarantine` | `ProjectDetailTransformer` |")
        p("| 2 | `khlcnt` | `khlcnt_plan_detail` | `silver.plan_revision` | *(Không có bảng con trực tiếp)* | `silver.observation`, `silver.quality_issue`, `silver.quarantine` | `PlanDetailTransformer` |")
        p("| 3 | `khlcnt` | `khlcnt_bid_package_detail` | `silver.package_revision` | `silver.package_location`, `silver.package_lot` | `silver.observation`, `silver.quality_issue`, `silver.quarantine` | `PackageDetailTransformer` |")
        p("| 4 | `notify_contractor` | `notify_contractor_standard_detail`<br>`notify_contractor_vk_adb_detail`<br>`notify_contractor_reoffer_detail` | `silver.notice_revision` | `silver.notice_lot`, `silver.notice_location`, `silver.notice_reoffer_submission` | `silver.observation`, `silver.quality_issue`, `silver.quarantine` | `StandardNoticeTransformer`<br>`VkAdbNoticeTransformer`<br>`ReofferNoticeTransformer` |")
        p("| 5 | `contractor_result` | `contractor_result_detail` | `silver.result_revision` | `silver.award_lot`, `silver.award_contractor` | `silver.observation`, `silver.quality_issue`, `silver.quarantine` | `ContractorResultTransformer` |")
        p()
        p("> **Ghi chú về `khlcnt`:** Tài nguyên Kế hoạch Lựa chọn Nhà thầu bao gồm 2 bảng Bronze (`khlcnt_plan_detail` và `khlcnt_bid_package_detail`), được chuẩn hóa phân rã thành **đúng 4 bảng Silver**: `silver.plan_revision`, `silver.package_revision`, `silver.package_lot` và `silver.package_location`.")
        p()

        for g_idx, group in enumerate(SILVER_MAPPING_GROUPS, start=1):
            p(f"### 3.{g_idx}. {group.title}")
            p()
            p(f"- **Bảng Bronze nguồn:** {', '.join(f'`{bt}`' for bt in group.bronze_tables)}")
            p(f"- **Các bảng Silver đích:** {', '.join(f'`{st}`' for st in group.silver_tables)}")
            p(f"- **Cơ chế chuẩn hóa:** {group.description}")
            p()
            p("| STT | Bảng Silver Đích (`Target Table`) | Cột Silver (`Target Column`) | Kiểu Dữ Liệu Silver | Trường Nguồn Bronze (`Bronze JSON Path`) | Độ Đầy Đủ Thực Nghiệm (`Empirical Coverage`) | Quy Tắc Chuyển Đổi & Làm Sạch (`Transformation & Cleaning Rule`) | Ý Nghĩa Nghiệp Vụ |")
            p("| :---: | :--- | :--- | :---: | :--- | :--- | :--- | :--- |")

            for m_idx, m in enumerate(group.mappings, start=1):
                paths_display = "<br>".join(f"`{bp}`" for bp in m.bronze_paths)
                coverage_display = lookup_empirical_coverage(profiles, m.bronze_table, m.bronze_paths)
                p(
                    f"| {m_idx} | `{m.silver_table}` | `{m.silver_column}` | `{m.silver_type}` | "
                    f"{paths_display} | {coverage_display} | {m.transform_rule} | {m.business_note} |"
                )
            p()

        p("---")
        p()

        # =====================================================================
        # PHẦN 4: SO SÁNH ĐỘT BIẾN & LỆCH DỮ LIỆU GIỮA CÁC CỬA SỔ NẠP (DRIFT)
        # =====================================================================
        p("## 4. So Sánh Dịch Chuyển Schema & Chất Lượng Giữa Các Giai Đoạn (Window Drift Analysis)")
        p()
        if drifts:
            for d in drifts:
                p(f"### So sánh cửa sổ `{d.window_a}` vs. `{d.window_b}`")
                p()
                if d.new_fields:
                    p(f"- **Trường mới xuất hiện ({len(d.new_fields)}):** {', '.join(f'`{f}`' for f in d.new_fields)}")
                else:
                    p("- **Trường mới xuất hiện:** *(Không có)*")
                if d.dropped_fields:
                    p(f"- **Trường ngừng sử dụng ({len(d.dropped_fields)}):** {', '.join(f'`{f}`' for f in d.dropped_fields)}")
                else:
                    p("- **Trường ngừng sử dụng:** *(Không có)*")
                p()
                if d.null_ratio_shifts:
                    p("#### Biến động tỷ lệ Null lớn (Null Ratio Shift ≥ 10%)")
                    p()
                    p("| Trường Dữ Liệu (`Field Path`) | Tỷ Lệ Null Cửa Sổ A (`2022`) | Tỷ Lệ Null Cửa Sổ B (`2025`) | Độ Lệch (`Delta`) | Giải Thích Nghiệp Vụ |")
                    p("| :--- | ---: | ---: | ---: | :--- |")
                    for f_name, shift in d.null_ratio_shifts.items():
                        wa = shift["window_a_null_ratio"] * 100
                        wb = shift["window_b_null_ratio"] * 100
                        delta = wb - wa
                        note = (
                            "Chính thức áp dụng bắt buộc từ cột mốc Golive e-GP mới (16/09/2022)"
                            if wb < wa
                            else "Trường lịch sử từ hệ thống e-GP cũ ngừng sử dụng"
                        )
                        p(f"| `{f_name}` | `{wa:.1f}%` | `{wb:.1f}%` | `{delta:+.1f}%` | {note} |")
                    p()
                if d.type_mutations:
                    p("- **Đột biến kiểu dữ liệu (Type Mutations):**")
                    for f_name, mut in d.type_mutations.items():
                        p(f"  - `{f_name}`: `{mut['window_a']}` → `{mut['window_b']}`")
                    p()
        else:
            p("*(Khảo sát đơn cửa sổ hoặc không phát hiện drift lớn giữa các chu kỳ)*")
            p()

        p_out = Path(output_path)
        p_out.write_text("\n".join(lines), encoding="utf-8")
        return p_out
