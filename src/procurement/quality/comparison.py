"""Reconcile two completed audits with a selective repair plan and receipts."""

from collections import Counter
from pathlib import Path

from procurement.ingestion.engine.metadata import calculate_content_hash
from procurement.quality.audit import record_key
from procurement.quality.files import now, read_json, write_json


def _audit(directory):
    directory = Path(directory)
    state = read_json(directory / "selection.json")
    summary = read_json(directory / "summary.json")
    if state["status"] != "complete" or summary["status"] != "complete":
        raise ValueError("Comparison requires completed audits")
    days = {}
    for selection in state["selection"]:
        day = read_json(directory / "days" / f"{selection['date']}.json")
        if calculate_content_hash(day) != state["completed"][selection["date"]]:
            raise ValueError("Audit checkpoint changed")
        days[selection["date"]] = day
    return state, summary, days


def compare_audits(before, after, plan, work, output):
    """No storage/API calls. The after audit must independently verify repair outputs."""
    before_state, before_summary, before_days = _audit(before)
    after_state, after_summary, after_days = _audit(after)
    for key in ("resource", "year", "config_hash", "storage_namespace"):
        if before_state[key] != after_state[key] or before_state[key] != plan[key]:
            raise ValueError(f"Audit/repair scope differs: {key}")
    if calculate_content_hash({k: v for k, v in plan.items() if k != "plan_hash"}) != plan["plan_hash"]:
        raise ValueError("Repair plan changed")
    if set(before_days) != set(after_days):
        raise ValueError("Audit date sets differ")
    planned = {day["selection"]["date"]: day for day in plan["days"]}
    blocked = {day["date"]: day["reason"] for day in plan.get("blocked", [])}
    counts = Counter(copied=0, refetched=0, moved=0, resolved_failures=0)
    day_results = []
    for date, old_day in sorted(before_days.items()):
        new_day = after_days[date]
        item = {"date": date, "before": old_day["status"], "after": new_day["status"]}
        item["remaining_quality"] = dict(Counter(
            row["result"]["status"] for row in new_day.get("rows", [])
            if row["result"]["status"] in {"fail", "unresolved"}
        ))
        if date in blocked:
            item["blocked_reason"] = blocked[date]
        if old_day["status"] != "complete" or new_day["status"] != "complete":
            item["repair_status"] = "integrity_unresolved"
            item["needs_attention"] = True
            day_results.append(item)
            continue
        old = {record_key(row): row for row in old_day["rows"]}
        new = {record_key(row): row for row in new_day["rows"]}
        if len(old) != len(old_day["rows"]) or len(new) != len(new_day["rows"]) or old.keys() != new.keys():
            raise ValueError(f"Identity/version set changed: {date}")
        resolved = sum(row["result"]["status"] == "fail" and
                       new[key]["result"]["status"] in {"pass", "warn"}
                       for key, row in old.items())
        counts["resolved_failures"] += resolved
        item["resolved_failures"] = resolved
        if date not in planned:
            item["repair_status"] = "blocked" if date in blocked else "not_selected"
        else:
            receipt_path = Path(work) / f"{date}.json"
            receipt = read_json(receipt_path) if receipt_path.exists() else {}
            confirmed = (receipt.get("status") == "success"
                         and receipt.get("plan_hash") == plan["plan_hash"]
                         and receipt.get("run_id") == new_day["selection"]["run_id"])
            item["repair_status"] = "verified" if confirmed else "not_verified"
            if not confirmed and receipt.get("error"):
                item["repair_error"] = receipt["error"]
            if confirmed:
                selected = planned[date]
                if selected["selection"] != old_day["selection"]:
                    raise ValueError(f"Before audit differs from planned baseline: {date}")
                if any(row["result"]["status"] not in {"pass", "warn"} for row in new.values()):
                    raise ValueError(f"Committed repair still fails the after audit: {date}")
                day_counts = Counter(copied=0, refetched=0, moved=0)
                for row in selected["records"]:
                    key = record_key(row)
                    if row["content_hash"] != old[key]["content_hash"]:
                        raise ValueError(f"Planned baseline hash differs: {date}")
                    day_counts["refetched" if row["refetch"] else "copied"] += 1
                    day_counts["moved"] += old[key]["table"] != new[key]["table"]
                    if not row["refetch"] and (old[key]["content_hash"] != new[key]["content_hash"]
                                               or old[key]["table"] != new[key]["table"]):
                        raise ValueError(f"Unselected record changed: {date}/{key}")
                if dict(day_counts) != receipt["counts"]:
                    raise ValueError(f"Repair receipt counts differ: {date}")
                counts.update(day_counts)
                item["counts"] = dict(day_counts)
        item["needs_attention"] = bool(item["remaining_quality"]) or item["repair_status"] in {
            "blocked", "not_verified",
        }
        day_results.append(item)
    report = {
        "schema_version": 1, "created_at": now(), "plan_hash": plan["plan_hash"],
        "resource": plan["resource"], "year": plan["year"], "counts": dict(counts),
        "before": {k: before_summary[k] for k in ("records", "quality_status", "days", "tables")},
        "after": {k: after_summary[k] for k in ("records", "quality_status", "days", "tables")},
        "fully_verified": after_summary["fully_verified"],
        "repair_days": dict(Counter(item["repair_status"] for item in day_results)),
        "days": day_results,
    }
    output = Path(output)
    if output.exists():
        raise ValueError("Comparison output exists; choose another output")
    write_json(output / "comparison.json", report)
    lines = [f"# Bronze quality: {plan['resource']} {plan['year']}", "",
             "| Chỉ số | Trước | Sau |", "| --- | ---: | ---: |",
             f"| Records đọc được | {before_summary['records']:,} | {after_summary['records']:,} |"]
    for status in ("pass", "warn", "fail", "unresolved"):
        lines.append(f"| {status} | {before_summary['quality_status'].get(status, 0):,} | "
                     f"{after_summary['quality_status'].get(status, 0):,} |")
    lines.extend(["", (f"Giữ lại: {counts['copied']:,}; tải lại: {counts['refetched']:,}; "
                  f"chuyển bảng: {counts['moved']:,}; hết lỗi: {counts['resolved_failures']:,}."),
                  "", f"Trạng thái ngày: {report['repair_days']}.", "",
                  ("Cảnh báo null/size không phải lỗi đã xác nhận. Ngày thiếu/hỏng dữ liệu "
                  "không được tính là đạt."), "", "## Ngày cần xử lý tiếp", ""])
    for item in day_results:
        if not item["needs_attention"]:
            continue
        remaining = item["remaining_quality"]
        detail = (f"fail={remaining.get('fail', 0)}, "
                  f"unresolved={remaining.get('unresolved', 0)}")
        if item.get("blocked_reason"):
            detail += f"; {item['blocked_reason']}"
        if item.get("repair_error"):
            detail += f"; {item['repair_error'].get('message', 'repair error')}"
        lines.append(f"- {item['date']}: {item['repair_status']} ({item['after']}); {detail}.")
    (output / "comparison.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report
