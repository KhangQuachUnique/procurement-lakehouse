"""Read-only source audit. Resolve logical tables by name and schema, never glob blindly."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

PROJECT = Path(__file__).resolve().parents[1]
WORKSPACE = PROJECT.parents[1]
FKS = {
    "report_date": ("dim_date", "date_key"),
    "geography_key": ("dim_geography", "geography_key"),
    "sector_key": ("dim_sector", "sector_key"),
    "method_key": ("dim_procurement_method", "method_key"),
    "currency_key": ("dim_currency", "currency_key"),
    "value_band_key": ("dim_value_band", "band_key"),
    "party_key": ("dim_party", "party_key"),
    "metric_key": ("dim_duration_metric", "metric_key"),
    "status_key": ("dim_progress_status", "status_key"),
}
NAMES = sorted([t for t, _ in FKS.values()] + [
    "fact_award_daily", "fact_bidding_competition_daily", "fact_demand_daily",
    "fact_duration_daily", "fact_notice_daily", "fact_savings_daily",
    "fact_status_daily", "fact_supplier_daily",
])


def save(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def discover(root: Path):
    result = {n: [] for n in NAMES}
    unresolved = []
    for p in sorted(root.rglob("*")):
        if not p.is_file() or p.suffix.lower() != ".parquet":
            continue
        rel = p.relative_to(root)
        candidates = [n for n in NAMES if p.stem == n or n in rel.parts[:-1]]
        if len(candidates) != 1:
            unresolved.append(str(rel))
        else:
            result[candidates[0]].append(p)
    if unresolved:
        raise ValueError(f"Unmapped/ambiguous parquet files; provide an explicit layout: {unresolved}")
    missing = [n for n, files in result.items() if not files]
    if missing:
        raise ValueError(f"Missing logical tables: {missing}")
    for name, files in result.items():
        first = pq.read_schema(files[0])
        for p in files[1:]:
            if not first.equals(pq.read_schema(p), check_metadata=False):
                raise ValueError(f"Schema mismatch among parts of {name}: {p}")
    return result


def connection(files):
    con = duckdb.connect()
    con.execute("SET threads=4")
    for name, paths in files.items():
        con.from_parquet([str(p) for p in paths], hive_partitioning=False).create_view(name)
    return con


def records(con, sql):
    cur = con.execute(sql)
    columns = [d[0] for d in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def profile(root):
    files = discover(root)
    con = connection(files)
    report = {"audited_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
              "data_root": str(root), "tables": {}, "relationships": [], "errors": [], "caveats": []}
    for name, paths in files.items():
        schema = pq.read_schema(paths[0])
        cols = schema.names
        pk = "group_key" if name.startswith("fact_") else next(k for t, k in FKS.values() if t == name)
        grain = [c for c in cols if c in FKS] if name.startswith("fact_") else [pk]
        grain += [c for c in ["notice_family", "recipient_allocation_state"] if c in cols]
        info = {"files": [], "columns": [{"name": c.name, "type": str(c.type), "nullable": c.nullable} for c in schema],
                "primary_key": pk, "grain": grain}
        for p in paths:
            pf = pq.ParquetFile(p)
            with p.open("rb") as stream:
                sha = hashlib.file_digest(stream, "sha256").hexdigest()
            info["files"].append({"path": p.relative_to(root).as_posix(), "bytes": p.stat().st_size,
                "rows": pf.metadata.num_rows, "sha256": sha,
                "mtime_utc": dt.datetime.fromtimestamp(p.stat().st_mtime, dt.timezone.utc).isoformat(),
                "created_by": pf.metadata.created_by,
                "metadata": {k.decode(errors="replace"): v.decode(errors="replace") for k, v in (pf.metadata.metadata or {}).items() if k != b"ARROW:schema"}})
        info["rows"], info["distinct_pk"], info["null_pk"] = con.execute(
            f'SELECT count(*), count(DISTINCT "{pk}"), count(*) FILTER (WHERE "{pk}" IS NULL) FROM {name}').fetchone()
        info["duplicate_grains"] = con.execute(f'SELECT count(*) FROM (SELECT {",".join(grain)} FROM {name} GROUP BY ALL HAVING count(*)>1)').fetchone()[0]
        if info["distinct_pk"] != info["rows"] or info["null_pk"] or info["duplicate_grains"]:
            report["errors"].append(f"Invalid PK/grain: {name}")
        expressions = []
        for c in schema:
            expressions.extend([f'count(*) FILTER (WHERE "{c.name}" IS NULL) AS "{c.name}__nulls"',
                                f'approx_count_distinct("{c.name}") AS "{c.name}__approx_distinct"'])
            if str(c.type).startswith(("decimal", "int", "date")):
                expressions += [f'min("{c.name}") AS "{c.name}__min"', f'max("{c.name}") AS "{c.name}__max"']
            if str(c.type).startswith("decimal"):
                expressions += [f'sum("{c.name}") AS "{c.name}__sum"',
                    f'max(abs("{c.name}" - round("{c.name}",4))) AS "{c.name}__currency_rounding_max"',
                    f'sum(abs("{c.name}" - round("{c.name}",4))) AS "{c.name}__currency_rounding_total_bound"',
                    f'count(*) FILTER (WHERE "{c.name}"<0) AS "{c.name}__negatives"']
        info["column_profile"] = records(con, f'SELECT {",".join(expressions)} FROM {name}')[0]
        for c in cols:
            if c.endswith("version") or c in ("recipient_allocation_state", "currency_code", "metric_name", "duration_unit", "notice_family"):
                info.setdefault("categories", {})[c] = records(con, f'SELECT "{c}", count(*) AS rows FROM {name} GROUP BY ALL ORDER BY rows DESC')
        if name.startswith("fact_"):
            for c, (dim, key) in FKS.items():
                if c not in cols:
                    continue
                nulls, orphans = con.execute(f'SELECT count(*) FILTER(WHERE f."{c}" IS NULL), count(*) FILTER(WHERE f."{c}" IS NOT NULL AND d."{key}" IS NULL) FROM {name} f LEFT JOIN {dim} d ON f."{c}"=d."{key}"').fetchone()
                rel = {"fact": name, "foreign_key": c, "dimension": dim, "primary_key": key, "nulls": nulls, "orphans": orphans}
                report["relationships"].append(rel)
                if nulls or orphans:
                    report["errors"].append(f"Broken relationship: {rel}")
        report["tables"][name] = info
        print(f'{name}: {info["rows"]:,} rows, {len(paths)} part(s), PK/grain checked', flush=True)
    report["date_dimension"] = records(con, "SELECT min(date_key) AS first_date,max(date_key) AS last_date,count(*) AS rows,date_diff('day',min(date_key),max(date_key))+1 AS expected_days FROM dim_date")[0]
    report['date_dimension']['continuous']=report['date_dimension']['rows']==report['date_dimension']['expected_days']
    report['date_dimension']['dates_before_2000']=records(con,"SELECT date_key,year_month FROM dim_date WHERE date_key < DATE '2000-01-01' ORDER BY date_key")
    if not report['date_dimension']['continuous']:
        report['caveats'].append('dim_date is not continuous and must not be marked as a standard date table. Preserve source dates; use explicit month/year grouping, not classic time-intelligence functions.')
    report["sample_dimensions"] = {n: records(con, f"SELECT * FROM {n}") for n in ("dim_currency", "dim_duration_metric", "dim_progress_status", "dim_sector", "dim_value_band")}
    report["reconciliations"] = {
        "award_savings": records(con, "SELECT (SELECT sum(award_scope_count) FROM fact_award_daily) AS award_scopes,(SELECT sum(scope_count) FROM fact_savings_daily) AS savings_scopes,(SELECT sum(award_amount_sum) FROM fact_award_daily) AS award_amount,(SELECT sum(descriptive_award_sum) FROM fact_savings_daily) AS descriptive_award"),
        "competition": records(con, "SELECT sum(opening_count) AS openings,sum(single_bidder_count) AS single_count,sum(multi_bidder_count) AS multi_count,sum(total_bidders) AS bidders,sum(tech_passed_count) AS passed,sum(tech_failed_count) AS failed FROM fact_bidding_competition_daily"),
        "allocation": records(con, "SELECT recipient_allocation_state,count(*) AS rows,sum(award_scope_count) AS scopes,sum(award_amount_sum) AS amount FROM fact_supplier_daily GROUP BY ALL"),
    }
    report["caveats"] += [
        "No export manifest/release ID supplied. File mtime is filesystem evidence, not an authoritative export timestamp. Cross-table atomic snapshot cannot be proven from absent metadata.",
        "Date coverage may differ by fact event family; do not infer cohort conversion between daily facts.",
    ]
    save(PROJECT / "validation/source_profile.json", report)
    save(PROJECT / "validation/source_files.json", {n: [p.relative_to(root).as_posix() for p in ps] for n, ps in files.items()})
    if report["errors"]:
        raise ValueError(report["errors"])
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", type=Path, default=WORKSPACE / "PowerBI/export/parquet")
    profile(parser.parse_args().data_root.resolve())
