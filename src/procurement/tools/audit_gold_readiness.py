"""Read-only evidence for the four-resource Gold scope (2022–2025).

Writes local aggregates only; never crawls, publishes, or changes source tables.
Each scan uses the immutable metadata location captured at the start.
"""

import argparse
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import sqlalchemy as sa

from procurement.common.settings import Settings
from procurement.infrastructure.database import create_application_engine
from procurement.storage.iceberg import RestCatalog, connect, literal

CORE = (
    "project_revision",
    "plan_revision",
    "package_revision",
    "notice_revision",
    "result_revision",
)
CHILDREN = {
    "project_location": "project_revision",
    "package_location": "package_revision",
    "package_lot": "package_revision",
    "notice_location": "notice_revision",
    "notice_lot": "notice_revision",
    "award_lot": "result_revision",
    "award_contractor": "result_revision",
}
RESOURCES = ("project", "khlcnt", "notify_contractor", "contractor_result")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--catalog-uri", default="http://127.0.0.1:8181")
    parser.add_argument("--s3-endpoint", default="http://127.0.0.1:8333")
    parser.add_argument(
        "--resume-final-checks",
        action="store_true",
        help="Reuse completed inventory/join checks only when all captured snapshots still match",
    )
    args = parser.parse_args()
    settings = Settings.from_environment()
    config = settings.iceberg_config(catalog_uri=args.catalog_uri, s3_endpoint=args.s3_endpoint)
    report = {
        "started_at": datetime.now(UTC).isoformat(),
        "period": ["2022-01-01", "2025-12-31"],
        "resources": RESOURCES,
        "snapshots": {},
        "queries": {},
        "tables": {},
    }
    if args.resume_final_checks:
        report = json.loads(args.output.read_text())
        if "result_notice_version" not in report["queries"]:
            raise ValueError("Resume requires completed inventories and join checks")
        report["resumed_at"] = datetime.now(UTC).isoformat()
    args.output.parent.mkdir(parents=True, exist_ok=True)

    def save():
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n")

    db = connect(config, max_memory="4GB", threads=1)
    db.execute("SET TimeZone='Asia/Ho_Chi_Minh'")
    db.execute("SET http_retries=5")
    db.execute("SET http_timeout=120")
    db.execute("SET http_keep_alive=false")

    def query(name, sql):
        result = db.execute(sql)
        columns = [d[0] for d in result.description]
        report["queries"][name] = {
            "sql": sql,
            "rows": [dict(zip(columns, row)) for row in result.fetchall()],
        }
        save()
        print(name, flush=True)
        return report["queries"][name]["rows"]

    with RestCatalog(config) as catalog:
        for table in (*CORE, *CHILDREN, "observation", "quality_issue", "quarantine"):
            info = catalog.table("silver", table)
            if (
                args.resume_final_checks
                and report["snapshots"][table]["metadata_location"] != info["metadata-location"]
            ):
                raise ValueError("Snapshots changed; run a fresh audit")
            report["snapshots"][table] = {
                "metadata_location": info["metadata-location"],
                "snapshot_id": info["metadata"].get("current-snapshot-id"),
            }
            db.execute(
                f"CREATE TEMP VIEW raw_{table} AS SELECT * FROM iceberg_scan({literal(info['metadata-location'])})"
            )
        save()

        # Metadata only; a committed empty day is valid and need not have observations.
        engine = create_application_engine()
        try:
            with engine.connect() as conn, conn.begin():
                conn.execute(sa.text("SET TRANSACTION READ ONLY"))
                sql = """SELECT resource, extract(year from source_date)::int AS year,
                    count(*) AS partitions, count(current_commit_id) AS committed_days,
                    min(source_date) AS first_day, max(source_date) AS last_day
                    FROM bronze_meta.partitions
                    WHERE source='muasamcong' AND resource IN :resources
                    AND source_date BETWEEN '2022-01-01' AND '2025-12-31'
                    GROUP BY resource, extract(year from source_date) ORDER BY resource, year"""
                statement = sa.text(sql).bindparams(sa.bindparam("resources", expanding=True))
                report["bronze_coverage"] = [
                    dict(row)
                    for row in conn.execute(statement, {"resources": RESOURCES}).mappings()
                ]
        finally:
            engine.dispose()
        save()

        if not args.resume_final_checks:
            for table in CORE:
                cols = [r[0] for r in db.execute(f"DESCRIBE raw_{table}").fetchall()]
                report["tables"][table] = cols
                query(
                    table + "_inventory",
                    f"""SELECT count(*) row_count, count(DISTINCT revision_id) revisions,
                    count(DISTINCT entity_id) entities, count(*) FILTER(WHERE is_current) marked_current,
                    min(public_date) first_public_date, max(public_date) last_public_date FROM raw_{table}""",
                )
                # Diagnostic deterministic representative, NOT an accepted business-version rule.
                projection = ", ".join('r."' + c + '"' for c in cols if c != "root_json")
                db.execute(f"""CREATE TEMP TABLE selected_{table} AS SELECT {projection} FROM raw_{table} r
                    WHERE public_date BETWEEN DATE '2022-01-01' AND DATE '2025-12-31'
                    QUALIFY row_number() OVER(PARTITION BY entity_id ORDER BY public_date DESC NULLS LAST,
                    revision_id DESC)=1""")
                fields = [c for c in cols if c not in {"root_json", "entity_type", "is_current"}]
                query(
                    table + "_nonnull",
                    "SELECT count(*) AS row_count, "
                    + ", ".join(f'count("{c}") AS "{c}"' for c in fields)
                    + f" FROM selected_{table}",
                )
                query(
                    table + "_year",
                    f"""SELECT year(public_date) AS publication_year, count(*) entities FROM selected_{table}
                    GROUP BY 1 ORDER BY 1""",
                )
                for c in ("currency", "status_raw", "bid_field", "bid_form", "source_family"):
                    if c in cols:
                        query(
                            table + "_" + c,
                            f'SELECT "{c}" AS category_value, count(*) row_count FROM selected_{table} GROUP BY 1 ORDER BY 2 DESC LIMIT 20',
                        )
                for c in (
                    "total_investment",
                    "bid_price",
                    "total_winning_price",
                    "guarantee_amount",
                ):
                    if c in cols:
                        query(
                            table + "_" + c,
                            f"""SELECT count({c}) populated, count(*) FILTER(WHERE {c}<=0) nonpositive,
                            min({c}) minimum, median({c}) median, max({c}) maximum FROM selected_{table}""",
                        )

            for child, parent in CHILDREN.items():
                query(
                    child + "_inventory",
                    f"""SELECT count(*) row_count, count(DISTINCT revision_id) parent_revisions,
                    count(*) FILTER(WHERE p.revision_id IS NULL) orphan_rows
                    FROM raw_{child} c LEFT JOIN (SELECT DISTINCT revision_id FROM raw_{parent}) p USING(revision_id)""",
                )
                cols = [r[0] for r in db.execute(f"DESCRIBE raw_{child}").fetchall()]
                report["tables"][child] = cols
                query(
                    child + "_selected_nonnull",
                    "SELECT count(*) row_count, "
                    + ", ".join(
                        f'count(c."{c}") AS "{c}"'
                        for c in cols
                        if c not in {"raw_json", "revision_id"}
                    )
                    + f" FROM raw_{child} c JOIN selected_{parent} p USING(revision_id)",
                )

            for name, left, lk, right, rk in (
                ("package_plan_id", "package_revision", "plan_id", "plan_revision", "root_id"),
                ("package_plan_no", "package_revision", "plan_no", "plan_revision", "plan_no"),
                (
                    "notice_package_no",
                    "notice_revision",
                    "package_no",
                    "package_revision",
                    "package_no",
                ),
                (
                    "result_notice_no",
                    "result_revision",
                    "notify_no",
                    "notice_revision",
                    "notify_no",
                ),
                (
                    "result_package_no",
                    "result_revision",
                    "package_no",
                    "package_revision",
                    "package_no",
                ),
            ):
                if rk == "root_id":
                    # No explicit plan source-id column; root id retained in source JSON.
                    db.execute(
                        "CREATE TEMP VIEW plan_ids AS SELECT p.entity_id, json_extract_string(r.root_json,'$.id') root_id FROM selected_plan_revision p JOIN raw_plan_revision r USING(revision_id)"
                    )
                    target = "plan_ids"
                else:
                    target = "selected_" + right
                query(
                    name,
                    f"""WITH keys AS (SELECT {rk} k, count(*) n FROM {target} WHERE {rk} IS NOT NULL GROUP BY 1)
                    SELECT count(*) source_entities, count(l.{lk}) populated_keys,
                    count(*) FILTER(WHERE k.n=1) unique_matches,
                    count(*) FILTER(WHERE k.n>1) ambiguous_matches,
                    count(*) FILTER(WHERE l.{lk} IS NOT NULL AND k.n IS NULL) unmatched_keys
                    FROM selected_{left} l LEFT JOIN keys k ON l.{lk}=k.k""",
                )

            query(
                "notice_intervals",
                """SELECT count(*) row_count,
                count(*) FILTER(WHERE published_at IS NOT NULL AND bid_close_date IS NOT NULL) paired,
                count(*) FILTER(WHERE bid_close_date<published_at) negative,
                median(date_diff('hour', published_at,bid_close_date)/24.0) median_days
                FROM selected_notice_revision""",
            )
            query(
                "result_notice_version",
                """SELECT count(*) result_entities,
                count(*) FILTER(WHERE r.notify_version IS NOT NULL AND n.notify_version IS NOT NULL) both_versions,
                count(*) FILTER(WHERE r.notify_version=n.notify_version) same_version
                FROM selected_result_revision r JOIN selected_notice_revision n ON r.notify_no=n.notify_no""",
            )
        # Stream JSON per batch instead of expanding nested JSON in one large join.
        inventory = report["queries"]["result_revision_inventory"]["rows"][0]
        unique_entities = inventory["row_count"] == inventory["entities"]
        if not args.resume_final_checks:
            for table in CORE:
                if unique_entities or table != "result_revision":
                    db.execute(f"DROP TABLE selected_{table}")
        if args.resume_final_checks and not unique_entities:
            raise ValueError("Full audit required to rebuild selected result revisions")
        raw_sql = "SELECT root_json FROM raw_result_revision WHERE public_date BETWEEN DATE '2022-01-01' AND DATE '2025-12-31'"
        if not unique_entities:
            raw_sql = "SELECT r.root_json FROM raw_result_revision r JOIN selected_result_revision s USING(revision_id)"
        outcomes = Counter()
        with_price = Counter()
        scanned = 0
        reader = db.execute(raw_sql).to_arrow_reader(batch_size=32)
        for batch in reader:
            for encoded in batch.column(0).to_pylist():
                scanned += 1
                root = json.loads(encoded)
                for lot in root.get("lotResultDTO") or []:
                    if not isinstance(lot, dict):
                        continue
                    for contractor in lot.get("contractorList") or []:
                        if not isinstance(contractor, dict):
                            continue
                        outcome = contractor.get("bidResult")
                        key = (
                            type(outcome).__name__,
                            str(outcome) if outcome is not None else None,
                        )
                        outcomes[key] += 1
                        with_price[key] += contractor.get("bidWiningPrice") is not None
            if scanned % 100000 < 32:
                print(f"raw results scanned: {scanned}", flush=True)
        report["queries"]["result_raw_outcomes"] = {
            "sql": raw_sql,
            "method": "Stream batches of 32 roots; count lotResultDTO[].contractorList[].bidResult by JSON type and value",
            "scanned_result_rows": scanned,
            "rows": [
                {"outcome_type": k[0], "outcome": k[1], "row_count": v, "with_price": with_price[k]}
                for k, v in outcomes.most_common()
            ],
        }
        save()
        print("result_raw_outcomes", flush=True)
        query(
            "notice_version_order",
            """SELECT count(*) row_count,
            count(*) FILTER(WHERE try_cast(notify_version AS BIGINT)>=10) numeric_version_ge_10,
            count(*) FILTER(WHERE notify_version IS NOT NULL AND try_cast(notify_version AS BIGINT) IS NULL) nonnumeric
            FROM raw_notice_revision""",
        )
        query(
            "quality",
            "SELECT code, severity, count(*) row_count FROM raw_quality_issue GROUP BY 1,2 ORDER BY 3 DESC LIMIT 40",
        )
        query(
            "quarantine",
            "SELECT reason, count(*) row_count FROM raw_quarantine GROUP BY 1 ORDER BY 2 DESC",
        )
        report["snapshots_unchanged"] = all(
            catalog.table("silver", t)["metadata"].get("current-snapshot-id") == v["snapshot_id"]
            for t, v in report["snapshots"].items()
        )
    db.close()
    report["finished_at"] = datetime.now(UTC).isoformat()
    report["selection_note"] = (
        "Diagnostic only: rows with public_date in 2022–2025, public_date DESC, revision_id DESC per entity; raw inventory covers all stored dates; not certified latest version or historical as-of state. Null counts are before metric eligibility."
    )
    save()


if __name__ == "__main__":
    main()
