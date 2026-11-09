"""Seal completed historical Silver fences, without writing/stopping Silver.

An explicit retained vector is derived from chronological publication ACKs.
Never substitute the live current vector, nor cut across a shared commit group.
"""

import argparse
import json
from datetime import date
from pathlib import Path

from procurement.common.silver_gold_contract import INPUT_TABLES
from procurement.readiness_with_exception import seal_with_null_policy
from procurement.silver.application.readiness import _ancestry, save_bundle
from procurement.storage.iceberg import identifier
from procurement.tools.gold_history_candidate import canonical, digest, file_hash, save


def retained_vector(reports, start, end):
    selected = sorted(
        (r for r in reports if str(start) <= r["days"][0]["date"] <= str(end)),
        key=lambda r: (r["days"][0]["date"], r["resource"]),
    )
    vector = {}
    for report in selected:
        entry = report["days"][0]
        group = entry.get("publication_batch")
        if group and (
            group["first"].rsplit("/", 1)[-1] < str(start)
            or group["last"].rsplit("/", 1)[-1] > str(end)
        ):
            raise ValueError("Requested scope cuts across a shared Silver batch")
        vector.update({k: v for k, v in entry["iceberg"].items() if k in INPUT_TABLES})
    if set(vector) != set(INPUT_TABLES):
        raise ValueError("Historical vector lacks required tables")
    return selected, vector


class RetainedFence:
    """Read-only historical catalog view; validates live retention on each read."""

    def __init__(self, catalog, namespace, vector):
        self.catalog, self.namespace, self.vector = catalog, namespace, vector

    def table(self, namespace, table):
        if namespace != self.namespace or table not in self.vector:
            raise ValueError("Outside historical fence")
        response = self.catalog.table(namespace, table)
        meta = response["metadata"]
        pin = self.vector[table]
        sid = pin["snapshot_id"]
        current = meta.get("current-snapshot-id")
        current = current if current is not None and current >= 0 else None
        if (
            meta.get("table-uuid") != pin["table_uuid"]
            or sid is None
            or sid not in {s["snapshot-id"] for s in meta.get("snapshots", [])}
            or not _ancestry(meta, sid, current)
        ):
            raise ValueError("Historical fence changed identity/lost retained ancestry: " + table)
        return response | {"metadata": meta | {"current-snapshot-id": sid}}


def seal(publication, destination, end, db, catalog):
    plan = json.loads((publication / "plan.json").read_text())
    namespace = identifier(plan["namespace"])
    start = date(2022, 1, 1)
    paths = sorted(
        p for p in (publication / "days").glob("*/*/published.json")
        if str(start) <= p.parent.name <= str(end)
    )
    canonical_root = Path(json.loads((publication / "job.json").read_text())["source"])
    reports, inventory = [], []
    for path in paths:
        ack = json.loads(path.read_text())
        checkpoint = canonical_root / "days" / path.parent.parent.name / path.parent.name / "accepted.json"
        expected = digest(canonical({"checkpoint": file_hash(checkpoint), "publication": plan}).encode())
        if ack["identity"] != expected:
            raise ValueError("Publication ACK no longer matches canonical checkpoint")
        report = ack["report"]
        if (report["resource"], report["days"][0]["date"], report["iceberg_namespace"]) != (
            path.parent.parent.name, path.parent.name, namespace
        ):
            raise ValueError("Publication ACK resource/date/namespace differs")
        reports.append(report)
        inventory.append({"path": str(path.resolve()), "sha256": file_hash(path)})
    reports, vector = retained_vector(reports, start, end)
    fence = RetainedFence(catalog, namespace, vector)
    bundle = seal_with_null_policy(
        reports, start=start, end=end, namespace=namespace, catalog=fence,
        count_rows=lambda table, sid: db.execute(
            f"SELECT count(*) FROM lake.{namespace}.{table} AT (VERSION => {sid})"
        ).fetchone()[0],
    )
    bundle["historical_fence"] = {
        "live_source_modified": False, "publication_plan_sha256": file_hash(publication / "plan.json"),
        "publication_ack_inventory": inventory, "vector_source": "chronological retained publication ACKs",
    }
    save_bundle(bundle, destination)
    return bundle


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--publication", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--execute-isolated", action="store_true")
    args = parser.parse_args()
    if not args.execute_isolated:
        raise SystemExit("Explicit isolated acceptance required")
    from dotenv import load_dotenv

    from procurement.common.settings import Settings
    from procurement.storage.iceberg import RestCatalog, connect
    from procurement.tools.gold_history_acceptance import accept

    args.output.mkdir(parents=True, exist_ok=True)
    load_dotenv(args.env_file)
    cfg = Settings.from_environment().iceberg_config()
    with connect(cfg, max_memory="512MB", threads=1) as db, RestCatalog(cfg) as catalog:
        for label, end in (("prefix", date(2022, 12, 31)), ("full", date(2023, 12, 28))):
            path = args.output / (label + "-ready.json")
            if not path.exists():
                print(canonical({"phase": "seal", "scope_end": str(end)}), flush=True)
                bundle = seal(args.publication, path, end, db, catalog)
                print(canonical({"phase": "sealed", "scope_end": str(end),
                                 "rows": {t["table"]: t["rows"] for t in bundle["receipt"]["tables"]}}), flush=True)
        save(args.output / "scope.json", {
            "authorized": "Real Gold acceptance on smaller 2022–2023 range; no migration",
            "source_scope": ["2022-01-01", "2023-12-28"], "prefix_end": "2022-12-31",
            "excluded_dates": ["2023-12-29", "2023-12-30", "2023-12-31"],
            "reason": "Last complete retained calendar-day fence before batch crossing into 2024",
            "silver_written": False, "production_written": False,
        })
        result = accept(args.output / "full-ready.json", args.output / "prefix-ready.json",
                        args.output / "gold", db, catalog,
                        on_progress=lambda s: print(canonical(s), flush=True))
        print(canonical(result), flush=True)


if __name__ == "__main__":
    main()
