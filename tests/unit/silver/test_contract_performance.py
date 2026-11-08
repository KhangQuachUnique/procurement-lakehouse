import copy
import json
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from procurement.silver.application.resources import get_silver_resource
from procurement.silver.application.revisions import assemble, validate
from procurement.silver.application.warehouse import ensure_views, typed_arrow_rows
from procurement.silver.domain.protocols import RawRecordEnvelope
from procurement.silver.domain.registry import require_transformer
from procurement.silver.domain.schema import get_partition_spec, get_silver_schemas

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "contract_performance"


def envelope(table, payload, ordinal=0):
    root = payload["contract" if table == "contract_detail" else "performance"]
    return RawRecordEnvelope(
        namespace="muasamcong",
        table_name=table,
        resource="contract_performance",
        source_date="2025-10-23",
        run_id="run",
        row_ordinal=ordinal,
        file_key=f"{table}.parquet",
        file_sha256="sha",
        source_id=root["id"],
        source_version=root.get("contractVersion")
        if table == "contract_detail"
        else root["version"],
        content_hash="content",
        ingested_at=datetime(2026, 11, 3, tzinfo=UTC),
        payload=payload,
    )


@pytest.mark.parametrize("year", [2024, 2025])
def test_full_profiled_samples_reconcile_and_cast_to_arrow(year):
    data = json.loads((FIXTURES / f"{year}.json").read_text())
    transformed = []
    for i, raw in enumerate(data["contracts"]):
        transformed.append(
            require_transformer("contract_detail")
            .transform(envelope("contract_detail", raw, i))
            .to_dict()
        )
    for i, raw in enumerate(data["linked"]):
        transformed.append(
            require_transformer("contract_performance_detail")
            .transform(
                envelope(
                    "contract_performance_detail",
                    {
                        "performance": raw["resultList"][0],
                        "linked": raw,
                        "resolution": {"status": "resolved"},
                    },
                    i,
                )
            )
            .to_dict()
        )
    tables = assemble(transformed)
    report = validate(tables)
    assert report["accepted"] == 24
    assert report["quarantined"] == 0
    assert report["blocking_issues"] == 0
    for name in ["contract_revision", "contract_performance_revision"]:
        arrow = typed_arrow_rows(name, tables[name])
        assert arrow.num_rows == 12
        assert arrow["source_date"][0].as_py() == date(2025, 10, 23)
    arrow = typed_arrow_rows("contract_revision", tables["contract_revision"])
    assert isinstance(arrow["price_after_tax"][0].as_py(), Decimal)
    contract = tables["contract_revision"][0]
    assert "completed_value" not in contract
    assert "price_before" not in contract


@pytest.mark.parametrize("table", ["contract_detail", "contract_performance_detail"])
def test_unrelated_context_does_not_create_revision(table):
    data = json.loads((FIXTURES / "2025.json").read_text())
    payload = (
        data["contracts"][0]
        if table == "contract_detail"
        else {
            "performance": data["linked"][0]["resultList"][0],
            "linked": data["linked"][0],
            "resolution": {"status": "resolved"},
        }
    )
    raw = envelope(table, payload)
    transformer = require_transformer(table)
    before = transformer.transform(raw)
    changed = copy.deepcopy(payload)
    changed["search"] = {"context": "changed"}
    changed["versionsList"] = []
    changed["linked"] = {"resultList": []}
    after = transformer.transform(replace(raw, payload=changed))
    assert before.revision.revision_id == after.revision.revision_id
    assert before.observation["payload_json"] != after.observation["payload_json"]
    root_key = "contract" if table == "contract_detail" else "performance"
    changed[root_key]["contractorName"] = "Changed source name"
    assert transformer.transform(replace(raw, payload=changed)).revision != before.revision


def test_invalid_values_warn_and_preserve_source_root():
    payload = {
        "performance": {
            "id": "report",
            "version": "00",
            "contractCode": "HD001",
            "contractVersion": "00",
            "publicDate": "bad-date",
            "isContractComplete": 7,
            "contractStatus": "99",
        },
        "resolution": {"status": "missing_contract_version"},
    }
    result = require_transformer("contract_performance_detail").transform(
        envelope("contract_performance_detail", payload)
    )
    assert result.quarantine is None
    assert result.typed_attributes["public_date"] is None
    assert result.typed_attributes["is_completed"] is None
    assert result.typed_attributes["progress_status_code"] == "99"
    assert {i.code for i in result.issues} == {
        "invalid_value",
        "unknown_progress_status",
        "missing_contract_version",
    }
    assert "bad-date" in result.typed_attributes["root_json"]


def test_schemas_registration_and_partitions():
    resource = get_silver_resource("contract_performance")
    assert resource.select_tables("performance") == ("contract_performance_detail",)
    assert set(resource.output_tables) == {
        "observation",
        "contract_revision",
        "contract_performance_revision",
        "quality_issue",
        "quarantine",
    }
    for table in ["contract_revision", "contract_performance_revision"]:
        assert get_partition_spec(table) == "source_date"


def test_latest_and_exact_version_views_are_global():
    db = duckdb.connect()
    try:
        db.execute("ATTACH ':memory:' AS lake")
        db.execute("CREATE SCHEMA lake.silver")
        schema = get_silver_schemas()["contract_revision"]
        db.execute(f"CREATE TABLE lake.silver.contract_revision ({schema})")
        for revision, version, day in [
            ("a", "00", "2025-10-20"),
            ("b", "01", "2025-10-21"),
            ("c", "00", "2025-10-23"),
        ]:
            db.execute(
                """INSERT INTO lake.silver.contract_revision
                (revision_id,entity_id,entity_type,source_date,observed_at,root_json,
                 contract_version,contract_code,is_current)
                VALUES (?, 'entity', 'contract', ?, ?, '{}', ?, 'HD001', true)""",
                [revision, day, day + "T00:00:00+00:00", version],
            )
        views = ensure_views(db)
        assert "silver.v_contract_version" in views
        assert db.execute("SELECT revision_id FROM silver.v_latest_contract").fetchall() == [("b",)]
        assert db.execute(
            "SELECT revision_id FROM silver.v_contract_version ORDER BY revision_id"
        ).fetchall() == [("b",), ("c",)]
    finally:
        db.close()


def test_nonempty_and_empty_refresh_remove_stale_domain_rows_by_source_date():
    from unittest.mock import MagicMock

    from procurement.silver.application.warehouse import stage_partition

    db, catalog = MagicMock(), MagicMock()
    catalog.table.return_value = {"metadata": {"format-version": 2, "current-snapshot-id": 1}}
    db.sql.return_value.fetchone.return_value = [1]
    rows = [
        {
            "revision_id": "r",
            "entity_id": "e",
            "entity_type": "contract_performance",
            "source_date": date(2025, 10, 17),
            "root_json": "{}",
        }
    ]
    stage_partition(
        db,
        catalog,
        "silver",
        {"contract_performance_revision": rows},
        partition_date=date(2025, 10, 17),
    )
    sql = [call.args[0] for call in db.execute.call_args_list]
    assert (
        "DELETE FROM lake.silver.contract_performance_revision WHERE source_date = DATE '2025-10-17'"
        in sql
    )
    assert any("revision_id IN" in statement for statement in sql)
    db.reset_mock()
    stage_partition(
        db,
        catalog,
        "silver",
        {"contract_performance_revision": []},
        partition_date=date(2025, 10, 17),
    )
    sql = [call.args[0] for call in db.execute.call_args_list]
    assert (
        "DELETE FROM lake.silver.contract_performance_revision WHERE source_date = DATE '2025-10-17'"
        in sql
    )
    assert not any("INSERT INTO" in statement for statement in sql)


@pytest.mark.parametrize(
    "options,iceberg", [({"family": "contract"}, None), ({"skip_existing": True}, object())]
)
def test_reject_partial_or_unverified_publication_before_storage_writes(options, iceberg):
    from unittest.mock import MagicMock

    from procurement.silver.application.process_silver_service import ProcessSilverService

    bronze, storage = MagicMock(), MagicMock()
    service = ProcessSilverService(bronze, storage, "bucket", "contract_performance", iceberg)
    with pytest.raises(ValueError):
        service.execute(date(2025, 10, 17), date(2025, 10, 17), **options)
    bronze.select_committed_partitions.assert_not_called()
    storage.write_candidate_tables.assert_not_called()


def test_refresh_removing_one_report_keeps_contract_referenced_by_another_day():
    from procurement.silver.application.warehouse import stage_partition

    class LocalIceberg:
        def __init__(self):
            self.db = duckdb.connect()
            self.db.execute("ATTACH ':memory:' AS lake")

        def execute(self, sql):
            # Exercise the real SQL mutations, replacing only Iceberg-specific DDL syntax.
            if sql.startswith("CREATE TABLE"):
                sql = sql.split(" PARTITIONED BY ")[0]
            return self.db.execute(sql)

        def __getattr__(self, name):
            return getattr(self.db, name)

    class LocalCatalog:
        def table(self, namespace, name):
            count = db.sql(f"SELECT count(*) FROM lake.{namespace}.{name}").fetchone()[0]
            return {
                "metadata": {
                    "format-version": 2,
                    "current-snapshot-id": 1,
                    "snapshots": [{"snapshot-id": 1, "summary": {"total-records": str(count)}}],
                }
            }

    db = LocalIceberg()
    catalog = LocalCatalog()
    try:
        for number, day in [(1, date(2025, 10, 17)), (2, date(2025, 10, 18))]:
            common = {
                "source_date": day,
                "observed_at": f"{day}T00:00:00+00:00",
                "root_json": "{}",
                "contract_code": "HD001",
                "contract_version": "00",
            }
            contract = {
                **common,
                "revision_id": "contract-revision",
                "entity_id": "contract",
                "entity_type": "contract",
            }
            performance = {
                **common,
                "revision_id": f"report-{number}",
                "entity_id": f"performance-{number}",
                "entity_type": "contract_performance",
            }
            stage_partition(
                db,
                catalog,
                "silver",
                {
                    "contract_revision": typed_arrow_rows("contract_revision", [contract]),
                    "contract_performance_revision": typed_arrow_rows(
                        "contract_performance_revision", [performance]
                    ),
                },
                partition_date=day,
            )
        stage_partition(
            db,
            catalog,
            "silver",
            {
                "contract_revision": [],
                "contract_performance_revision": [],
            },
            partition_date=date(2025, 10, 18),
        )
        assert db.sql(
            "SELECT revision_id FROM lake.silver.contract_performance_revision"
        ).fetchall() == [("report-1",)]
        assert (
            db.sql("""SELECT count(*) FROM lake.silver.contract_performance_revision p
            JOIN lake.silver.contract_revision c
            ON p.contract_code=c.contract_code AND p.contract_version=c.contract_version""").fetchone()[
                0
            ]
            == 1
        )
    finally:
        db.close()
