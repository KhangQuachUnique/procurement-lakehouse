from datetime import UTC, date, datetime
from fnmatch import fnmatch

import duckdb

from procurement.models.control import DayManifest, RunManifest
from procurement.tools import bronze_explorer


class _FakeFilesystem:
    def __init__(self, entries_by_path):
        self.entries_by_path = entries_by_path

    def ls(self, path: str, detail: bool = True):
        assert detail is True
        if path not in self.entries_by_path:
            raise FileNotFoundError(path)
        return self.entries_by_path[path]


class _FakeConnection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: str):
        self.statements.append(statement)
        return self


def test_parse_endpoint_supports_http_and_https() -> None:
    assert bronze_explorer._parse_endpoint("http://localhost:8333") == (
        "localhost:8333",
        False,
    )
    assert bronze_explorer._parse_endpoint("https://storage.example.com") == (
        "storage.example.com",
        True,
    )


def test_parse_endpoint_rejects_paths() -> None:
    try:
        bronze_explorer._parse_endpoint("http://localhost:8333/s3")
    except ValueError as exc:
        assert "must not contain a path" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_discover_bronze_tables_under_dlt_dataset() -> None:
    fs = _FakeFilesystem(
        {
            "bucket/bronze": [
                {"name": "bucket/bronze/muasamcong", "type": "directory"},
            ],
            "bucket/bronze/muasamcong": [
                {
                    "name": "bucket/bronze/muasamcong/project_detail",
                    "type": "directory",
                },
                {
                    "name": (
                        "bucket/bronze/muasamcong/"
                        "notify_contractor_standard_detail"
                    ),
                    "type": "directory",
                },
                {
                    "name": "bucket/bronze/muasamcong/_dlt_loads",
                    "type": "directory",
                },
            ],
        }
    )

    assert bronze_explorer._discover_bronze_tables(fs, "bucket") == [
        bronze_explorer.BronzeTable(
            dataset="muasamcong",
            table="notify_contractor_standard_detail",
        ),
        bronze_explorer.BronzeTable(dataset="muasamcong", table="project_detail"),
    ]


def test_discover_bronze_tables_supports_direct_partitioned_layout() -> None:
    fs = _FakeFilesystem(
        {
            "bucket/bronze": [
                {"name": "bucket/bronze/project_detail", "type": "directory"},
            ],
            "bucket/bronze/project_detail": [
                {
                    "name": "bucket/bronze/project_detail/source_date=2025-01-01",
                    "type": "directory",
                },
            ],
        }
    )

    assert bronze_explorer._discover_bronze_tables(fs, "bucket") == [
        bronze_explorer.BronzeTable(dataset=None, table="project_detail")
    ]


def test_create_bronze_views_uses_dataset_table_path() -> None:
    con = _FakeConnection()

    created = bronze_explorer._create_bronze_views(
        con,
        bucket="procurement-lakehouse",
        tables=[
            bronze_explorer.BronzeTable(
                dataset="muasamcong",
                table="notify_contractor_standard_detail",
            )
        ],
    )

    assert created == ["notify_contractor_standard_detail"]
    sql = "\n".join(con.statements)
    assert '"bronze_raw"."notify_contractor_standard_detail"' in sql
    assert (
        "s3://procurement-lakehouse/bronze/muasamcong/"
        "notify_contractor_standard_detail/**/*.parquet"
    ) in sql
    assert "hive_partitioning = true" in sql
    assert "union_by_name = false" in sql
    assert "filename = true" in sql


def test_create_bronze_views_disambiguates_duplicate_table_names() -> None:
    con = _FakeConnection()

    created = bronze_explorer._create_bronze_views(
        con,
        bucket="procurement-lakehouse",
        tables=[
            bronze_explorer.BronzeTable(dataset="source_a", table="detail"),
            bronze_explorer.BronzeTable(dataset="source_b", table="detail"),
        ],
    )

    assert created == ["source_a__detail", "source_b__detail"]


def test_current_views_select_one_run_for_all_detail_tables(tmp_path, monkeypatch):
    bucket = "bucket"
    prefix = f"{bucket}/_control/muasamcong/notify_contractor"
    tables = [bronze_explorer.BronzeTable("muasamcong", f"notify_contractor_{kind}_detail")
              for kind in ("standard", "reoffer", "vk_adb")]
    entries = {prefix: []}
    manifests, runs, parquet = {}, {}, {}
    writer = duckdb.connect()
    # The earlier attempt completes later and has a lexically larger id. Neither wins.
    for run, started, status, table_index in (("z-old", 1, "success", 0),
                                             ("a-new", 2, "success", 1),
                                             ("failed", 3, "failed", 0)):
        entries[prefix].append({"name": f"{prefix}/run_id={run}", "type": "directory"})
        run_prefix = f"{prefix}/run_id={run}"
        entries[run_prefix] = [{"name": f"{run_prefix}/source_date=2024-12-24", "type": "directory"}]
        timestamp = datetime(2025, 1, started, tzinfo=UTC)
        runs[run] = RunManifest(run_id=run, source="muasamcong", resource="notify_contractor",
                                start_date=date(2024, 12, 24), end_date=date(2024, 12, 24),
                                status="partial_failed", total_dates=1, started_at=timestamp)
        day = DayManifest(run_id=run, source="muasamcong", resource="notify_contractor",
                          source_date=date(2024, 12, 24), status=status, bronze_records=1,
                          started_at=timestamp, completed_at=datetime(2025, 2, 5-started, tzinfo=UTC))
        manifests[f"{run_prefix}/source_date=2024-12-24/day.json"] = day.model_dump(mode="json")
        table = tables[table_index]
        key = f"{bucket}/bronze/muasamcong/{table.table}/source_date=2024-12-24/run_id={run}/part.parquet"
        local = tmp_path / f"{run}.parquet"
        writer.execute(f"""COPY (SELECT 'IB1' AS source_id, '00' AS source_version,
            '{run}' AS run_id, DATE '2024-12-24' AS source_date,
            TIMESTAMPTZ '2025-01-01' AS ingested_at, 'hash' AS content_hash, '{{}}' AS payload)
            TO '{local.as_posix()}' (FORMAT parquet)""")
        parquet[key] = local.as_posix()
    writer.close()
    fs = _FakeFilesystem(entries)
    fs.glob = lambda pattern: [key for key in parquet if fnmatch(key, pattern)]
    monkeypatch.setattr(bronze_explorer, "read_run_manifest", lambda fs, identity, rid: runs[rid])
    monkeypatch.setattr(bronze_explorer, "read_json", lambda fs, key: manifests.get(key))
    selected = bronze_explorer._select_current_files(fs, bucket=bucket, tables=tables, year=2024)
    assert selected[tables[0]] == selected[tables[2]] == []
    assert len(selected[tables[1]]) == 1
    assert "run_id=a-new" in selected[tables[1]][0]
    assert bronze_explorer._select_current_files(
        fs, bucket=bucket, tables=tables, start=date(2022, 1, 1), end=date(2025, 12, 31),
    ) == selected
    assert all(not paths for paths in bronze_explorer._select_current_files(
        fs, bucket=bucket, tables=tables, start=date(2022, 1, 1), end=date(2023, 12, 31),
    ).values())
    local_files = {t: [parquet[key.removeprefix("s3://")] for key in keys] for t, keys in selected.items()}
    with duckdb.connect() as con:
        created = bronze_explorer._create_bronze_views(con, bucket=bucket, tables=tables,
                                                       files_by_table=local_files)
        bronze_explorer._create_notify_view(con, created)
        assert con.execute("SELECT source_id,run_id FROM bronze_raw.notify_contractor").fetchall() == [("IB1", "a-new")]
        assert con.execute("SELECT count(*) FROM bronze_raw.notify_contractor_standard_detail").fetchone() == (0,)
    # A newer successful empty day clears every table, even if historical files exist.
    new_manifest = manifests[f"{prefix}/run_id=a-new/source_date=2024-12-24/day.json"]
    new_manifest["bronze_records"] = 0
    assert all(not paths for paths in bronze_explorer._select_current_files(
        fs, bucket=bucket, tables=tables, year=2024,
    ).values())
    new_manifest["bronze_records"] = 1
    # Missing files for a selected nonempty day must never resurrect the old attempt.
    parquet.pop(next(key for key in parquet if "run_id=a-new" in key))
    issues = []
    missing = bronze_explorer._select_current_files(fs, bucket=bucket, tables=tables,
                                                    year=2024, issues=issues)
    assert all(not paths for paths in missing.values())
    assert issues == [{"resource": "notify_contractor", "source_date": "2024-12-24",
                       "run_id": "a-new", "expected_records": 1, "issue": "committed_files_missing"}]
    with duckdb.connect() as con:
        bronze_explorer._create_selection_issues(con, issues)
        assert con.execute("SELECT resource, expected_records FROM bronze_meta.selection_issues").fetchall() == [
            ("notify_contractor", 1),
        ]
        created = bronze_explorer._create_bronze_views(con, bucket=bucket, tables=tables,
                                                       files_by_table=missing)
        bronze_explorer._create_notify_view(con, created)
        assert con.execute("SELECT count(*) FROM bronze_raw.notify_contractor").fetchone() == (0,)


def test_explicit_files_do_not_bind_a_recursive_glob():
    table = bronze_explorer.BronzeTable("muasamcong", "project_detail")
    con = _FakeConnection()
    bronze_explorer._create_bronze_views(con, bucket="bucket", tables=[table],
                                        files_by_table={table: ["s3://bucket/selected.parquet"]})
    sql = "\n".join(con.statements)
    assert "**" not in sql
    assert "selected.parquet" in sql
    assert "union_by_name = false" in sql


def test_extensions_already_installed_do_not_trigger_install():
    class Connection:
        def load_extension(self, name):
            assert name == "httpfs"

        def install_extension(self, name):
            raise AssertionError("Should not download installed extension")

    bronze_explorer._load_extension(Connection(), "httpfs")
