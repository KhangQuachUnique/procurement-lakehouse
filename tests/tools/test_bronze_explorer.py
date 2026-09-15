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
    assert "union_by_name = true" in sql
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
