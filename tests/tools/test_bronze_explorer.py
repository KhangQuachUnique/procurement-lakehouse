from procurement.tools import bronze_explorer


class _FakeFilesystem:
    def __init__(self, entries):
        self.entries = entries

    def ls(self, path: str, detail: bool = True):
        assert path == "bucket/bronze"
        assert detail is True
        return self.entries


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


def test_discover_bronze_tables_only_returns_directories() -> None:
    fs = _FakeFilesystem(
        [
            {"name": "bucket/bronze/project_detail", "type": "directory"},
            {
                "name": "bucket/bronze/notify_contractor_standard_detail",
                "type": "directory",
            },
            {"name": "bucket/bronze/_temporary.txt", "type": "file"},
        ]
    )

    assert bronze_explorer._discover_bronze_tables(fs, "bucket") == [
        "notify_contractor_standard_detail",
        "project_detail",
    ]


def test_create_bronze_views_uses_hive_partitioning_and_union_by_name() -> None:
    con = _FakeConnection()

    created = bronze_explorer._create_bronze_views(
        con,
        bucket="procurement-lakehouse",
        tables=["notify_contractor_standard_detail"],
    )

    assert created == ["notify_contractor_standard_detail"]
    sql = "\n".join(con.statements)
    assert '"bronze_raw"."notify_contractor_standard_detail"' in sql
    assert (
        "s3://procurement-lakehouse/bronze/"
        "notify_contractor_standard_detail/**/*.parquet"
    ) in sql
    assert "hive_partitioning = true" in sql
    assert "union_by_name = true" in sql
    assert "filename = true" in sql
