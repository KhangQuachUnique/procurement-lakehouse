"""DuckDB connection to the SeaweedFS Iceberg REST catalog (format v2)."""

import re
from dataclasses import dataclass
from urllib.parse import urlsplit

import duckdb
import httpx


def literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


@dataclass(frozen=True)
class IcebergConfig:
    catalog_uri: str
    warehouse: str
    s3_endpoint: str
    access_key: str
    secret_key: str


def identifier(value: str) -> str:
    if not re.fullmatch(r"[a-z][a-z0-9_]*", value):
        raise ValueError("Invalid catalog identifier")
    return value


class RestCatalog:
    """Metadata operations absent from DuckDB SQL; credentials never enter artifacts."""

    def __init__(self, config: IcebergConfig):
        self.config = config

    def request(self, method, path, **kwargs):
        with httpx.Client(base_url=self.config.catalog_uri, timeout=60) as client:
            response = client.post("/v1/oauth/tokens", data={
                "grant_type": "client_credentials", "client_id": self.config.access_key,
                "client_secret": self.config.secret_key,
            })
            response.raise_for_status()
            client.headers["Authorization"] = "Bearer " + response.json()["access_token"]
            configuration = client.get("/v1/config", params={"warehouse": self.config.warehouse})
            configuration.raise_for_status()
            prefix = configuration.json().get("overrides", {}).get("prefix", "")
            response = client.request(method, f"/v1/{prefix}/{path}", **kwargs)
            response.raise_for_status()
            return response.json() if response.content else None

    def table(self, namespace, table):
        return self.request("GET", f"namespaces/{identifier(namespace)}/tables/{identifier(table)}")

    def pin(self, namespace, table, snapshot_id, release_id):
        name = "release_" + identifier(release_id)
        metadata = self.table(namespace, table)["metadata"]
        if snapshot_id not in {s["snapshot-id"] for s in metadata["snapshots"]}:
            raise ValueError("Release snapshot is missing")
        existing = metadata.get("refs", {}).get(name)
        if existing and existing["snapshot-id"] != snapshot_id:
            raise ValueError("Release snapshot reference is immutable")
        self.request("POST", f"namespaces/{identifier(namespace)}/tables/{identifier(table)}", json={
            "requirements": [{"type": "assert-ref-snapshot-id", "ref": name,
                              "snapshot-id": existing["snapshot-id"] if existing else None}],
            "updates": [{"action": "set-snapshot-ref", "ref-name": name,
                         "type": "tag", "snapshot-id": snapshot_id}],
        })


def connect(config: IcebergConfig, *, install=False):
    db = duckdb.connect()
    try:
        for extension in ("httpfs", "iceberg"):
            if install:
                db.execute(f"INSTALL {extension}")
            db.execute(f"LOAD {extension}")
        endpoint = urlsplit(config.s3_endpoint)
        if endpoint.scheme not in ("http", "https") or not endpoint.netloc:
            raise ValueError("S3 endpoint must be an HTTP(S) URL")
        db.execute(f"""CREATE SECRET lake_s3 (TYPE S3,
            KEY_ID {literal(config.access_key)}, SECRET {literal(config.secret_key)},
            ENDPOINT {literal(endpoint.netloc)}, URL_STYLE 'path', REGION 'us-east-1',
            USE_SSL {str(endpoint.scheme == 'https').lower()})""")
        db.execute(f"""CREATE SECRET lake_catalog (TYPE ICEBERG,
            CLIENT_ID {literal(config.access_key)}, CLIENT_SECRET {literal(config.secret_key)},
            OAUTH2_SERVER_URI {literal(config.catalog_uri.rstrip('/') + '/v1/oauth/tokens')})""")
        db.execute(f"""ATTACH {literal(config.warehouse)} AS lake (TYPE ICEBERG,
            ENDPOINT {literal(config.catalog_uri)}, SECRET lake_catalog, READ_ONLY false)""")
        return db
    except BaseException:
        db.close()
        raise
