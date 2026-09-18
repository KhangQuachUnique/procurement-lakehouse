"""Queries over a request's SQLite snapshot; only page drill-down reads object storage."""

import json

from procurement.models.control import DayManifest, RunManifest
from procurement.models.errors import ErrorRecord
from procurement.storage.control import list_page_manifests


class IndexedControlRepository:
    def __init__(self, db, fs):
        self.db, self.fs = db, fs

    def _list(self, kind, identity, *, limit=None, **filters):
        clauses, args = ["kind=?", "source=?", "resource=?"], [
            kind, identity.source, identity.resource,
        ]
        for name, value in filters.items():
            if value is None:
                continue
            if name == "start_date":
                clauses.append("end_date>=?" if kind == "run" else "source_date>=?")
            elif name == "end_date":
                clauses.append("start_date<=?" if kind == "run" else "source_date<=?")
            elif name in {"run_id", "source_date", "status"}:
                clauses.append(f"{name}=?")
            else:
                raise ValueError(f"Unsupported index filter: {name}")
            args.append(str(value))
        query = "SELECT payload FROM objects WHERE " + " AND ".join(clauses)
        query += " ORDER BY started_at DESC, run_id DESC"
        if limit is not None:
            query += " LIMIT ?"
            args.append(limit)
        model = RunManifest if kind == "run" else DayManifest
        return [model.model_validate_json(row[0]) for row in self.db.execute(query, args)]

    def list_runs(self, identity, **kwargs):
        return self._list("run", identity, **kwargs)

    def get_run(self, identity, run_id):
        items = self.list_runs(identity, run_id=run_id, limit=1)
        return items[0] if items else None

    def list_attempts(self, identity, **kwargs):
        return self._list("day", identity, **kwargs)

    def get_attempt(self, identity, *, run_id, source_date):
        items = self.list_attempts(identity, run_id=run_id, source_date=source_date, limit=1)
        return items[0] if items else None

    def list_pages(self, identity, *, run_id, source_date):
        return list_page_manifests(self.fs, identity, run_id=run_id, source_date=source_date)

    def get_execution(self, identity, run_id):
        row = self.db.execute(
            "SELECT payload FROM objects WHERE kind='execution' AND source=? AND resource=? "
            "AND run_id=?", (identity.source, identity.resource, run_id),
        ).fetchone()
        return json.loads(row[0]) if row else None


class IndexedErrorRepository:
    def __init__(self, db):
        self.db = db

    def list(self, identity, *, limit=200, **filters):
        clauses, args = ["source=?", "resource=?"], [identity.source, identity.resource]
        for name, value in filters.items():
            if name not in {"source_date", "run_id", "stage", "error_type"}:
                raise ValueError(f"Unsupported error filter: {name}")
            if value is not None:
                clauses.append(f"{name}=?")
                args.append(str(value))
        args.append(limit)
        query = "SELECT payload FROM errors WHERE " + " AND ".join(clauses)
        query += " ORDER BY occurred_at DESC, object_key, ordinal LIMIT ?"
        return [ErrorRecord.model_validate_json(row[0]) for row in self.db.execute(query, args)]
