"""SQLite state store for late bid opening watch."""
import sqlite3
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from procurement.common.dates import today_vn
from procurement.common.settings import settings
from procurement.ingestion.engine.metadata import calculate_content_hash


def namespace() -> str:
    """Namespace hash derived from current object storage endpoint and bucket."""
    return calculate_content_hash([settings.OBJECT_STORAGE_ENDPOINT, settings.OBJECT_STORAGE_BUCKET])


class WatchStore:
    """Namespace-bound SQLite store tracking notices and due checks."""

    def __init__(self, path: Path | str, *, dry_run: bool = False, read_only: bool = False) -> None:
        path = Path(path)
        expected_ns = self._get_namespace()
        if read_only and path.exists():
            self.db = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
            self.db.row_factory = sqlite3.Row
            existing = self.db.execute("SELECT value FROM metadata WHERE name='namespace'").fetchone()
            if not existing or existing[0] != expected_ns:
                self.db.close()
                raise ValueError("Watch storage namespace changed; use a different watch database")
            return
        if dry_run or read_only:
            self.db = sqlite3.connect(":memory:")
            if path.exists():
                with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True) as source:
                    source.backup(self.db)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS metadata (name TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS seeds (
                day TEXT PRIMARY KEY, run_id TEXT NOT NULL, opening_run TEXT);
            CREATE TABLE IF NOT EXISTS gaps (day TEXT PRIMARY KEY, reason TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS notices (
                key TEXT PRIMARY KEY, day TEXT NOT NULL, context TEXT NOT NULL,
                status TEXT NOT NULL, next_check TEXT, first_checked TEXT, error TEXT);
            CREATE INDEX IF NOT EXISTS due_notices ON notices(status, next_check, day);
        """)
        existing = self.db.execute("SELECT value FROM metadata WHERE name='namespace'").fetchone()
        if existing and existing[0] != expected_ns:
            self.db.close()
            raise ValueError("Watch storage namespace changed; use a different watch database")
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO metadata VALUES ('namespace', ?)", (expected_ns,))
            version = self.db.execute("SELECT value FROM metadata WHERE name='phase_rules'").fetchone()
            if not version or version[0] != "2":
                # Re-evaluate captured states under the new phase contract on the next seed.
                self.db.execute("DELETE FROM seeds")
                self.db.execute("INSERT OR REPLACE INTO metadata VALUES ('phase_rules', '2')")

    def _get_namespace(self) -> str:
        for mod_name in ("procurement.watcher.service", "procurement.ingestion.bid_opening_watch", "procurement.watcher"):
            mod = sys.modules.get(mod_name)
            if mod and "namespace" in mod.__dict__:
                return mod.__dict__["namespace"]()
        return namespace()

    def close(self) -> None:
        self.db.close()

    def due_days(
        self,
        *,
        now: datetime | None = None,
        start: date | None = None,
        end: date | None = None,
        limit: int = 31,
    ) -> list[dict[str, Any]]:
        """Read due closed dates without changing watch state."""
        if limit < 1:
            raise ValueError("limit must be positive")
        now = now or datetime.now(UTC)
        end = min(end or date.max, today_vn() - timedelta(days=1))
        return [
            dict(row)
            for row in self.db.execute(
                """
            SELECT day,min(next_check) AS due FROM notices
            WHERE status='pending' AND next_check<=? AND day>=? AND day<=?
            GROUP BY day ORDER BY due,day LIMIT ?
        """,
                (now.isoformat(), str(start or date.min), str(end), limit),
            ).fetchall()
        ]

    def status(
        self,
        now: datetime | None = None,
        *,
        start: date | None = None,
        end: date | None = None,
    ) -> dict[str, Any]:
        """Summarize current state of tracked notices, errors, and gaps."""
        now = now or datetime.now(UTC)
        bounds = (str(start or date.min), str(end or date.max))
        result: dict[str, Any] = {key: 0 for key in ("pending", "captured", "unresolved")}
        result.update(
            dict(self.db.execute("SELECT status, count(*) FROM notices WHERE day BETWEEN ? AND ? GROUP BY status", bounds))
        )
        result["errors"] = self.db.execute(
            "SELECT count(*) FROM notices WHERE error IS NOT NULL AND day BETWEEN ? AND ?", bounds
        ).fetchone()[0]
        result["coverage_gaps"] = self.db.execute(
            "SELECT count(*) FROM gaps WHERE day BETWEEN ? AND ?", bounds
        ).fetchone()[0]
        result["due_days"] = self.db.execute(
            "SELECT count(DISTINCT day) FROM notices WHERE status='pending' AND next_check<=? AND day BETWEEN ? AND ?",
            (now.isoformat(), *bounds),
        ).fetchone()[0]
        return result
