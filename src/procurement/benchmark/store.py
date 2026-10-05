import json
import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from procurement.benchmark.models import BenchmarkConfig, StageConfig

ACTIVE = ("queued", "running", "stopping")


def now():
    return datetime.now(UTC).isoformat()


def write_json(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


class BenchmarkStore:
    def __init__(self, state_dir="data/benchmarks", export_dir="exports/benchmarks"):
        self.state_dir = Path(state_dir).resolve()
        self.export_dir = Path(export_dir).resolve()
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.export_dir.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                    status TEXT NOT NULL, config TEXT NOT NULL, summary TEXT NOT NULL DEFAULT '{}',
                    command TEXT NOT NULL DEFAULT '{}', revision INTEGER NOT NULL DEFAULT 0
                );
                CREATE UNIQUE INDEX IF NOT EXISTS one_active_benchmark ON runs((1))
                    WHERE status IN ('queued', 'running', 'stopping');
            """)

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.state_dir / "index.sqlite3", timeout=10)
        db.row_factory = sqlite3.Row
        try:
            yield db
        finally:
            db.close()

    def folder(self, run_id):
        # IDs used in paths can only be generated UUID hex strings.
        if len(run_id) != 32 or any(c not in "0123456789abcdef" for c in run_id):
            raise KeyError(run_id)
        return self.export_dir / run_id

    def create(self, config: BenchmarkConfig):
        run_id = uuid4().hex
        with self.connect() as db:
            try:
                db.execute("INSERT INTO runs(id,created_at,updated_at,status,config) VALUES(?,?,?,?,?)",
                           (run_id, now(), now(), "queued", config.model_dump_json()))
                db.commit()
            except sqlite3.IntegrityError as exc:
                raise ValueError("Another benchmark is active; stop it before starting a new run") from exc
        folder = self.folder(run_id)
        try:
            folder.mkdir()
            write_json(folder / "config.json", config.model_dump(mode="json"))
        except Exception:
            self.update(run_id, status="failed", summary={"reason": "artifact_creation_failed"})
            raise
        return run_id

    @staticmethod
    def decode(row):
        result = dict(row)
        for name in ("config", "summary", "command"):
            result[name] = json.loads(result[name])
        return result

    def get(self, run_id):
        self.folder(run_id)
        with self.connect() as db:
            row = db.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
        if row is None:
            raise KeyError(run_id)
        return self.decode(row)

    def list(self):
        with self.connect() as db:
            return [self.decode(row) for row in db.execute(
                "SELECT * FROM runs ORDER BY created_at DESC LIMIT 100")]

    def update(self, run_id, *, status, summary):
        with self.connect() as db:
            # A worker heartbeat must never erase a pending stop command/status.
            db.execute("""UPDATE runs SET status=CASE WHEN status='stopping' AND ?='running'
                       THEN status ELSE ? END, summary=?,updated_at=? WHERE id=?""",
                       (status, status, json.dumps(summary), now(), run_id))
            db.commit()

    def command(self, run_id, stage: StageConfig | None = None):
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
            if row is None:
                raise KeyError(run_id)
            run = self.decode(row)
            if run["status"] not in ACTIVE or run["status"] == "stopping":
                raise ValueError("Run is finished or already stopping")
            if stage and run["config"]["mode"] != "manual":
                raise ValueError("Only manual runs accept stage changes")
            command = {"stage": stage.model_dump()} if stage else {"stop": True}
            db.execute("UPDATE runs SET command=?,revision=revision+1,status=? WHERE id=?",
                       (json.dumps(command), run["status"] if stage else "stopping", run_id))
            db.commit()
