"""Concrete local processing, with injected source/publisher IO boundaries."""

from .aggregation import accept_candidate, prepare_candidate
from .calculation import calculate
from .contracts import INPUT_TABLES, RunPlan
from .publication import prepare_outbox
from .relationship_index import accept_index
from .source import freeze
from .source_cache import accept_cache


class PipelineBackend:
    def __init__(self, reader, publisher, on_batch=None):
        self.reader, self.publisher, self.on_batch = reader, publisher, on_batch

    def freeze(self, db, plan):
        return freeze(db, plan, self.reader)

    def calculate(self, db, plan):
        db.execute("SET memory_limit=?", [plan.budget.memory_limit])
        db.execute("SET threads=?", [plan.budget.threads])
        return calculate(db, plan, self.on_batch)

    def prepare(self, db, plan):
        evidence = prepare_candidate(
            db,
            plan.run_id,
            contributions="changes",
            affected="affected",
            dimensions="dimensions",
            observation_day=plan.observation_day,
        )

        return evidence | {"outbox": prepare_outbox(db, plan, "candidate_" + plan.run_id)}

    def publish_verified(self, db, plan):
        return self.publisher.publish_verified(db, plan, "candidate_" + plan.run_id)

    def activate(self, db, plan):
        return self.publisher.activate(plan)

    def accept(self, db, plan):
        # Revalidate the acknowledged remote pointer before accepting local cache.
        if not self.publisher.is_active(plan):
            raise ValueError("Remote active generation differs; refusing local acceptance")
        accept_candidate(db, plan.run_id)
        accept_cache(db, plan)
        for name in INPUT_TABLES:
            db.execute(f"DELETE FROM input_cache.{name}")
            db.execute(f"INSERT INTO input_cache.{name} SELECT * FROM frozen.{name}")
        accept_index(db, plan.run_id)

    def equivalent(self, db, plan):
        previous = db.execute(
            "SELECT release_id FROM control.accepted WHERE target=?", [plan.target_namespace]
        ).fetchone()
        if previous is None:
            return None
        published = RunPlan.model_validate_json(
            db.execute("SELECT plan FROM control.runs WHERE run_id=?", [previous[0]]).fetchone()[0]
        )
        if plan.observation_day != published.observation_day:
            return None
        for name in INPUT_TABLES:
            missing = db.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_catalog=current_database() AND table_schema='input_cache' AND table_name=?",
                [name],
            ).fetchone()[0]
            if not missing:
                return None
            changed = db.execute(
                f"SELECT EXISTS(SELECT 1 FROM ((SELECT * FROM frozen.{name} EXCEPT ALL SELECT * FROM input_cache.{name}) UNION ALL (SELECT * FROM input_cache.{name} EXCEPT ALL SELECT * FROM frozen.{name})) LIMIT 1)"
            ).fetchone()[0]
            if changed:
                return None
        if not self.publisher.is_active(published):
            raise ValueError("Equivalent-source acknowledgement refused: remote pointer differs")
        accept_cache(db, plan)
        return {
            "reused": True,
            "release_id": previous[0],
            "new_source_input_id": plan.source.input_id,
            "source_checkpoint_advanced": True,
        }
