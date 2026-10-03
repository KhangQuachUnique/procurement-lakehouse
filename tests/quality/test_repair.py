"""Replacement concurrency preserves the plan and reuses validated checkpoints."""

import copy
from datetime import UTC, date, datetime
from threading import Barrier

from procurement.ingestion.engine.metadata import calculate_content_hash
from procurement.quality.contracts import TABLES, Route, load_config
from procurement.quality.repair import reconstruct


def test_parallel_replacements_only_fetch_targets_and_resume_from_cache(tmp_path):
    config = load_config()
    contexts = [{"id": key, "notifyNo": f"IB-{key}", "notifyVersion": "00"}
                for key in ("keep", "first", "second")]
    config.routes = [Route(name=ctx["id"], contract="standard", evidence="test", match=ctx)
                     for ctx in contexts]
    baseline, references = [], []
    timestamp = datetime(2025, 1, 3, tzinfo=UTC)
    for index, ctx in enumerate(contexts):
        payload = {"bidoNotifyContractorM": ctx} if index == 0 else {}
        record = {"source_id": ctx["notifyNo"], "source_version": "00",
                  "source_date": date(2025, 1, 2), "run_id": "baseline",
                  "ingested_at": timestamp, "payload": payload,
                  "content_hash": calculate_content_hash(payload)}
        baseline.append((TABLES["standard"], record))
        references.append({"source_id": record["source_id"], "source_version": "00",
                           "content_hash": record["content_hash"],
                           "context": ctx, "refetch": index > 0,
                           "route": config.routes[index].model_dump()})
    original = copy.deepcopy(references)
    barrier = Barrier(2)
    calls = []

    class Client:
        def post(self, endpoint, body):
            calls.append(body["id"])
            barrier.wait(timeout=10)
            return {"bidoNotifyContractorM": next(ctx for ctx in contexts if ctx["id"] == body["id"])}

    for run_id in ("first-attempt", "resumed-attempt"):
        tables, observations, counts = reconstruct(
            Client(), baseline, references, config, run_id, tmp_path,
            detail_workers=2,
        )
        assert counts == {"copied": 1, "refetched": 2, "moved": 0}
        records = tables[TABLES["standard"]]
        assert records[0].payload == baseline[0][1]["payload"]
        assert records[0].ingested_at == timestamp
        assert all(record.run_id == run_id for record in records)
        assert [item["action"] for item in observations] == ["copy", "refetch", "refetch"]
    assert sorted(calls) == ["first", "second"]
    assert references == original
