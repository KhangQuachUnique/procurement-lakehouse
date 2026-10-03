"""Browser requests establish routes; synthetic responses exercise validation only."""

import json
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from procurement.common.resources import ResourceIdentity
from procurement.ingestion.engine.metadata import calculate_content_hash
from procurement.ingestion.engine.stats import PageStats
from procurement.ingestion.sources.muasamcong.notify_contractor.extractor import (
    iter_notify_contractor_records,
)
from procurement.quality.contracts import (
    ENDPOINTS,
    TABLES,
    DetailValidationError,
    load_config,
    resolve_route,
)
from procurement.quality.repair import fetch_replacement, reconstruct

FIXTURES = Path(__file__).parent / "fixtures"
SAMPLES = json.loads((FIXTURES / "browser_routing_2022.json").read_text(encoding="utf-8"))["samples"]
REOFFER = json.loads((FIXTURES / "verified_reoffer.json").read_text(encoding="utf-8"))
SAMPLES.append({"context": REOFFER["context"], "contract": "reoffer",
                "endpoint": ENDPOINTS["reoffer"]})


@pytest.mark.parametrize("sample", SAMPLES, ids=lambda s: s["context"]["notifyNo"])
@pytest.mark.parametrize("year", [2022, 2023, 2024, 2025])
def test_crawl_and_repair_share_routes_across_years(sample, year, tmp_path):
    config = load_config()
    ctx = {**sample["context"], "notifyNo": f"IB{year % 100:02d}00000001",
           "publicDate": f"{year}-10-02T12:00:00"}
    kind = sample["contract"]
    route = resolve_route(ctx, config)
    assert route.contract == kind
    root = {k: ctx[k] for k in ("id", "notifyNo", "notifyVersion")}
    payload = root if kind == "reoffer" else {
        "bidoNotifyContractorM" if kind == "standard" else "bidoNotifyContractorP": root,
    }
    calls = []

    def post(endpoint, body):
        calls.append((endpoint, body))
        return payload

    api = SimpleNamespace(post=post)
    setattr(api, f"get_{kind}_detail", lambda notice_id: post(sample["endpoint"], {"id": notice_id}))
    errors = []
    records = list(iter_notify_contractor_records(
        api, identity=ResourceIdentity("muasamcong", "notify_contractor"), search_items=[ctx],
        run_id="crawl", source_date=date(year, 10, 2), search_page=0,
        errors=errors, stats=PageStats(), config=config,
    ))
    assert not errors
    assert records[0].table == TABLES[kind]
    baseline = {"source_id": ctx["notifyNo"], "source_version": ctx["notifyVersion"],
                "source_date": date(year, 10, 2), "run_id": "old",
                "ingested_at": datetime(year, 10, 3, tzinfo=UTC),
                "payload": {}, "content_hash": calculate_content_hash({})}
    reference = {**{k: baseline[k] for k in ("source_id", "source_version", "content_hash")},
                 "table": TABLES["standard"], "context": ctx,
                 "route": route.model_dump(), "refetch": True}
    tables, _, counts = reconstruct(api, [(TABLES["standard"], baseline)], [reference],
                                    config, "repair", tmp_path)
    assert set(tables) == {TABLES[kind]}
    assert tables[TABLES[kind]][0].payload == records[0].record.payload == payload
    assert counts == {"copied": 0, "refetched": 1, "moved": int(kind != "standard")}
    assert calls == [(sample["endpoint"], {"id": ctx["id"]})] * 2


@pytest.mark.parametrize("field,value", [("processApply", "unknown"), ("bidForm", None),
                                        ("bidForm", ""), ("bidForm", 1)])
def test_family_rules_reject_missing_or_unknown_routing_input(field, value):
    with pytest.raises(DetailValidationError, match="unresolved_route"):
        resolve_route({**SAMPLES[0]["context"], field: value}, load_config())


@pytest.mark.parametrize("sample", [SAMPLES[0], SAMPLES[3], SAMPLES[-1]])
def test_known_route_does_not_accept_empty_detail(sample, tmp_path):
    config = load_config()
    ctx = sample["context"]
    api = SimpleNamespace(post=lambda *_: {})
    kind = sample["contract"]
    setattr(api, f"get_{kind}_detail", lambda _: {})
    errors = []
    assert not list(iter_notify_contractor_records(
        api, identity=ResourceIdentity("muasamcong", "notify_contractor"), search_items=[ctx],
        run_id="crawl", source_date=date(2022, 10, 2), search_page=0,
        errors=errors, stats=PageStats(), config=config,
    ))
    assert errors[0].stage == "detail_validation"
    with pytest.raises(ValueError):
        fetch_replacement(api, {"context": ctx, "source_id": ctx["notifyNo"],
                                "source_version": ctx["notifyVersion"]}, config, tmp_path)
