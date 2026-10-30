from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from functools import partial
from typing import Any

from procurement.common.catalog import get_resource
from procurement.common.errors import build_error_record
from procurement.common.settings import settings
from procurement.ingestion.engine.metadata import utc_now
from procurement.ingestion.engine.models import ResourceSpec
from procurement.ingestion.engine.records import build_bronze_item
from procurement.ingestion.engine.stats import PageStats
from procurement.ingestion.sources.muasamcong.search import (
    build_search_payload,
    search_document_key,
)
from procurement.quality.contracts import (
    ENDPOINTS as NOTICE_DETAIL_ENDPOINTS,
    DetailValidationError,
    load_config,
    lookup,
    validate_detail,
)
from procurement.quality.opening_phases import (
    DUAL,
    ROUND_ROOT,
    financial_published,
    opening_mode,
)

SEARCH_PATH = "/o/egp-portal-contractor-selection-v2/services/smart/search"
BASE = "/o/egp-portal-contractor-selection-v2/services/"
STANDARD_NOTICE_PATH = NOTICE_DETAIL_ENDPOINTS["standard"]
ENDPOINTS = {
    "notify": BASE + "exposeldtkqmt/bid-notification-p/notify",
    "roundmng": BASE + "expose/ldtkqmt/bid-notification-p/roundmng",
    "bid_open": BASE + "expose/ldtkqmt/bid-notification-p/bid-open",
    "lot_open_detail": BASE + "expose/ldtkqmt/bid-notification-p/lotOpenDetail",
}

KHAC_OPENING_PATH = BASE + "expose/kqmt/bid-notify-contractor-out/get-by-id"

ADB_WB_OPENING_PATH = BASE + "expose/kqmt/" "bid-notify-contractor-out-adb-wb/get-by-id"


def request_body(context):
    notify_id = context.get("notifyId") or context.get("id")
    if not notify_id or not context.get("notifyNo"):
        raise ValueError("Missing bid opening request identity")
    if context.get("id") and context.get("notifyId") and context["id"] != notify_id:
        raise ValueError("Conflicting search UUIDs")
    return {
        "notifyId": notify_id,
        "notifyNo": context["notifyNo"],
        "type": "TBMT",
        "packType": 0,
    }


class BidOpeningApi:
    def __init__(self, client):
        self.client = client

    def search(self, *, page_number, page_size, window_from, window_to):
        return self.client.post(
            SEARCH_PATH,
            build_search_payload(
                page_number=page_number,
                page_size=page_size,
                index="es-contractor-selection",
                filters=[
                    {
                        "fieldName": "publicDate",
                        "searchType": "range",
                        "from": window_from,
                        "to": window_to,
                    },
                    {
                        "fieldName": "type",
                        "searchType": "in",
                        "fieldValues": ["es-notify-contractor"],
                    },
                    {
                        "fieldName": "stepCode",
                        "searchType": "in",
                        "fieldValues": [
                            "notify-contractor-step-2-kqmt",
                            "notify-contractor-step-3-dsntdkt",
                            "notify-contractor-step-4-kqlcnt",
                        ],
                    },
                    {
                        "fieldName": "publicDateKqmt",
                        "searchType": "not_null",
                        "fieldValues": [""],
                    },
                    {"fieldName": "isInternet", "searchType": "in", "fieldValues": [1]},
                ],
            ),
        )

    def fetch(self, context, evidence):
        body = request_body(context)

        if context.get("processApply") in ("ADB", "WB"):
            if context.get("bidMode") != "1_MTHS":
                raise DetailValidationError(
                    "unsupported_adb_opening_mode"
                    if context["processApply"] == "ADB"
                    else "unsupported_wb_opening_mode"
                )

            request = {
                "notifyNo": body["notifyNo"],
                "type": "TBMT",
                "packType": 0,
            }

            part = {
                "endpoint": ADB_WB_OPENING_PATH,
                "request": dict(request),
                "started_at": utc_now().isoformat(),
            }

            evidence.update(
                request=request,
                parts={"opening_adb_wb": part},
            )

            try:
                response = self.client.post(
                    ADB_WB_OPENING_PATH,
                    request,
                )
            except Exception as exc:
                part["status"] = "failed"
                exc.diagnostics = {
                    **getattr(exc, "diagnostics", {}),
                    "endpoint": ADB_WB_OPENING_PATH,
                    "request_id": body["notifyId"],
                    "part": "opening_adb_wb",
                }
                raise

            part.update(
                status="received",
                completed_at=utc_now().isoformat(),
            )

            return {"opening_adb_wb": response}

        if context.get("processApply") == "KHAC":
            mode = context.get("bidMode")
            notify_id = body["notifyId"]

            evidence.update(
                request={"id": notify_id},
                parts={},
            )

            payload = {}

            def receive_khac(part, request):
                part_evidence = {
                    "endpoint": KHAC_OPENING_PATH,
                    "request": dict(request),
                    "started_at": utc_now().isoformat(),
                }

                evidence["parts"][part] = part_evidence

                try:
                    payload[part] = self.client.post(
                        KHAC_OPENING_PATH,
                        request,
                    )

                except Exception as exc:
                    part_evidence["status"] = "failed"

                    exc.diagnostics = {
                        **getattr(exc, "diagnostics", {}),
                        "endpoint": KHAC_OPENING_PATH,
                        "request_id": notify_id,
                        "part": part,
                    }
                    raise

                part_evidence.update(
                    status="received",
                    completed_at=utc_now().isoformat(),
                )

            # One-envelope opening
            if mode == "1_MTHS":
                receive_khac(
                    "opening_out",
                    {"id": notify_id},
                )

            # Two-envelope opening
            elif mode == "1_HTHS":
                receive_khac(
                    "opening_out_technical",
                    {
                        "id": notify_id,
                        "packType": 1,
                    },
                )

                round_info = lookup(
                    payload,
                    "opening_out_technical." "bidoBidroundOutMngViewDTO",
                )

                if financial_published(round_info):
                    receive_khac(
                        "opening_out_financial",
                        {
                            "id": notify_id,
                            "packType": 2,
                        },
                    )
                else:
                    evidence["financial_status"] = "awaiting_publication"

            else:
                raise DetailValidationError("unsupported_khac_opening_mode")

            return payload

        evidence.update(request=body, parts={})
        payload = {}

        def receive(part, endpoint, request):
            path = ENDPOINTS[endpoint]
            evidence["parts"][part] = {
                "endpoint": path,
                "request": dict(request),
                "started_at": utc_now().isoformat(),
            }
            try:
                post = (
                    self.client.post_array
                    if endpoint == "lot_open_detail"
                    else self.client.post
                )
                payload[part] = post(path, request)
                evidence["parts"][part]["completed_at"] = utc_now().isoformat()
                evidence["parts"][part]["status"] = "received"
            except Exception as exc:
                evidence["parts"][part]["status"] = "failed"
                exc.diagnostics = {  # pyright: ignore[reportAttributeAccessIssue]
                    **getattr(exc, "diagnostics", {}),
                    "endpoint": path,
                    "request_id": body["notifyId"],
                    "part": part,
                }
                raise

        receive("notify", "notify", body)
        receive("roundmng", "roundmng", body)
        if opening_mode(payload, context) == DUAL:
            published = financial_published(lookup(payload, ROUND_ROOT))
            for phase, pack in (("technical", 1), ("financial", 2)):
                if phase == "financial" and not published:
                    evidence["financial_status"] = "awaiting_publication"
                    continue
                request = {**body, "packType": pack, "viewType": 0}
                receive(f"bid_open_{phase}", "bid_open", request)
                receive(f"lot_open_detail_{phase}", "lot_open_detail", request)
        else:
            receive("bid_open", "bid_open", body)
            receive("lot_open_detail", "lot_open_detail", body)

        # Check whether a missing financial opening belongs
        # to a cancelled and subsequently linked LDT notice.
        financial = payload.get("bid_open_financial")

        # Existing case: two-envelope notice with missing financial data.
        missing_financial = (
            context.get("processApply") == "LDT"
            and context.get("bidMode") == "1_HTHS"
            and isinstance(financial, dict)
            and "bidSubmissionByContractorViewResponse" in financial
            and financial["bidSubmissionByContractorViewResponse"] is None
        )

        # New case: single-envelope notice with missing round metadata.
        round_response = payload.get("roundmng")

        missing_single_round = (
            context.get("processApply") == "LDT"
            and context.get("bidMode") == "1_MTHS"
            and isinstance(round_response, dict)
            and "bidoBidroundMngViewDTO" in round_response
            and round_response["bidoBidroundMngViewDTO"] is None
        )

        if missing_financial or missing_single_round:
            # Keep the existing cancellation_notice fetch below.
            notice_request = {"id": body["notifyId"]}

            part = {
                "endpoint": STANDARD_NOTICE_PATH,
                "request": dict(notice_request),
                "started_at": utc_now().isoformat(),
            }
            evidence["parts"]["cancellation_notice"] = part

            try:
                response = self.client.post(
                    STANDARD_NOTICE_PATH,
                    notice_request,
                )
            except Exception:
                part["status"] = "failed"
                raise

            part.update(
                status="received",
                completed_at=utc_now().isoformat(),
            )

            # Preserve the original response for validation.
            payload["cancellation_notice"] = response

        return payload

    def financial_available(self, context):
        """Read publication metadata without refetching an already captured technical part."""
        response = self.client.post(ENDPOINTS["roundmng"], request_body(context))
        payload = {"roundmng": response}
        contract = load_config(resource="bid_opening").contracts["bid_opening"]
        result = validate_detail(
            payload,
            context,
            contract.model_copy(update={"assembly": False, "roots": [ROUND_ROOT]}),
        )
        if (
            result["status"] not in {"pass", "warn"}
            or response.get("error")
            or response.get("success") is False
        ):
            raise DetailValidationError("Invalid financial publication metadata")
        if opening_mode(payload, context) != DUAL:
            raise DetailValidationError(
                "Opening mode changed during financial publication check"
            )
        return financial_published(lookup(payload, ROUND_ROOT))


def _iter_records(
    api,
    *,
    identity,
    config,
    search_items,
    run_id,
    source_date,
    search_page,
    errors,
    stats,
):
    import httpx

    for item in search_items:
        context = {
            key: item.get(key)
            for key in (
                "id",
                "notifyId",
                "notifyNo",
                "notifyVersion",
                "publicDate",
                "publicDateKqmt",
                "bidRealityOpenDate",
                "bidOpenDate",
                "stepCode",
                "processApply",
                "bidForm",
                "bidMode",
                "isInternet",
            )
        }
        context["id"] = context.get("notifyId") or context.get("id")
        observation = {
            "context": context,
            "contract": "bid_opening",
            "config_hash": config.fingerprint,
            "rule_version": config.version,
        }
        try:
            payload = api.fetch(item, observation)
            result = validate_detail(payload, context, config.contracts["bid_opening"])
            observation["result"] = result
            if result["status"] not in {"pass", "warn"}:
                raise DetailValidationError(
                    ",".join(
                        f"{i['code']}:{i.get('path', '')}" for i in result["issues"]
                    )
                )
            record = build_bronze_item(
                table="bid_opening_detail",
                source_id=result["identity"]["notifyNo"],
                source_version=result["identity"]["notifyVersion"],
                payload=payload,
                run_id=run_id,
                source_date=source_date,
            )
        except (httpx.HTTPError, ValueError, TypeError, KeyError) as exc:
            stats.error("bid_opening")
            observation.setdefault(
                "result", {"status": "fail", "error": type(exc).__name__}
            )
            errors.append(
                build_error_record(
                    identity=identity,
                    run_id=run_id,
                    source_date=source_date,
                    page_number=search_page,
                    source_id=item.get("notifyNo"),
                    stage="bid_opening_detail",
                    exc=exc,
                )
            )
            stats.quality_observations.append(observation)
            continue
        stats.quality_observations.append(observation)
        stats.record("bid_opening")
        yield record


def iter_records(
    api,
    *,
    identity,
    config,
    search_items,
    run_id,
    source_date,
    search_page,
    errors,
    stats,
    detail_workers=3,
):
    """Parallelize notices only; worker-local evidence is merged in search order."""
    if not 1 <= detail_workers <= 32:
        raise ValueError("bid opening detail_workers must be between 1 and 32")
    arguments = {
        "identity": identity,
        "config": config,
        "run_id": run_id,
        "source_date": source_date,
        "search_page": search_page,
    }
    if detail_workers == 1 or len(search_items) <= 1:
        yield from _iter_records(
            api, search_items=search_items, errors=errors, stats=stats, **arguments
        )
        return

    def collect(item):
        local_stats, local_errors = PageStats(), []
        records = list(
            _iter_records(
                api,
                search_items=[item],
                errors=local_errors,
                stats=local_stats,
                **arguments,
            )
        )
        return records, local_errors, local_stats

    remaining = iter(enumerate(search_items))
    results: list[Any] = [None] * len(search_items)
    with ThreadPoolExecutor(
        max_workers=min(detail_workers, len(search_items)),
        thread_name_prefix="bid-opening-detail",
    ) as pool:
        pending = {}

        def submit_next():
            entry = next(remaining, None)
            if entry is not None:
                index, item = entry
                pending[pool.submit(collect, item)] = index

        try:
            for _ in range(min(detail_workers, len(search_items))):
                submit_next()
            while pending:
                done, _ = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    results[pending.pop(future)] = future.result()
                for _ in done:
                    submit_next()
        except BaseException:
            for future in pending:
                future.cancel()
            cancel = getattr(api.client, "cancel_requests", None)
            if cancel is not None:
                cancel()
            raise

    # Only the caller changes page counters/evidence and passes records to the writer.
    for res in results:
        if res is None:
            continue
        records, local_errors, local_stats = res
        errors.extend(local_errors)
        stats.merge(local_stats)
        stats.quality_observations.extend(local_stats.quality_observations)
        yield from records


def create_bid_opening_spec(client, *, detail_workers=None):
    workers = (
        settings.BID_OPENING_DETAIL_WORKERS
        if detail_workers is None
        else detail_workers
    )
    if not 1 <= workers <= 32:
        raise ValueError("bid opening detail_workers must be between 1 and 32")
    identity = get_resource("bid_opening").identity
    config = load_config(resource="bid_opening")
    api = BidOpeningApi(client)
    return ResourceSpec(
        identity=identity,
        pipeline_name="muasamcong_bronze",
        dataset_name="muasamcong",
        fetch_page=api.search,
        search_key=search_document_key,
        iter_records=partial(
            iter_records, api, identity=identity, config=config, detail_workers=workers
        ),
        quality_config_hash=config.fingerprint,
    )
