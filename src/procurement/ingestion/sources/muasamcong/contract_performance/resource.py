"""Read-only CSDL contract execution adapter; daily by report publication date."""

from collections import defaultdict
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from typing import Any

from procurement.common.catalog import get_resource
from procurement.ingestion.engine.metadata import calculate_content_hash
from procurement.ingestion.engine.models import BronzeItem, ResourceSpec
from procurement.ingestion.engine.records import build_bronze_item
from procurement.ingestion.engine.stats import PageStats
from procurement.ingestion.sources.muasamcong.search import JsonPostClient
from procurement.models.errors import ErrorRecord

SEARCH_PATH = "/o/egp-portal-database/services/result-perform-contract/search-view-po"
LOOKUP_PATH = "/o/egp-portal-database/services/smart/search"
LINKED_PATH = "/o/egp-portal-contractor-selection-v2/services/get-linked-list-contract-po"
CONTRACT_PATH = (
    "/o/egp-portal-contractor-selection-v2/services/econsign/contract-info/detail-by-id-for-po"
)
IDENTITY = get_resource("contract_performance").identity


def required_text(value: Any, field: str) -> str:
    if value is None or isinstance(value, (dict, list, bool)) or not str(value).strip():
        raise ValueError(f"Missing/invalid source field: {field}")
    return str(value).strip()


def report_key(item: dict[str, Any]) -> tuple[str, str]:
    return required_text(item.get("id"), "id"), required_text(item.get("version"), "version")


class ContractPerformanceApi:
    def __init__(self, client: JsonPostClient) -> None:
        self.client = client

    def search(
        self, *, page_number: int, page_size: int, window_from: str, window_to: str
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            key: {"contains": None}
            for key in (
                "contractorName",
                "contractNo",
                "contractCode",
                "bidName",
                "taxCode",
                "investorName",
            )
        }
        body.update(
            {
                "publicDate": {"greaterThan": window_from, "lessThan": window_to},
                "contractSignDate": {"greaterThan": None, "lessThan": None},
                "pageSize": page_size,
                "pageNumber": page_number,
            }
        )
        raw = self.client.post(SEARCH_PATH, body)
        page = raw.get("page")
        if not isinstance(page, dict) or "currentPage" not in page:
            raise ValueError("Performance search missing page/currentPage")
        # Response uses 1-based currentPage; source response is never modified in place.
        return {**raw, "page": {**page, "currentPage": int(page["currentPage"]) - 1}}

    def linked(self, code: str) -> dict[str, Any]:
        response = self.client.post(LINKED_PATH, {"contractCode": code})
        if not isinstance(response.get("resultList"), list):
            raise TypeError("Linked response missing resultList")
        return response

    def lookup(self, code: str) -> str | None:
        response = self.client.post(
            LOOKUP_PATH,
            [
                {
                    "pageNumber": 0,
                    "pageSize": 1,
                    "query": [
                        {
                            "index": "es-contractor-selection",
                            "keyWord": code,
                            "matchType": "exact",
                            "matchFields": ["contractCode"],
                            "filters": [
                                {
                                    "fieldName": "type",
                                    "searchType": "in",
                                    "fieldValues": ["es-ct-contract"],
                                }
                            ],
                        }
                    ],
                }
            ],
        )
        page = response.get("page")
        if not isinstance(page, dict) or not isinstance(page.get("content"), list):
            raise TypeError("Contract lookup missing page.content")
        total = int(page["totalElements"])
        if total == 0 and not page["content"]:
            return None
        if total != 1 or len(page["content"]) != 1:
            raise ValueError("Contract lookup is ambiguous or inconsistent")
        hit = page["content"][0]
        if hit.get("contractCode") != code:
            raise ValueError("Contract lookup returned another contractCode")
        return required_text(hit.get("id"), "lookup.id")

    def contract(self, source_id: str, code: str) -> dict[str, Any]:
        response = self.client.post(CONTRACT_PATH, {"id": source_id})
        root = response.get("contract")
        if not isinstance(root, dict) or not isinstance(response.get("versionsList"), list):
            raise TypeError("Contract detail missing contract/versionsList")
        if root.get("id") != source_id or root.get("contractCode") != code:
            raise ValueError("Contract detail identity mismatch")
        required_text(root.get("contractVersion"), "contract.contractVersion")
        return response


class PerformanceExtraction:
    """One day of caches; each contract group is handled by only one worker at a time."""

    def __init__(self, api: ContractPerformanceApi, workers: int) -> None:
        self.api, self.workers = api, workers
        self.linked_cache: dict[str, dict] = {}
        self.contract_cache: dict[str, dict[str, dict]] = {}
        self.emitted: set[tuple] = set()

    def resolve(self, group):
        code, items = group
        if code not in self.linked_cache:
            self.linked_cache[code] = self.api.linked(code)
        linked = self.linked_cache[code]
        reports = []
        for item in items:
            matches = [
                r
                for r in linked["resultList"]
                if isinstance(r, dict) and report_key(r) == report_key(item)
            ]
            if len(matches) != 1:
                raise ValueError(
                    "Linked response must contain exactly the requested report version"
                )
            report = matches[0]
            if report.get("contractCode") != code or required_text(
                report.get("contractVersion"), "contractVersion"
            ) != required_text(item.get("contractVersion"), "search.contractVersion"):
                raise ValueError("Report contract reference changed between search and detail")
            reports.append((item, report))

        if code not in self.contract_cache:
            source_id = self.api.lookup(code)
            self.contract_cache[code] = {}
            if source_id:
                current = self.api.contract(source_id, code)
                self.contract_cache[code][source_id] = current
        responses = self.contract_cache[code]
        output = []
        for item, report in reports:
            wanted = str(report["contractVersion"])
            exact = [
                r for r in responses.values() if str(r["contract"]["contractVersion"]) == wanted
            ]
            if not exact and responses:
                # versionsList comes from the already observed current contract response.
                ids = {
                    str(v["id"])
                    for response in responses.values()
                    for v in response["versionsList"]
                    if isinstance(v, dict) and str(v.get("version")) == wanted and v.get("id")
                }
                if len(ids) > 1:
                    raise ValueError("Conflicting contract version identifiers")
                if ids:
                    source_id = next(iter(ids))
                    resolved = self.api.contract(source_id, code)
                    if str(resolved["contract"]["contractVersion"]) != wanted:
                        raise ValueError("Referenced contract version mismatch")
                    responses[source_id] = resolved
                    exact = [resolved]
            status = (
                "resolved"
                if exact
                else ("missing_contract_version" if responses else "missing_contract")
            )
            output.append((item, report, status))
        return linked, output, list(responses.values())

    def records(
        self,
        *,
        search_items: list[dict[str, Any]],
        run_id: str,
        source_date: date,
        search_page: int,
        errors: list[ErrorRecord],
        stats: PageStats,
    ) -> Iterator[BronzeItem]:
        groups = defaultdict(list)
        for item in search_items:
            report_key(item)
            required_text(item.get("contractVersion"), "search.contractVersion")
            if required_text(item.get("publicDate"), "search.publicDate")[:10] != str(source_date):
                raise ValueError("Search returned publication date outside source_date")
            groups[required_text(item.get("contractCode"), "search.contractCode")].append(item)
        # Four workers are sufficient; the HTTP budget also covers retries and discovery calls.
        pool = ThreadPoolExecutor(max_workers=self.workers)
        futures = [pool.submit(self.resolve, group) for group in groups.items()]
        try:
            for future in futures:
                linked, reports, contracts = future.result()
                for response in contracts:
                    root = response["contract"]
                    key = (
                        "contract",
                        root["id"],
                        str(root["contractVersion"]),
                        calculate_content_hash(root),
                    )
                    if key in self.emitted:
                        continue
                    self.emitted.add(key)
                    stats.record("contract")
                    yield build_bronze_item(
                        table="contract_detail",
                        source_id=root["id"],
                        source_version=str(root["contractVersion"]),
                        payload=response,
                        run_id=run_id,
                        source_date=source_date,
                    )
                for search, report, status in reports:
                    if required_text(report.get("publicDate"), "performance.publicDate")[
                        :10
                    ] != str(source_date):
                        raise ValueError("Report publication date changed outside source_date")
                    if status != "resolved":
                        stats.quality_observations.append({"code": status, "id": report["id"]})
                    stats.record("contract_performance")
                    yield build_bronze_item(
                        table="contract_performance_detail",
                        source_id=str(report["id"]),
                        source_version=str(report["version"]),
                        run_id=run_id,
                        source_date=source_date,
                        payload={
                            "performance": report,
                            "search": search,
                            "linked": linked,
                            "resolution": {"status": status},
                        },
                    )

        finally:
            for future in futures:
                future.cancel()
            pool.shutdown(wait=True, cancel_futures=True)


def create_contract_performance_spec(
    client: JsonPostClient, *, detail_workers: int = 4
) -> ResourceSpec:
    if not 1 <= detail_workers <= 4:
        raise ValueError("contract_performance detail_workers must be between 1 and 4")
    api = ContractPerformanceApi(client)
    extraction = PerformanceExtraction(api, detail_workers)
    return ResourceSpec(
        identity=IDENTITY,
        pipeline_name="muasamcong_bronze",
        dataset_name="muasamcong",
        fetch_page=api.search,
        iter_records=extraction.records,
        search_key=report_key,
        strict_pagination=True,
    )
