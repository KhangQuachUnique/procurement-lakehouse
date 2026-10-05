"""Resource-specific source operations shared by audit evidence and repair."""
from procurement.quality.contracts import ENDPOINTS

QUALITY_RESOURCES = ("notify_contractor", "bid_opening")


def search_api(client, resource):
    if resource == "bid_opening":
        from procurement.ingestion.sources.muasamcong.bid_opening.resource import BidOpeningApi
        return BidOpeningApi(client)
    if resource == "notify_contractor":
        from procurement.ingestion.sources.muasamcong.notify_contractor.resource import (
            NotifyContractorApi,
        )
        return NotifyContractorApi(client)
    raise ValueError(f"No quality adapter: {resource}")


def fetch_detail(client, route, context, evidence):
    if route.contract == "bid_opening":
        return search_api(client, "bid_opening").fetch(context, evidence)
    return client.post(ENDPOINTS[route.contract], {"id": context["id"]})
