"""Resource metadata shared by jobs and Ops, without importing ingestion or DLT."""

from dataclasses import dataclass

from procurement.common.resources import ResourceIdentity

DEFAULT_SOURCE = "muasamcong"


@dataclass(frozen=True)
class ResourceDefinition:
    identity: ResourceIdentity
    tables: tuple[str, ...]
    spec_factory: str


RESOURCE_CATALOG = (
    ResourceDefinition(
        ResourceIdentity(DEFAULT_SOURCE, "project"),
        ("project_detail",),
        "procurement.ingestion.sources.muasamcong.project.resource:create_project_spec",
    ),
    ResourceDefinition(
        ResourceIdentity(DEFAULT_SOURCE, "khlcnt"),
        ("khlcnt_plan_detail", "khlcnt_bid_package_detail"),
        "procurement.ingestion.sources.muasamcong.khlcnt.resource:create_khlcnt_spec",
    ),
    ResourceDefinition(
        ResourceIdentity(DEFAULT_SOURCE, "notify_contractor"),
        ("notify_contractor_standard_detail", "notify_contractor_reoffer_detail"),
        "procurement.ingestion.sources.muasamcong.notify_contractor.resource:"
        "create_notify_contractor_spec",
    ),
    ResourceDefinition(
        ResourceIdentity(DEFAULT_SOURCE, "contractor_result"),
        ("contractor_result_detail",),
        "procurement.ingestion.sources.muasamcong.contractor_result.resource:"
        "create_contractor_result_spec",
    ),
)

SUPPORTED_RESOURCES = tuple(item.identity.resource for item in RESOURCE_CATALOG)


def get_resource(resource: str, *, source: str = DEFAULT_SOURCE) -> ResourceDefinition:
    for definition in RESOURCE_CATALOG:
        if definition.identity == ResourceIdentity(source, resource):
            return definition
    raise ValueError(f"Unsupported resource: {source}/{resource}")
