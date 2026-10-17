"""Resource-level Silver contracts, independent of CLI spelling."""

from dataclasses import dataclass

from procurement.common.catalog import get_resource

NOTICE_MAPPING_VERSION = "notice-2"
NOTICE_OUTPUT_TABLES = (
    "observation",
    "notice_revision",
    "quality_issue",
    "quarantine",
    "notice_lot",
    "notice_location",
    "notice_reoffer_submission",
)
NOTICE_TABLES = (
    "notify_contractor_standard_detail",
    "notify_contractor_vk_adb_detail",
    "notify_contractor_reoffer_detail",
)

PROJECT_MAPPING_VERSION = "project-1"
PROJECT_OUTPUT_TABLES = (
    "observation",
    "project_revision",
    "project_location",
    "quality_issue",
    "quarantine",
)
PROJECT_TABLES = ("project_detail",)


@dataclass(frozen=True)
class SilverResource:
    """Specification of a Silver candidate resource contract."""

    resource: str
    families: tuple[tuple[str, str], ...]
    output_tables: tuple[str, ...]
    mapping_version: str

    def select_tables(self, family: str | None = None) -> tuple[str, ...]:
        available = dict(self.families)
        if family is not None and family not in available:
            raise ValueError(f"Unknown family: {family}; choose {', '.join(available)}")
        return (available[family],) if family else tuple(available.values())


def get_silver_resource(name: str) -> SilverResource:
    """Retrieve the SilverResource configuration for a resource alias."""
    if name in {"notice", "tbmt", "notify_contractor"}:
        definition = get_resource("notify_contractor")
        families = tuple(
            (table.removeprefix("notify_contractor_").removesuffix("_detail"), table)
            for table in definition.tables
        )
        return SilverResource(
            definition.identity.resource, families, NOTICE_OUTPUT_TABLES, NOTICE_MAPPING_VERSION
        )
    if name in {"project", "du_an"}:
        definition = get_resource("project")
        families = tuple((table.removesuffix("_detail"), table) for table in definition.tables)
        return SilverResource(
            definition.identity.resource, families, PROJECT_OUTPUT_TABLES, PROJECT_MAPPING_VERSION
        )
    raise ValueError(f"Silver resource not implemented: {name}")


__all__ = [
    "NOTICE_MAPPING_VERSION",
    "NOTICE_OUTPUT_TABLES",
    "PROJECT_MAPPING_VERSION",
    "PROJECT_OUTPUT_TABLES",
    "SilverResource",
    "get_silver_resource",
]
