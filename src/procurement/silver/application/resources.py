"""Resource-level Silver contracts, independent of CLI spelling."""

from dataclasses import dataclass

from procurement.common.catalog import get_resource

NOTICE_MAPPING_VERSION = "notice-3"
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

PROJECT_MAPPING_VERSION = "project-2"
PROJECT_OUTPUT_TABLES = (
    "observation",
    "project_revision",
    "project_location",
    "quality_issue",
    "quarantine",
)
PROJECT_TABLES = ("project_detail",)

RESULT_MAPPING_VERSION = "result-3"
RESULT_OUTPUT_TABLES = (
    "observation",
    "result_revision",
    "award_lot",
    "award_contractor",
    "quality_issue",
    "quarantine",
)
RESULT_TABLES = ("contractor_result_detail",)

KHLCNT_MAPPING_VERSION = "khlcnt-2"
KHLCNT_OUTPUT_TABLES = (
    "observation",
    "plan_revision",
    "package_revision",
    "package_location",
    "package_lot",
    "quality_issue",
    "quarantine",
)
KHLCNT_TABLES = (
    "khlcnt_plan_detail",
    "khlcnt_bid_package_detail",
)


@dataclass(frozen=True)
class SilverResource:
    """Specification of a Silver candidate resource contract."""

    resource: str
    families: tuple[tuple[str, str], ...]
    output_tables: tuple[str, ...]
    mapping_version: str

    def candidate_groups(
        self, data_prefix: str, *, isolate_governance: bool = False
    ) -> tuple[tuple[str, tuple[str, ...]], ...]:
        """Keep new execution lineage files from overwriting other resources' daily files."""
        if isolate_governance or self.resource in {"contract_performance", "contractor_result"}:
            domain = tuple(
                t
                for t in self.output_tables
                if t not in {"observation", "quality_issue", "quarantine"}
            )
            governance = tuple(t for t in self.output_tables if t not in domain)
            return (
                (data_prefix, domain),
                (f"{data_prefix}/_governance/{self.resource}", governance),
            )
        return ((data_prefix, self.output_tables),)

    def select_tables(self, family: str | None = None) -> tuple[str, ...]:
        available = dict(self.families)
        if family is not None and family not in available:
            raise ValueError(f"Unknown family: {family}; choose {', '.join(available)}")
        return (available[family],) if family else tuple(available.values())


def get_silver_resource(name: str) -> SilverResource:
    """Retrieve the SilverResource configuration for a resource alias."""
    if name == "contract_performance":
        return SilverResource(
            resource=name,
            families=(
                ("contract", "contract_detail"),
                ("performance", "contract_performance_detail"),
            ),
            output_tables=(
                "observation",
                "contract_revision",
                "contract_performance_revision",
                "quality_issue",
                "quarantine",
            ),
            mapping_version="contract-performance-2",
        )
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
    if name in {"result", "contractor_result", "kq_lcnt"}:
        definition = get_resource("contractor_result")
        families = tuple((table.removesuffix("_detail"), table) for table in definition.tables)
        return SilverResource(
            definition.identity.resource, families, RESULT_OUTPUT_TABLES, RESULT_MAPPING_VERSION
        )
    if name in {"khlcnt", "plan", "package"}:
        definition = get_resource("khlcnt")
        families = (
            ("plan", "khlcnt_plan_detail"),
            ("package", "khlcnt_bid_package_detail"),
        )
        return SilverResource(
            definition.identity.resource, families, KHLCNT_OUTPUT_TABLES, KHLCNT_MAPPING_VERSION
        )
    raise ValueError(f"Silver resource not implemented: {name}")


__all__ = [
    "KHLCNT_MAPPING_VERSION",
    "KHLCNT_OUTPUT_TABLES",
    "NOTICE_MAPPING_VERSION",
    "NOTICE_OUTPUT_TABLES",
    "PROJECT_MAPPING_VERSION",
    "PROJECT_OUTPUT_TABLES",
    "RESULT_MAPPING_VERSION",
    "RESULT_OUTPUT_TABLES",
    "SilverResource",
    "get_silver_resource",
]
