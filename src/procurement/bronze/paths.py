"""Layout rules and path templates for Bronze storage."""

from datetime import date


def bronze_attempt_prefix(
    *,
    bucket: str,
    dataset: str = "muasamcong",
    table_name: str,
    source_date: date,
    run_id: str,
) -> str:
    """Return prefix path for an attempt's files on object storage."""
    return f"{bucket}/bronze/{dataset}/{table_name}/source_date={source_date.isoformat()}/run_id={run_id}"


def bronze_layout_template() -> str:
    """Return DLT filesystem layout template."""
    return "{table_name}/source_date={source_date}/run_id={run_id}/{load_id}.{file_id}.{ext}"
