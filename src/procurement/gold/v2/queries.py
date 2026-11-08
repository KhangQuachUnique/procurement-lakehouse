"""Per-fact safe rollups: currencies/metrics separated, status not summed over days."""

from .contracts import identifier
from .schema import FACTS, TABLES


def rollup_sql(table, period="month", dimensions=None, schema="gold_v2"):
    if table not in FACTS or period not in {"day", "week", "month", "quarter", "year"}:
        raise ValueError("Unsupported fact or period")
    identifier(schema)
    contract = TABLES[table]
    allowed = set(contract.grain) - {"report_date"}
    dimensions = (
        list(dimensions)
        if dimensions is not None
        else [c for c in contract.grain if c != "report_date"]
    )
    if len(dimensions) != len(set(dimensions)) or not set(dimensions) <= allowed:
        raise ValueError("Invalid fact-specific dimensions")
    if "currency_key" in contract.grain and "currency_key" not in dimensions:
        raise ValueError("Money currencies must remain separated")
    if table == "fact_duration_daily" and "metric_key" not in dimensions:
        raise ValueError("Duration metrics/units must remain separated")
    source = schema + "." + table
    if table == "fact_status_daily":
        source = f"(SELECT * FROM {source} QUALIFY report_date=max(report_date) OVER(PARTITION BY date_trunc('{period}',report_date)))"
    fields = [f"date_trunc('{period}',report_date)::DATE AS period_start", *dimensions]
    fields.extend(f"SUM({c.name}) AS {c.name}" for c in contract.measures)
    if table == "fact_duration_daily":
        fields.append("SUM(duration_sum)/NULLIF(SUM(sample_count),0) AS average_duration")
    if table == "fact_status_daily":
        fields.append("MAX(report_date) AS observed_date")
    if table == "fact_savings_daily":
        fields.extend(
            [
                "100.0*SUM(descriptive_difference_sum)/NULLIF(SUM(descriptive_baseline_sum),0) AS descriptive_difference_percent",
                "100.0*SUM(verified_saving_sum)/NULLIF(SUM(verified_baseline_sum),0) AS verified_saving_percent",
            ]
        )
    return (
        "SELECT "
        + ",".join(fields)
        + " FROM "
        + source
        + " GROUP BY "
        + ",".join(str(i + 1) for i in range(1 + len(dimensions)))
    )
