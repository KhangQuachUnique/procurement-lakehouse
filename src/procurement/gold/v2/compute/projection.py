"""Public-grain contributions and dimensions from private business relations."""

from pathlib import Path

from procurement.gold.v2.aggregation import create_ledger
from procurement.gold.v2.schema import DIMENSIONS, FACTS, TABLES, create_tables


def project(db, plan, *, retain_accepted=True):
    db.execute("DROP SCHEMA IF EXISTS changes CASCADE")
    create_ledger(db, "changes")
    for name in FACTS:
        query = Path(__file__).with_name("sql").joinpath(name + ".sql").read_text()
        # These are validated dates from immutable plan, not raw SQL values.
        query = query.format(
            event_start=plan.event_start.isoformat(), event_end=plan.event_end.isoformat()
        )
        db.execute(f"INSERT INTO changes.{name} " + query)
    db.execute("DROP SCHEMA IF EXISTS dimensions CASCADE")
    create_tables(db, "dimensions")
    for name in DIMENSIONS:
        if name in {"dim_progress_status", "dim_duration_metric"}:
            continue
        fields = []
        for col in TABLES[name].columns:
            fields.append(
                {
                    "iso_year": "extract(isoyear FROM date_key)",
                    "iso_week": "week(date_key)",
                    "week_start_date": "date_trunc('week',date_key)::DATE",
                    "valid_from": "NULL::DATE",
                    "valid_to": "NULL::DATE",
                    "is_current": "NULL::BOOLEAN",
                }.get(col.name, col.name)
            )
        db.execute(f"INSERT INTO dimensions.{name} SELECT {','.join(fields)} FROM business.{name}")
    db.execute(
        """INSERT INTO dimensions.dim_date SELECT ?,year(?),quarter(?),month(?),day(?),
      dayofweek(?),dayofweek(?) IN (0,6),year(?)||'-Q'||quarter(?),strftime(?,'%Y-%m'),
      extract(isoyear FROM ?),week(?),date_trunc('week',?)::DATE
      WHERE NOT EXISTS (SELECT 1 FROM dimensions.dim_date WHERE date_key=?)""",
        [plan.observation_day] * 14,
    )
    for metric, unit in (
        ("plan_to_notice_days", "days"),
        ("notice_to_close_hours", "hours"),
        ("close_to_first_award_days", "days"),
        ("close_to_complete_award_days", "days"),
    ):
        db.execute(
            "INSERT INTO dimensions.dim_duration_metric VALUES (?,?,?)", [metric, metric, unit]
        )
    db.execute("""INSERT INTO dimensions.dim_progress_status
      SELECT DISTINCT 'STATUS_'||sha256(to_json(list_value(notice_observation_state,award_observation_state))),
        notice_observation_state,award_observation_state FROM business.fact_procurement_demand""")
    db.execute("""INSERT INTO dimensions.dim_party
      SELECT DISTINCT recipient_set_key,'UNKNOWN_GROUP',NULL,
        CASE WHEN recipient_set_key='REC_MULTIPLE_INDEPENDENT' THEN 'Nhiều bên nhận — chưa phân bổ'
             WHEN recipient_set_key='REC_NO_WINNER' THEN 'Chưa có bên nhận' ELSE 'Bên nhận chưa định danh' END,
        NULL,'reporting_bucket'
      FROM business.fact_award a WHERE NOT EXISTS (
        SELECT 1 FROM dimensions.dim_party p WHERE p.party_key=a.recipient_set_key)""")
    from ..references import apply

    apply(db, plan)
    # Retain dictionary entries referenced by past status snapshots/days. Current
    # descriptors replace old same-key rows; keys cannot fan out by version.
    exists = db.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_catalog=current_database() AND table_schema='accepted' AND table_name='dim_date'"
    ).fetchone()[0]
    if exists and retain_accepted:
        for name in DIMENSIONS:
            key = TABLES[name].key
            db.execute(
                f"INSERT INTO dimensions.{name} SELECT * FROM accepted.{name} a WHERE NOT EXISTS (SELECT 1 FROM dimensions.{name} d WHERE d.{key}=a.{key})"
            )
    return {
        name: db.execute(f"SELECT count(*) FROM changes.{name}").fetchone()[0] for name in FACTS
    }
