import pytest

from procurement.gold.domain.models import INPUT_TABLES
from procurement.gold.domain.clustering.planning import affected_owners, plan_components, select_batch
from procurement.silver.domain.schema import get_silver_schemas
from tests.support.silver_business import populate_comprehensive_silver_fixtures


def populated(db):
    db.execute("CREATE SCHEMA frozen")
    db.execute("CREATE SCHEMA input_cache")
    for name, ddl in get_silver_schemas().items():
        db.execute(f"CREATE TABLE frozen.{name} ({ddl})")
    db.execute("CREATE SCHEMA silver")
    for name, ddl in get_silver_schemas().items():
        db.execute(f"CREATE TABLE silver.{name} ({ddl})")
    populate_comprehensive_silver_fixtures(db)
    for n in INPUT_TABLES:
        db.execute(f"CREATE OR REPLACE TABLE frozen.{n} AS SELECT * FROM silver.{n}")
        db.execute(f"CREATE TABLE input_cache.{n} AS SELECT * FROM frozen.{n}")


def test_noop_then_edit_and_old_new_links(db):
    populated(db)
    assert plan_components(db)["affected_nodes"] == 0
    db.execute("UPDATE frozen.package_revision SET bid_price=bid_price+1 WHERE entity_id='p1'")
    result = plan_components(db)
    assert result["affected_nodes"] > 0
    affected_owners(db)
    assert db.execute("SELECT count(*) FROM affected WHERE owner_key='PKG:p1'").fetchone()[0] == 1
    select_batch(db, 0)
    assert (
        db.execute("SELECT count(*) FROM scoped.package_revision WHERE entity_id='p1'").fetchone()[
            0
        ]
        > 0
    )
    # Move a notice link: both old and new dependency families remain in closure.
    db.execute(
        "UPDATE frozen.notice_revision SET package_id='pkg_reparented' WHERE entity_id=(SELECT min(entity_id) FROM frozen.notice_revision)"
    )
    assert plan_components(db)["affected_nodes"] >= result["affected_nodes"]


def test_oversized_components_fail_without_splitting(db):
    populated(db)
    with pytest.raises(ValueError, match="Whole dependency component"):
        plan_components(db, bootstrap=True, batch_rows=1)
