from unittest.mock import Mock

import duckdb
import pytest

from procurement.gold.domain.schema import TABLES
from procurement.tools.gold_history_acceptance import compare_generations


def test_parity_is_multiset_exact_not_just_totals():
    with duckdb.connect(config={"threads": "1", "memory_limit": "256MB"}) as db:
        db.execute("CREATE SCHEMA v2_incremental")
        db.execute("CREATE SCHEMA v2_clean")
        for name in TABLES:
            db.execute(f"CREATE TABLE v2_incremental.{name}(key VARCHAR,amount BIGINT)")
            db.execute(f"CREATE TABLE v2_clean.{name}(key VARCHAR,amount BIGINT)")
        publisher = Mock()
        first, second = Mock(), Mock()
        assert all(v == 0 for v in compare_generations(db, publisher, first, second).values())
        fact = "fact_award_daily"
        db.execute(f"INSERT INTO v2_incremental.{fact} VALUES ('a',10),('a',10)")
        db.execute(f"INSERT INTO v2_clean.{fact} VALUES ('a',10)")
        with pytest.raises(ValueError, match="generation mismatch"):
            compare_generations(db, publisher, first, second)
        db.execute(f"DELETE FROM v2_incremental.{fact}")
        db.execute(f"DELETE FROM v2_clean.{fact}")
        db.execute(f"INSERT INTO v2_incremental.{fact} VALUES ('a',10),('b',20)")
        db.execute(f"INSERT INTO v2_clean.{fact} VALUES ('a',20),('b',10)")
        with pytest.raises(ValueError, match="generation mismatch"):
            compare_generations(db, publisher, first, second)
