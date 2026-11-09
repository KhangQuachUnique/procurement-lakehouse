"""SQL assets use only trusted internal expressions and validated context."""

from functools import lru_cache
from pathlib import Path


@lru_cache
def template(name):
    if Path(name).name != name or not name.endswith(".sql"):
        raise ValueError("Invalid SQL asset name")
    return Path(__file__).with_name(name).read_text()


def execute_sql(db, name, values):
    db.execute(template(name).format_map(values))
