"""Cardinality-safe reference resolution compiled into grouped relational joins."""


def reference_join(
    source_id: str,
    source_code: str,
    target: str,
    code_column: str,
    raw_id: str,
    alias: str,
) -> str:
    """At most one target; ambiguous exact IDs never fall back to codes.

    Grouped ID maps avoid correlated OR/nested-loop joins over millions of rows.
    Arguments are internal SQL expressions, never unvalidated user input.
    """
    return f"""
    LEFT JOIN (
        SELECT reference_id, COUNT(DISTINCT entity_id) AS count, MIN(entity_id) AS entity_id
        FROM (
            SELECT t.entity_id AS reference_id, t.entity_id FROM {target} t
            UNION ALL
            SELECT {raw_id} AS reference_id, t.entity_id FROM {target} t
        ) identifiers
        WHERE NULLIF(TRIM(reference_id), '') IS NOT NULL
        GROUP BY reference_id
    ) {alias}_id ON {alias}_id.reference_id = {source_id}
    LEFT JOIN (
        SELECT t.{code_column} AS business_code, COUNT(DISTINCT t.entity_id) AS count,
               MIN(t.entity_id) AS entity_id
        FROM {target} t
        WHERE NULLIF(TRIM(t.{code_column}), '') IS NOT NULL
        GROUP BY t.{code_column}
    ) {alias}_code ON {alias}_code.business_code = {source_code}
    LEFT JOIN {target} {alias} ON {alias}.entity_id = CASE
        WHEN {alias}_id.count = 1 THEN {alias}_id.entity_id
        WHEN COALESCE({alias}_id.count, 0) = 0 AND {alias}_code.count = 1 THEN {alias}_code.entity_id
        ELSE NULL END
    """
