"""Global descriptive refresh: deterministic source-wide labels, not last batch wins."""


def refresh_party_labels(db):
    db.execute(
        "CREATE OR REPLACE TEMP TABLE latest_supplier_labels AS\n    WITH results AS (\n      SELECT r.*,o.observed_at,ROW_NUMBER() OVER (\n        PARTITION BY coalesce(r.result_id,r.entity_id)\n        ORDER BY coalesce(try_cast(regexp_extract(coalesce(r.result_version,'0'),'([0-9]+)',1) AS BIGINT),0) DESC,\n                 o.observed_at DESC NULLS LAST,r.public_date DESC NULLS LAST,r.revision_id DESC) rn\n      FROM frozen.result_revision r JOIN frozen.observation o USING(observation_id)\n    ), candidates AS (\n      SELECT 'PARTY:' || coalesce(nullif(trim(a.contractor_code),''),nullif(trim(a.tax_code),'')) party_key,\n             a.contractor_name party_name,a.tax_code,r.observed_at,r.revision_id,a.award_contractor_id\n      FROM frozen.award_contractor a JOIN results r USING(revision_id) WHERE r.rn=1\n      UNION ALL\n      SELECT 'CON:' || nullif(trim(a.venture_code),''),a.venture_name,NULL,r.observed_at,r.revision_id,a.award_contractor_id\n      FROM frozen.award_contractor a JOIN results r USING(revision_id) WHERE r.rn=1\n    ), described AS (\n      SELECT *,first_value(tax_code IGNORE NULLS) OVER (\n        PARTITION BY party_key ORDER BY observed_at DESC NULLS LAST,revision_id DESC,award_contractor_id DESC\n        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) known_tax_code\n      FROM candidates WHERE party_key IN (SELECT party_key FROM dimensions.dim_party)\n    )\n    SELECT * EXCLUDE(tax_code,known_tax_code),known_tax_code AS tax_code\n    FROM described WHERE nullif(trim(party_name),'') IS NOT NULL\n    QUALIFY row_number() OVER(PARTITION BY party_key ORDER BY observed_at DESC NULLS LAST,revision_id DESC,award_contractor_id DESC)=1"
    )
    db.execute(
        "UPDATE dimensions.dim_party p SET party_name=s.party_name,tax_code=s.tax_code FROM latest_supplier_labels s WHERE p.party_key=s.party_key"
    )
    db.execute(
        "CREATE OR REPLACE TEMP TABLE global_procurer_labels AS\n      WITH ranked AS (\n        SELECT *,row_number() OVER (\n          PARTITION BY coalesce(source_family,'VN'),coalesce(notify_no,entity_id)\n          ORDER BY coalesce(try_cast(regexp_extract(coalesce(notify_version,'0'),'([0-9]+)',1) AS BIGINT),0) DESC,\n            published_at DESC NULLS LAST,public_date DESC NULLS LAST,revision_id DESC) rn\n        FROM frozen.notice_revision\n      ) SELECT 'PARTY_PROCURER:' || trim(procuring_entity_code) party_key,\n          first(procuring_entity_name ORDER BY procuring_entity_name NULLS LAST,procuring_entity_code NULLS LAST) party_name,\n          first(procuring_entity_code ORDER BY procuring_entity_name NULLS LAST,procuring_entity_code NULLS LAST) party_code\n        FROM ranked WHERE rn=1 AND nullif(trim(procuring_entity_code),'') IS NOT NULL\n        GROUP BY trim(procuring_entity_code)"
    )
    db.execute(
        "UPDATE dimensions.dim_party p SET party_name=g.party_name,party_code=g.party_code\n      FROM global_procurer_labels g WHERE p.party_key=g.party_key"
    )
    db.execute(
        "CREATE OR REPLACE TEMP TABLE global_investor_labels AS\n"
        "  WITH ranked_notice AS (\n"
        "    SELECT *,row_number() OVER (\n"
        "      PARTITION BY coalesce(source_family,'VN'),coalesce(notify_no,entity_id)\n"
        "      ORDER BY coalesce(try_cast(regexp_extract(coalesce(notify_version,'0'),'([0-9]+)',1) AS BIGINT),0) DESC,\n"
        "        published_at DESC NULLS LAST,public_date DESC NULLS LAST,revision_id DESC) rn\n"
        "    FROM frozen.notice_revision\n"
        "  ), ranked_plan AS (\n"
        "    SELECT *,row_number() OVER (\n"
        "      PARTITION BY entity_id\n"
        "      ORDER BY public_date DESC NULLS LAST,revision_id DESC) rn\n"
        "    FROM frozen.plan_revision\n"
        "  ), candidates AS (\n"
        "    SELECT\n"
        "      CASE\n"
        "        WHEN nullif(trim(investor_code),'') IS NOT NULL THEN 'PARTY_INVESTOR:' || trim(investor_code)\n"
        "        ELSE 'PARTY_INVESTOR_NAME:' || md5(upper(trim(investor_name)))\n"
        "      END AS party_key,\n"
        "      nullif(trim(investor_code),'') AS party_code,\n"
        "      investor_name AS party_name,\n"
        "      'silver.notice_revision' AS source_namespace\n"
        "    FROM ranked_notice\n"
        "    WHERE rn=1 AND coalesce(nullif(trim(investor_code),''),nullif(trim(investor_name),'')) IS NOT NULL\n"
        "    UNION ALL\n"
        "    SELECT\n"
        "      'PARTY_INVESTOR_NAME:' || md5(upper(trim(investor_name))) AS party_key,\n"
        "      NULL AS party_code,\n"
        "      investor_name AS party_name,\n"
        "      'silver.plan_revision' AS source_namespace\n"
        "    FROM ranked_plan\n"
        "    WHERE rn=1 AND nullif(trim(investor_name),'') IS NOT NULL\n"
        "  )\n"
        "  SELECT\n"
        "    party_key,\n"
        "    first(party_name ORDER BY source_namespace, party_name NULLS LAST, party_code NULLS LAST) AS party_name,\n"
        "    first(party_code ORDER BY source_namespace, party_name NULLS LAST, party_code NULLS LAST) AS party_code,\n"
        "    first(source_namespace ORDER BY source_namespace, party_name NULLS LAST, party_code NULLS LAST) AS source_namespace\n"
        "  FROM candidates\n"
        "  GROUP BY party_key"
    )
    db.execute(
        "UPDATE dimensions.dim_party p SET party_name=g.party_name,party_code=g.party_code,source_namespace=g.source_namespace\n"
        "  FROM global_investor_labels g WHERE p.party_key=g.party_key"
    )
