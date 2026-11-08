SELECT owner_key,report_date,status_key,sector_key,package_count FROM (SELECT package_key AS owner_key,
package_published_date_key AS report_date,
'STATUS_' || sha256(to_json(list_value(notice_observation_state,award_observation_state))) AS status_key,
sector_key AS sector_key,
CAST(coalesce((1),0) AS BIGINT) AS package_count FROM business.fact_procurement_demand WHERE (is_in_event_period OR package_published_date_key IS NULL)) projected
