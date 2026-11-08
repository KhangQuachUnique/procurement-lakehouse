SELECT owner_key,report_date,geography_key,sector_key,value_band_key,currency_key,package_count,eligible_amount_count,planned_amount_sum FROM (SELECT package_key AS owner_key,
package_published_date_key AS report_date,
reporting_geography_key AS geography_key,
sector_key AS sector_key,
package_value_band_key AS value_band_key,
currency_key AS currency_key,
CAST(coalesce((1),0) AS BIGINT) AS package_count,
CAST(coalesce((CASE WHEN is_amount_eligible THEN 1 ELSE 0 END),0) AS BIGINT) AS eligible_amount_count,
CAST(coalesce((CASE WHEN is_amount_eligible THEN planned_package_amount ELSE 0 END),0) AS DECIMAL(38,6)) AS planned_amount_sum FROM business.fact_procurement_demand WHERE (is_in_event_period OR package_published_date_key IS NULL)) projected
