from profile_sources import PROJECT, WORKSPACE, connection, discover, records, save
import json

def main():
    db = connection(discover(WORKSPACE/'PowerBI/export/parquet'))
    schema = json.loads((WORKSPACE/'src/procurement/gold/domain/schema.json').read_text())
    queries = {
        'competition': """SELECT sector_name, min(report_date) first_day, max(report_date) last_day,
            sum(opening_count) openings, sum(single_bidder_count) single_openings,
            sum(single_bidder_count)/sum(opening_count) single_rate,
            sum(opening_count-single_bidder_count-multi_bidder_count) zero_or_unknown
            FROM fact_bidding_competition_daily JOIN dim_sector USING(sector_key) GROUP BY 1 ORDER BY openings DESC""",
        'band_shares': """SELECT band_label, sum(package_count) packages, sum(eligible_amount_count) priced,
            sum(package_count)/sum(sum(package_count)) over() package_share,
            sum(planned_amount_sum)/sum(sum(planned_amount_sum)) over() value_share
            FROM fact_demand_daily f JOIN dim_value_band ON value_band_key=band_key
            WHERE f.currency_key='VND' GROUP BY 1 ORDER BY value_share DESC""",
        'difference': """SELECT sector_name,sum(descriptive_sample_count) samples,
            sum(descriptive_difference_sum)/nullif(sum(descriptive_baseline_sum),0) rate
            FROM fact_savings_daily JOIN dim_sector USING(sector_key) WHERE currency_key='VND'
            GROUP BY 1 ORDER BY samples DESC""",
        'duration': """SELECT metric_key,sum(sample_count) samples,sum(excluded_sample_count) exclusions,
            sum(duration_sum)/nullif(sum(sample_count),0) mean FROM fact_duration_daily GROUP BY 1 ORDER BY 1""",
        'supplier': """WITH people AS (SELECT sector_key,party_key,sum(award_amount_sum) amount,
            sum(eligible_amount_count) samples FROM fact_supplier_daily JOIN dim_party USING(party_key)
            WHERE currency_key='VND' AND recipient_allocation_state='single_recipient'
            AND eligible_amount_count>0 AND party_kind<>'UNKNOWN' GROUP BY 1,2),
            ranked AS (SELECT *,row_number() over(partition by sector_key order by amount desc,party_key) r FROM people)
            SELECT sector_name,count(*) identities,sum(samples) samples,sum(amount) FILTER(WHERE r<=10)/nullif(sum(amount),0) cr10
            FROM ranked JOIN dim_sector USING(sector_key) GROUP BY 1 ORDER BY samples DESC""",
        'mixed_eligibility': """SELECT count(*) mixed_groups FROM fact_supplier_daily WHERE
            recipient_allocation_state='single_recipient' AND eligible_amount_count BETWEEN 1 AND award_scope_count-1""",
    }
    for name, columns in schema.items():
        relation = db.sql('select * from '+name+' limit 0')
        actual = list(zip(relation.columns, map(str, relation.types)))
        if actual != [(c['name'], c['type']) for c in columns]:
            raise ValueError('Gold/export column mismatch in '+name)
    counts = {'demand':'package_count','award':'award_scope_count','notice':'notice_count',
              'savings':'scope_count','bidding_competition':'opening_count','duration':'interval_count',
              'supplier':'award_scope_count','status':'package_count'}
    for fact, column in counts.items():
        if fact == 'status':
            # Retain the last observed snapshot per month when inspecting status volume.
            source = "(SELECT * FROM fact_status_daily QUALIFY report_date=max(report_date) OVER (PARTITION BY date_trunc('month',report_date)))"
        else:
            source = f'fact_{fact}_daily'
        queries[f'months_{fact}'] = f"SELECT date_trunc('month',report_date) AS month, sum({column}) AS volume FROM {source} GROUP BY 1 ORDER BY 1"
    queries['award_price_coverage'] = "SELECT currency_key,sum(effective_award_scope_count) effective,sum(eligible_amount_count) priced,sum(award_amount_sum) amount FROM fact_award_daily GROUP BY 1"
    queries['geography_allocation'] = "SELECT is_multi_geography,is_unknown,sum(eligible_amount_count) samples,sum(award_amount_sum) amount FROM fact_award_daily JOIN dim_geography USING(geography_key) WHERE currency_key='VND' GROUP BY ALL"
    queries['negative_difference'] = "SELECT count(*) negative_daily_groups,sum(descriptive_sample_count) samples_in_these_groups,sum(descriptive_difference_sum) difference FROM fact_savings_daily WHERE currency_key='VND' AND descriptive_difference_sum<0"
    queries['largest_planned_groups'] = '''SELECT group_key,report_date,sector_key,geography_key,
        value_band_key,package_count,eligible_amount_count,planned_amount_sum
        FROM fact_demand_daily WHERE currency_key='VND' ORDER BY planned_amount_sum DESC,group_key LIMIT 10'''
    queries['quarterly_market'] = '''SELECT date_trunc('quarter',report_date) AS quarter,
        sum(package_count) packages,sum(eligible_amount_count) priced,sum(planned_amount_sum) amount_vnd
        FROM fact_demand_daily WHERE currency_key='VND' GROUP BY 1 ORDER BY 1'''
    queries['yearly_awards'] = '''SELECT year(report_date) AS year,currency_key,sum(award_scope_count) scopes,
        sum(eligible_amount_count) priced,sum(award_amount_sum) amount
        FROM fact_award_daily GROUP BY 1,2 ORDER BY 1,2'''
    result = {key:{'sql':sql,'rows':records(db,sql)} for key,sql in queries.items()}
    save(PROJECT/'validation/redesign_insights.json',result)

if __name__ == '__main__':
    main()
