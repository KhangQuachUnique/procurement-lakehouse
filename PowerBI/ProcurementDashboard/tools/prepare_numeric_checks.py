"""Independent SQL baselines for supported Gold V3 measures and edge cases."""
import json
from decimal import Decimal
from profile_sources import PROJECT, WORKSPACE, discover, connection, save


def main():
    con = connection(discover(WORKSPACE/'PowerBI/export/parquet'))
    queries, expected = [], []
    metrics = {
        'planned': ('Kế hoạch VND', 'fact_demand_daily', 'sum(planned_amount_sum)', 'money'),
        'packages': ('Số gói kế hoạch', 'fact_demand_daily', 'sum(package_count)', 'count'),
        'award': ('Trúng thầu VND', 'fact_award_daily', 'sum(award_amount_sum)', 'money'),
        'award_scopes': ('Lượt kết quả', 'fact_award_daily', 'sum(award_scope_count)', 'count'),
        'baseline': ('Dự toán mô tả VND', 'fact_savings_daily', 'CASE WHEN sum(descriptive_sample_count)>0 THEN sum(descriptive_baseline_sum) END', 'money'),
        'difference': ('Chênh lệch mô tả VND', 'fact_savings_daily', 'CASE WHEN sum(descriptive_sample_count)>0 THEN sum(descriptive_difference_sum) END', 'money'),
        'difference_rate': ('Tỷ lệ chênh lệch mô tả', 'fact_savings_daily', 'sum(descriptive_difference_sum)/nullif(sum(descriptive_baseline_sum),0)', 'ratio'),
        'verified_samples': ('Mẫu xác minh', 'fact_savings_daily', 'sum(verified_sample_count)', 'count'),
        'contract_samples': ('Mẫu hợp đồng', 'fact_savings_daily', 'sum(contract_sample_count)', 'count'),
        'bidders_avg': ('Nhà thầu bình quân', 'fact_bidding_competition_daily', 'sum(total_bidders)::DOUBLE/nullif(sum(opening_count),0)', 'ratio'),
        'single_rate': ('Tỷ lệ một nhà thầu', 'fact_bidding_competition_daily', 'sum(single_bidder_count)::DOUBLE/nullif(sum(opening_count),0)', 'ratio'),
        'discount_count_rate': ('Tỷ lệ lượt có giảm giá', 'fact_bidding_competition_daily', 'sum(discounted_bidder_count)::DOUBLE/nullif(sum(total_bidders),0)', 'ratio'),
        'technical_issues': ('Nhóm kỹ thuật cần xác minh', 'fact_bidding_competition_daily', 'count(*) filter(where tech_passed_count+tech_failed_count>total_bidders)', 'count'),
    }
    scopes = [
        ('all', 'TRUE', []),
        ('year2025', 'year(report_date)=2025', ['dim_date[year]=2025']),
        ('jan2025_hh', "report_date between date '2025-01-01' and date '2025-01-31' AND sector_key='SEC_HH'", ['dim_date[year_month]="2025-01"', 'dim_sector[sector_key]="SEC_HH"']),
    ]
    def record(name, dax, sql=None, value=None, kind='ratio'):
        if sql is not None:
            value=con.execute(sql).fetchone()[0]
        tolerance = Decimal(0) if kind=='count' else max(Decimal(1) if kind=='money' else Decimal('1e-9'), abs(Decimal(str(value or 0)))*Decimal('1e-12'))
        queries.append({'name':name, 'dax':'EVALUATE ROW("value",'+dax+')'})
        expected.append({'name':name, 'sql':sql, 'value':None if value is None else str(value), 'absolute_tolerance':str(tolerance)})
    for scope, where, filters in scopes:
        for key, (measure, fact, expr, kind) in metrics.items():
            dax='['+measure+']'
            if filters: dax='CALCULATE('+dax+','+','.join(filters)+')'
            record(scope+'/'+key,dax,f'SELECT {expr} FROM {fact} WHERE {where}',kind=kind)
        for metric in ['plan_to_notice_days','notice_to_close_hours','close_to_first_award_days','close_to_open_hours','open_to_award_days']:
            dax='CALCULATE([Thời gian bình quân],dim_duration_metric[metric_key]="'+metric+'"'+(','+','.join(filters) if filters else '')+')'
            record(scope+'/'+metric,dax,f"SELECT sum(duration_sum)/nullif(sum(sample_count),0) FROM fact_duration_daily WHERE metric_key='{metric}' AND ({where})")
        supplier_where="currency_key='VND' AND recipient_allocation_state='single_recipient' AND eligible_amount_count>0 AND party_key IN (SELECT party_key FROM dim_party WHERE party_kind<>'UNKNOWN')"
        for name, measure, expr, kind in [('supplier','Người nhận · tỷ VND','sum(award_amount_sum)/1000000000','ratio'),('people','Số khóa người nhận','count(distinct party_key)','count')]:
            dax='['+measure+']'
            if filters:dax='CALCULATE('+dax+','+','.join(filters)+')'
            record(scope+'/'+name,dax,f'SELECT {expr} FROM fact_supplier_daily WHERE {supplier_where} AND ({where})',kind=kind)
    for name,dax in [
        ('duration_no_metric','[Thời gian bình quân]'),
        ('duration_multi','CALCULATE([Thời gian bình quân],dim_duration_metric[metric_key] IN {"plan_to_notice_days","notice_to_close_hours"})'),
        ('empty_month','CALCULATE([Tỷ lệ một nhà thầu],dim_date[year_month]="2022-01")'),
        ('usd','CALCULATE([Trúng thầu VND],dim_currency[currency_code]="USD")'),
        ('unknown_currency','CALCULATE([Trúng thầu VND],dim_currency[currency_code]="UNKNOWN")'),
        ('empty_money','CALCULATE([Chênh lệch mô tả VND],dim_date[year_month]="2022-01")'),
    ]: record(name,dax)
    inventory=json.loads((PROJECT/'validation/model_inventory.json').read_text(encoding='utf-8'))
    for name in inventory['measures']:
        queries.append({'name':'compile/'+name,'dax':'EVALUATE ROW("value",['+name+'])'})
    for table in inventory['tables']:
        condition=" WHERE date_key BETWEEN DATE '2022-01-01' AND DATE '2025-12-31'" if table=='dim_date' else ''
        record('rowcount/'+table,'COUNTROWS('+table+')','SELECT count(*) FROM '+table+condition,kind='count')
    # Independent Top-10 comparison and denominator scope at the default year.
    for year in [2023,2025]:
        sql=f"""WITH parties AS (SELECT party_key,sum(award_amount_sum)/1000000000 AS v FROM fact_supplier_daily WHERE {supplier_where} AND year(report_date)={year} GROUP BY 1), top_people AS (SELECT * FROM parties ORDER BY v DESC,party_key LIMIT 10) SELECT (SELECT sum(v) FROM top_people)/(SELECT sum(v) FROM parties)"""
        record('top10/'+str(year),f'CALCULATE([Tỷ trọng Top 10 người nhận],dim_date[year]={year})',sql)
    save(PROJECT/'validation/numeric_expected.json',expected)
    save(PROJECT/'validation/dax_queries.json',queries)
    print(f'Prepared {len(queries)} queries and {len(expected)} SQL baselines')

if __name__=='__main__': main()
