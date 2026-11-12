"""Independent DuckDB baselines and matching executable DAX queries, including edge cases."""
from decimal import Decimal
from profile_sources import PROJECT, WORKSPACE, discover, connection, records, save

def main():
    con=connection(discover(WORKSPACE/'PowerBI/export/parquet'))
    queries=[]; expected=[]
    metrics={
        'planned':('Giá trị kế hoạch VNĐ','fact_demand_daily','sum(planned_amount_sum)','currency_key=\'VND\''),
        'award':('Giá trị trúng thầu VNĐ','fact_award_daily','sum(award_amount_sum)','currency_key=\'VND\''),
        'saving':('Tiết kiệm xác minh VNĐ','fact_savings_daily','CASE WHEN sum(verified_sample_count)>0 THEN sum(verified_saving_sum) END','currency_key=\'VND\''),
        'baseline':('Dự toán xác minh VNĐ','fact_savings_daily','CASE WHEN sum(verified_sample_count)>0 THEN sum(verified_baseline_sum) END','currency_key=\'VND\''),
        'saving_rate':('Tỷ lệ tiết kiệm xác minh','fact_savings_daily','sum(verified_saving_sum)/nullif(sum(verified_baseline_sum),0)','currency_key=\'VND\''),
        'bidders_avg':('Nhà thầu bình quân','fact_bidding_competition_daily','sum(total_bidders)::DOUBLE/nullif(sum(opening_count),0)','TRUE'),
        'single_rate':('Tỷ lệ một nhà thầu','fact_bidding_competition_daily','sum(single_bidder_count)::DOUBLE/nullif(sum(opening_count),0)','TRUE'),
        'technical_rate':('Tỷ lệ đạt kỹ thuật','fact_bidding_competition_daily','sum(tech_passed_count)::DOUBLE/nullif(sum(tech_passed_count)+sum(tech_failed_count),0)','TRUE'),
        'supplier':('Giá trị phân bổ đơn VNĐ','fact_supplier_daily','sum(award_amount_sum)',"currency_key='VND' AND recipient_allocation_state='single_recipient'"),
    }
    for scope,sql_slice,dax_filters in [
        ('all','TRUE',[]),
        ('jan2025_hh',"report_date BETWEEN DATE '2025-01-01' AND DATE '2025-01-31' AND sector_key='SEC_HH'",['dim_date[year_month]="2025-01"','dim_sector[sector_key]="SEC_HH"']),
    ]:
        for key,(measure,table,expr,where) in metrics.items():
            name=scope+'/'+key
            sql=f'SELECT {expr} AS value FROM {table} WHERE {where} AND ({sql_slice})'
            value=con.execute(sql).fetchone()[0]
            if dax_filters:
                dax='CALCULATE(['+measure+'], '+', '.join(dax_filters)+')'
            else: dax='['+measure+']'
            queries.append({'name':name,'dax':'EVALUATE ROW("value", '+dax+')'})
            expected.append({'name':name,'sql':sql,'value':None if value is None else str(value),'absolute_tolerance': '5' if key in ['planned','award','saving','baseline','supplier'] else '0.0000000001'})
    for metric in ['plan_to_notice_days','notice_to_close_hours']:
        for scope,sql_slice,dax_filters in [('all','TRUE',[]),('jan2025_hh',"report_date BETWEEN DATE '2025-01-01' AND DATE '2025-01-31' AND sector_key='SEC_HH'",['dim_date[year_month]="2025-01"','dim_sector[sector_key]="SEC_HH"'])]:
            sql=f"SELECT sum(duration_sum)/nullif(sum(sample_count),0) FROM fact_duration_daily WHERE report_date IS NOT NULL AND metric_key='{metric}' AND ({sql_slice})"
            value=con.execute(sql).fetchone()[0]
            name=scope+'/'+metric
            dax='EVALUATE ROW("value",CALCULATE([Thời gian trung bình],dim_duration_metric[metric_key]="'+metric+'"'+(' , '+', '.join(dax_filters) if dax_filters else '')+'))'
            queries.append({'name':name,'dax':dax})
            expected.append({'name':name,'sql':sql,'value':None if value is None else str(value),'absolute_tolerance':'0.000000001'})
    edges=[
        ('duration_no_metric','[Thời gian trung bình]',None),
        ('duration_zero_samples','CALCULATE([Thời gian trung bình],dim_duration_metric[metric_key]="open_to_award_days")',None),
        ('duration_multi_metric','CALCULATE([Thời gian trung bình], dim_duration_metric[metric_key] IN {"open_to_award_days","close_to_open_hours"})',None),
        ('competition_empty','CALCULATE([Tỷ lệ một nhà thầu], dim_date[year]=2020)',None),
        ('vnd_with_usd_filter','CALCULATE([Giá trị trúng thầu VNĐ],dim_currency[currency_code]="USD")',None),
        ('vnd_with_unknown_filter','CALCULATE([Giá trị trúng thầu VNĐ],dim_currency[currency_code]="UNKNOWN")',None),
        ('zero_denominator','DIVIDE(1,0)',None),
    ]
    for name,dax,value in edges:
        queries.append({'name':name,'dax':'EVALUATE ROW("value",'+dax+')'})
        expected.append({'name':name,'value':value,'absolute_tolerance':'0'})
    # Evaluate every measure to surface expression errors, even those not used on a page.
    import json
    inventory=json.loads((PROJECT/'validation/model_inventory.json').read_text(encoding='utf-8'))
    for name in inventory['measures']:
        queries.append({'name':'compile/'+name,'dax':'EVALUATE ROW("value",['+name+'])'})
    for table in inventory['tables']:
        queries.append({'name':'rowcount/'+table,'dax':'EVALUATE ROW("value",COUNTROWS('+table+'))'})
        expected.append({'name':'rowcount/'+table,'value':str(con.execute('SELECT count(*) FROM '+table).fetchone()[0]),'absolute_tolerance':'0'})
    date_issue=records(con,"SELECT count(*) AS undated_rows,count(DISTINCT group_key) AS distinct_groups,sum(sample_count) AS valid_samples,sum(duration_sum) AS duration_sum FROM fact_duration_daily WHERE report_date IS NULL")[0]
    dated=records(con,"SELECT count(*) AS dated_rows,count(DISTINCT group_key) AS distinct_groups FROM fact_duration_daily WHERE report_date IS NOT NULL")[0]
    if date_issue['valid_samples']!=0 or date_issue['duration_sum']!=0 or dated['dated_rows']!=dated['distinct_groups']:
        raise ValueError('Duration policy invalid: investigate new source data before proceeding')
    save(PROJECT/'validation/source_issue_policy.json',{
        'status':'source_errors_preserved_with_explicit_analytic_boundary',
        'undated_duration':date_issue,'dated_duration':dated,
        'decision':'Import all rows unchanged. Keep group_key hidden; never use it as relationship key. Duration measures explicitly require a non-null report_date and one metric. Undated excluded counts are ambiguous because groups repeat; do not present them as unique sample counts.',
        'source_repair_required':'Re-export duration with an unambiguous grain for missing-date observations; upstream snapshot provenance must be supplied to prove a common release.'})
    save(PROJECT/'validation/numeric_expected.json',expected)
    save(PROJECT/'validation/dax_queries.json',queries)
    print(f'Prepared {len(queries)} engine queries, {len(expected)} independently expected results.')

if __name__=='__main__':main()
