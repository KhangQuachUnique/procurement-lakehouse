"""Independently reconcile Top 10 entity/geography identities and monetary values."""
import json
from profile_sources import PROJECT, WORKSPACE, discover, connection, records, save

con=connection(discover(WORKSPACE/'PowerBI/export/parquet'))
queries=json.loads((PROJECT/'validation/dax_queries.json').read_text(encoding='utf-8'))
cases=[]
for name,dim,display,fact,key,measure,extra in [
    ('top10_geography','dim_geography','province_display','fact_award_daily','geography_key','Giá trị trúng thầu VNĐ',''),
    ('top10_supplier','dim_party','party_display','fact_supplier_daily','party_key','Giá trị phân bổ đơn VNĐ'," AND f.recipient_allocation_state='single_recipient'"),
]:
    label="concat_ws(' · ',d.province_name,d.geography_version,d.province_code)" if name=='top10_geography' else "CASE WHEN d.party_name IS NULL OR trim(d.party_name)='' THEN 'Chưa có tên' ELSE d.party_name END || ' [' || d.party_key || ']'"
    sql=f"SELECT {label} AS label,sum(f.award_amount_sum) AS amount FROM {fact} f JOIN {dim} d USING({key}) WHERE f.currency_key='VND'{extra} GROUP BY ALL ORDER BY amount DESC,label ASC LIMIT 10"
    rows=records(con,sql)
    dax=f'EVALUATE TOPN(10,FILTER(SUMMARIZECOLUMNS({dim}[{display}],"amount",[{measure}]),NOT ISBLANK([amount])),[amount],DESC,{dim}[{display}],ASC) ORDER BY [amount] DESC,{dim}[{display}] ASC'
    cases.append({'name':name,'group_column':display,'sql':sql,'expected':rows,'absolute_tolerance':'5'})
    queries=[q for q in queries if q['name']!=name]
    queries.append({'name':name,'dax':dax})
save(PROJECT/'validation/group_expected.json',cases)
save(PROJECT/'validation/dax_queries.json',queries)
print('Prepared Top 10 identity/value comparisons:',len(cases))
