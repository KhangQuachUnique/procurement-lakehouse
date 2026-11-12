import json
from profile_sources import PROJECT, records, connection, discover, WORKSPACE

con = connection(discover(WORKSPACE / 'PowerBI/export/parquet'))
print(records(con, 'SELECT report_date IS NULL AS missing_date,count(*) AS rows,count(DISTINCT group_key) AS keys,sum(sample_count) AS samples,sum(excluded_sample_count) AS excluded FROM fact_duration_daily GROUP BY ALL'))
print(records(con, 'SELECT report_date,sector_key,method_key,metric_key,count(*) AS n,count(DISTINCT group_key) AS keys,sum(sample_count) AS samples FROM fact_duration_daily GROUP BY ALL HAVING count(*)>1 ORDER BY n DESC LIMIT 8'))
print(records(con, 'SELECT metric_key, count(*) AS rows,sum(sample_count) AS samples,sum(excluded_sample_count) AS excluded FROM fact_duration_daily WHERE report_date IS NULL GROUP BY ALL'))
p = json.loads((PROJECT / 'validation/source_profile.json').read_text(encoding='utf-8'))
print(json.dumps(p['sample_dimensions'], ensure_ascii=False))
print(p['reconciliations'])
print([(n,t['column_profile'].get('report_date__min'),t['column_profile'].get('report_date__max')) for n,t in p['tables'].items() if n.startswith('fact_')])
