from decimal import Decimal
from profile_sources import PROJECT, WORKSPACE, discover, connection, save
import pyarrow.parquet as pq

files=discover(WORKSPACE/'PowerBI/export/parquet')
con=connection(files)
results=[]
for table,parts in files.items():
    for col in pq.read_schema(parts[0]):
        if not str(col.type).startswith('decimal'):continue
        exact,approx,max_value,min_value=con.execute(f'SELECT sum("{col.name}"),sum(CAST("{col.name}" AS DOUBLE)),max("{col.name}"),min("{col.name}") FROM {table}').fetchone()
        delta=None if exact is None else abs(Decimal(str(approx))-exact)
        results.append({'table':table,'column':col.name,'exact_decimal_sum':exact,'duckdb_double_sum':approx,'difference_in_source_units':delta,'max_value':max_value,'min_value':min_value})
save(PROJECT/'validation/precision_assessment.json',{'model_type':'double','reason':'Retain source six-decimal scale approximately without fixed-decimal four-place truncation or fixed-decimal aggregate overflow; money is displayed in billions with two decimals. Raw parquet remains exact.','limits':'IEEE 754 binary64 is approximate, about 15 significant decimal digits. Compare live DAX totals with exact SQL separately. Not an accounting ledger.','columns':results})
print('Precision profiles:',len(results))
