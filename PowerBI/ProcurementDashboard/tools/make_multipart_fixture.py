import json
import uuid
import pyarrow.parquet as pq
from profile_sources import PROJECT, WORKSPACE, save

root=(PROJECT/'validation'/('multipart-fixture-'+uuid.uuid4().hex[:8])).resolve()
assert root.parent==(PROJECT/'validation').resolve()
folder=root/'dim_date'/'nested';folder.mkdir(parents=True)
table=pq.read_table(WORKSPACE/'PowerBI/export/parquet/dim_date.parquet')
cut=len(table)//2
pq.write_table(table.slice(0,cut),folder/'first.parquet')
pq.write_table(table.slice(cut),folder/'second.parquet')
save(PROJECT/'validation/multipart_fixture.json',{'root':str(root),'rows':len(table),'parts':2})
print(root)
