"""Exercise multi-part discovery and schema failures in isolated, disposable fixtures."""
import tempfile
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from profile_sources import PROJECT, WORKSPACE, NAMES, discover, save

def main():
    source=discover(WORKSPACE/'PowerBI/export/parquet')
    outcomes=[]
    fixture_parent=(PROJECT/'validation').resolve()
    with tempfile.TemporaryDirectory(prefix='layout-check-',dir=fixture_parent) as temp:
        root=Path(temp).resolve()
        if root.parent!=fixture_parent:
            raise RuntimeError('Fixture root escaped project validation directory')
        for name,files in source.items():
            pq.write_table(pq.read_schema(files[0]).empty_table(),root/(name+'.parquet'))
        result=discover(root)
        assert len(result)==17 and all(len(v)==1 for v in result.values())
        outcomes.append({'scenario':'17 independent single-file logical tables','status':'passed'})
        flat=root/'dim_date.parquet'
        schema=pq.read_schema(flat)
        folder=root/'dim_date'/'partition=2025';folder.mkdir(parents=True)
        pq.write_table(schema.empty_table(),folder/'part-000.parquet')
        pq.write_table(schema.empty_table(),folder/'part-001.parquet')
        flat.unlink()
        assert len(discover(root)['dim_date'])==2
        outcomes.append({'scenario':'nested multi-part table, no assumed part filename','status':'passed'})
        bad=folder/'part-invalid.parquet'
        pq.write_table(pa.table({'unexpected':[1]}),bad)
        try: discover(root)
        except ValueError as e:
            assert 'Schema mismatch' in str(e)
        else:raise AssertionError('Schema mismatch accepted')
        bad.unlink()
        outcomes.append({'scenario':'mixed schema parts rejected','status':'passed'})
        unknown=root/'unmapped.parquet';pq.write_table(schema.empty_table(),unknown)
        try:discover(root)
        except ValueError as e:assert 'Unmapped/ambiguous' in str(e)
        else:raise AssertionError('Unmapped source accepted')
        unknown.unlink()
        outcomes.append({'scenario':'unmapped layout rejected','status':'passed'})
        (root/'dim_currency.parquet').unlink()
        try:discover(root)
        except ValueError as e:assert 'Missing logical tables' in str(e)
        else:raise AssertionError('Missing table accepted')
        outcomes.append({'scenario':'missing logical table rejected','status':'passed'})
    save(PROJECT/'validation/layout_validation.json',{'status':'passed','checks':outcomes,'limits':'Tests Python discovery. Actual Power Query multi-part refresh must be exercised independently; production source currently has one part per logical table.'})
    print('Layout checks passed:',len(outcomes))

if __name__=='__main__':main()
