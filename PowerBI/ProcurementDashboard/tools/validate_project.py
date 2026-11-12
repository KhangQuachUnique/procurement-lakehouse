"""Validate official JSON contracts plus references, geometry, roles and interaction boundaries."""
import itertools
import json
from pathlib import Path
from urllib.parse import urldefrag
import jsonschema
from referencing import Registry, Resource
from profile_sources import PROJECT, save

def main():
    contracts=PROJECT/'validation/contracts'
    index=json.loads((contracts/'index.json').read_text())
    resources={u:json.loads((contracts/f).read_text()) for u,f in index.items()}
    registry=Registry().with_resources((u,Resource.from_contents(j)) for u,j in resources.items())
    inventory=json.loads((PROJECT/'validation/model_inventory.json').read_text(encoding='utf-8'))
    roles={'card':{'Values'},'cardVisual':{'Data'},'lineChart':{'Category','Y','Y2','Series','Tooltips','Rows'},
        'clusteredBarChart':{'Category','Y','Series','Tooltips'},'clusteredColumnChart':{'Category','Y','Series','Tooltips'},
        'scatterChart':{'Category','X','Y','Size','Series','Tooltips'},'pivotTable':{'Rows','Columns','Values','Tooltips'},
        'tableEx':{'Values','Tooltips'},'slicer':{'Values'},'textbox':set(),'pageNavigator':set()}
    errors=[]; checks=[]; visuals=0
    paths=[PROJECT/'ProcurementDashboard.pbip']
    for item in ['ProcurementDashboard.Report','ProcurementDashboard.SemanticModel']:
        paths += [p for p in (PROJECT/item).rglob('*') if p.is_file() and '.pbi' not in p.parts and (p.suffix in ['.json','.pbir','.pbism'] or p.name=='.platform')]
    for p in paths:
        j=json.loads(p.read_text(encoding='utf-8'))
        try:
            if '$schema' in j:
                schema=resources[j['$schema']]
                validator=jsonschema.validators.validator_for(schema)(schema,registry=registry)
                errors += [f'{p.relative_to(PROJECT)} {list(e.path)}: {e.message}' for e in validator.iter_errors(j)]
                checks.append(str(p.relative_to(PROJECT)))
        except Exception as e:
            errors.append(f'{p}: {e}')
        if p.name!='visual.json': continue
        visuals+=1
        visual=j['visual']; typ=visual['visualType']
        q=visual.get('query',{}).get('queryState',{})
        if set(q)-roles.get(typ,set()): errors.append(f'{p}: invalid roles {set(q)} for {typ}')
        def walk(x):
            if isinstance(x,dict):
                for kind in ['Column','Measure']:
                    if kind not in x: continue
                    f=x[kind]; src=f.get('Expression',{}).get('SourceRef',{})
                    if 'Entity' not in src: continue
                    table=src['Entity']; prop=f['Property']
                    valid=prop in inventory['measures'] if kind=='Measure' and table=='_Measures' else prop in inventory['tables'].get(table,{}).get('columns',{})
                    if not valid: errors.append(f'{p}: missing {kind} {table}.{prop}')
                for v in x.values(): walk(v)
            elif isinstance(x,list):
                for v in x: walk(v)
        walk(j)
        pos=j['position']
        if min(pos['x'],pos['y'])<0 or pos['x']+pos['width']>1280 or pos['y']+pos['height']>720: errors.append(f'{p}: canvas overflow')
    theme_schema=json.loads((contracts/'theme-schema.json').read_text(encoding='utf-8-sig'))
    theme=json.loads((PROJECT/'theme.json').read_text(encoding='utf-8'))
    errors += [f'Theme {list(e.path)}: {e.message}' for e in jsonschema.Draft7Validator(theme_schema).iter_errors(theme)]
    pages=PROJECT/'ProcurementDashboard.Report/definition/pages'
    order=json.loads((pages/'pages.json').read_text())['pageOrder']
    for page in order:
        folder=pages/page
        docs=[json.loads(p.read_text(encoding='utf-8')) for p in (folder/'visuals').rglob('visual.json')]
        for a,b in itertools.combinations(docs,2):
            x,y=a['position'],b['position']
            if min(x['x']+x['width'],y['x']+y['width'])>max(x['x'],y['x']) and min(x['y']+x['height'],y['y']+y['height'])>max(x['y'],y['y']):
                errors.append(f'{page}: overlapping {a["name"]} and {b["name"]}')
        pd=json.loads((folder/'page.json').read_text(encoding='utf-8'))
        names={v['name'] for v in docs}
        if len(names)!=len(docs): errors.append(f'{page}: duplicate visual IDs')
        slicers=[v for v in docs if v['visual']['visualType']=='slicer']
        expected={'dim_date.year_month','dim_sector.sector_name'}
        actual={v['visual']['query']['queryState']['Values']['projections'][0]['queryRef'] for v in slicers}
        if expected-actual: errors.append(f'{page}: missing required slicers {expected-actual}')
        if any(r['source'] not in names or r['target'] not in names for r in pd['visualInteractions']): errors.append(f'{page}: missing interaction target')
        for v in docs:
            if v['visual']['visualType']=='slicer' and v['visual']['query']['queryState']['Values']['projections'][0]['queryRef'].startswith('dim_duration_metric.'):
                for target in docs:
                    if target['visual']['visualType']=='tableEx':
                        rule=next(r for r in pd['visualInteractions'] if r['source']==v['name'] and r['target']==target['name'])
                        if rule['type']!='NoFilter': errors.append('Metric must not filter supplier table')
    assert len(order)==4
    outcome={'status':'passed' if not errors else 'failed','json_files_checked':len(checks),'visuals_checked':visuals,'pages':order,'errors':errors,
        'limits':['Static validation is not a Desktop render or refresh test.','TMDL parsing and engine execution recorded separately.']}
    save(PROJECT/'validation/static_validation.json',outcome)
    print(json.dumps(outcome,ensure_ascii=False,indent=2))
    if errors: raise SystemExit(1)

if __name__=='__main__': main()
