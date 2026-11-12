import json
from pathlib import Path

d = Path(__file__).resolve().parents[1] / 'validation/contracts'
index = json.loads((d/'index.json').read_text())
for u,f in index.items():
    j=json.loads((d/f).read_text())
    if any(s in u for s in ['visualConfiguration/','page/2.','filterConfiguration/','versionMetadata/']):
        print(u, 'required',j.get('required'),'properties',list(j.get('properties',{})))
        names = ['VisualQuery','QueryState','Projection','SortDefinition','VisualInteraction','VisualInteractions','PageFormattingObjects','FilterContainer','Filter','SyncGroup','VisualQueryState','QueryProjection']
        for k,v in j.get('definitions',{}).items():
            if k in names or any(s in k for s in ['Interaction','Slicer','Sync']):
                print(k, json.dumps(v)[:7000])
