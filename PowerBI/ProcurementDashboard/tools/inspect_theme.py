import json
from pathlib import Path
p=Path(__file__).resolve().parents[1]/'validation/contracts/theme-schema.json'
j=json.loads(p.read_text(encoding='utf-8-sig'))
for name in ['visual-cardVisual','visual-card','visual-pageNavigator','visual-lineChart','visual-clusteredBarChart']:
    for block in j['definitions'][name].get('allOf',[]):
        for prop,v in block.get('properties',{}).items():
            if name in ['visual-cardVisual','visual-pageNavigator','visual-card']:
                print(name,prop, list(v.get('items',{}).get('properties',{})))
            elif prop in ['categoryAxis','valueAxis','layout','labels']:
                print(name,prop,json.dumps(v)[:6500])
def walk(x,path=''):
    if isinstance(x,dict):
        for k,v in x.items():
            if k in ['cardVisual','card','pageNavigator']:
                print(path+'/'+k,json.dumps(v,ensure_ascii=False)[:22000])
            if k=='definitions':
                for d,dv in v.items():
                    if any(s in d.lower() for s in ['cardvisual','cardvisual_','pagenavigator']):
                        print('DEF',d,list(dv.get('properties',{})))
            walk(v,path+'/'+k)
    elif isinstance(x,list):
        for v in x:walk(v,path)
walk(j)
