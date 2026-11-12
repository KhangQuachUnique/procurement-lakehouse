import json
from pathlib import Path
import requests

d=Path(__file__).resolve().parents[1]/'validation/contracts'
repo='microsoft/skills-for-fabric'
r=requests.get(f'https://api.github.com/repos/{repo}/git/trees/main?recursive=1',timeout=60)
r.raise_for_status()
paths=[x['path'] for x in r.json()['tree'] if ('powerbi' in x['path'].lower() or 'pbir' in x['path'].lower()) and x['type']=='blob']
(d/'authoring-paths.json').write_text(json.dumps(paths,indent=2))
print('\n'.join(p for p in paths if any(s in p for s in ['visual','authoring-part','slicer','theme'])))
u='https://api.github.com/repos/microsoft/powerbi-desktop-samples/contents/Report%20Theme%20JSON%20Schema'
r=requests.get(u,timeout=60); r.raise_for_status()
themes=[x for x in r.json() if x['name'].endswith('.json')]
print('themes',[(x['name'],x['download_url']) for x in themes[-8:]])
chosen=next((x for x in themes if '2.154' in x['name']),themes[-1])
data=requests.get(chosen['download_url'],timeout=60);data.raise_for_status()
(d/'theme-schema.json').write_text(data.text,encoding='utf-8')
(d/'theme-schema-source.txt').write_text(chosen['download_url'])
for p in paths:
    if p.startswith('plugins/') and any(p.endswith('/'+name+'.md') for name in ['cartesian','card','table','textbox','button','filters','slicers-part-02','theming']):
        resp=requests.get(f'https://raw.githubusercontent.com/{repo}/main/{p}',timeout=60)
        resp.raise_for_status()
        (d/Path(p).name).write_text(resp.text,encoding='utf-8')
