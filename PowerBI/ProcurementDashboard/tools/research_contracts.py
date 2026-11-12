import json
from pathlib import Path
import requests

ROOT = Path(__file__).resolve().parents[1] / 'validation/contracts'
index = json.loads((ROOT / 'index.json').read_text())
for u, f in index.items():
    if any(s in u for s in ['visualConfiguration/', 'report/3.', 'page/2.', 'visualContainer/']):
        j = json.loads((ROOT / f).read_text())
        print(u, '\n', json.dumps(j, indent=2)[:28000])
if __name__ == '__main__':
    for repo in ['microsoft/powerbi-desktop-samples', 'microsoft/fabric-samples']:
        r = requests.get(f'https://api.github.com/repos/{repo}/git/trees/main?recursive=1', timeout=60)
        r.raise_for_status()
        paths = [x['path'] for x in r.json()['tree'] if x['path'].endswith(('visual.json','report.json','page.json'))]
        (ROOT / (repo.split('/')[1] + '-paths.json')).write_text(json.dumps(paths,indent=2))
        print(repo, paths[:35])
