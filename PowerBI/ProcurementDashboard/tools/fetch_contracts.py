"""Cache Microsoft JSON contracts and source examples for reproducible offline validation."""
import hashlib
import json
from pathlib import Path
from urllib.parse import urljoin, urldefrag
import requests

ROOT = Path(__file__).resolve().parents[1] / "validation/contracts"
ROOT.mkdir(parents=True, exist_ok=True)
INDEX = json.loads((ROOT/'index.json').read_text()) if (ROOT/'index.json').exists() else {}


def fetch(url):
    url = urldefrag(url)[0]
    if url in INDEX:
        return
    print(url, flush=True)
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    obj = r.json()
    filename = hashlib.sha256(url.encode()).hexdigest()[:16] + ".json"
    INDEX[url] = filename
    (ROOT / filename).write_text(json.dumps(obj, indent=2), encoding="utf-8")
    def walk(x):
        if isinstance(x, dict):
            ref = x.get("$ref", "")
            if ref and not ref.startswith("#"):
                fetch(urljoin(url, ref))
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)
    walk(obj)


if __name__ == "__main__":
    for part in [
        "../gitIntegration/platformProperties/2.0.0",
        "../pbip/pbipProperties/1.0.0", "report/definitionProperties/2.0.0",
        "semanticModel/definitionProperties/1.0.0",
        "report/definition/versionMetadata/1.0.0", "report/definition/report/3.0.0",
        "report/definition/pagesMetadata/1.0.0", "report/definition/page/2.0.0",
        "report/definition/visualContainer/2.1.0",
        "report/definition/visualContainer/2.9.0",
    ]:
        fetch(urljoin("https://developer.microsoft.com/json-schemas/fabric/item/", part + "/schema.json"))
    (ROOT / "index.json").write_text(json.dumps(INDEX, indent=2), encoding="utf-8")
