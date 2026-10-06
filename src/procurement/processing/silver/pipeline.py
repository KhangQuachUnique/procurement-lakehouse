"""Content-addressed day transformations; a full rebuild remains the reference result."""

import json
from pathlib import Path

from procurement.processing.silver.contracts import VERSION
from procurement.processing.silver.observations import transform
from procurement.processing.silver.planner import plan
from procurement.processing.silver.revisions import assemble, validate
from procurement.processing.silver.selection import digest, records


def build(fs, selection, *, namespace, cache_dir, previous=(), full=False):
    directory = Path(cache_dir)
    directory.mkdir(parents=True, exist_ok=True)
    planned = plan(selection, previous, {**VERSION, "namespace": namespace})
    rows, rebuilt = [], 0
    for partition, item in zip(selection, planned, strict=True):
        path = directory / (item["fingerprint"] + ".json")
        cached = None
        if not full and not item["dirty"] and path.exists():
            envelope = json.loads(path.read_text(encoding="utf-8"))
            if envelope["checksum"] == digest(envelope["rows"]):
                cached = envelope["rows"]
        if cached is None:
            cached = [transform(namespace, partition, file, ordinal, record)
                      for file, ordinal, record in records(fs, partition)]
            temporary = path.with_suffix(".tmp")
            temporary.write_text(json.dumps({"checksum": digest(cached), "rows": cached},
                                            ensure_ascii=False, default=str), encoding="utf-8")
            temporary.replace(path)
            rebuilt += 1
        rows.extend(cached)
    tables = assemble(rows)
    report = validate(tables)
    return tables, {**report, "partitions": planned, "rebuilt_partitions": rebuilt,
                    "contracts": VERSION, "selection_hash": digest(selection)}
