"""Dirty whole-day planning; every semantic input participates in the cache key."""

from pathlib import Path

from procurement.processing.silver.selection import digest


def code_fingerprint():
    root = Path(__file__).parent
    return digest({p.relative_to(root).as_posix(): p.read_text(encoding="utf-8")
                   for p in sorted(root.rglob("*.py"))})


def partition_key(partition, contracts):
    return digest({"input": partition, "contracts": contracts, "code": code_fingerprint()})


def plan(partitions, previous, contracts):
    prior = {(p["resource"], p["source_date"]): p["fingerprint"] for p in previous}
    return [{"resource": p["resource"], "source_date": p["source_date"],
             "fingerprint": (fingerprint := partition_key(p, contracts)),
             "dirty": prior.get((p["resource"], p["source_date"])) != fingerprint}
            for p in partitions]
