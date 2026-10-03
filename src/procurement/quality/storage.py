"""Quality evidence is separate from raw payloads and mandatory before day commit."""

from procurement.common.settings import settings
from procurement.storage.io import read_json, write_json


def quality_prefix(identity, run_id, source_date):
    return (f"{settings.OBJECT_STORAGE_BUCKET}/_quality/{identity.source}/{identity.resource}/"
            f"run_id={run_id}/source_date={source_date}")


def save_quality_page(fs, identity, run_id, source_date, page_number, config_hash, observations):
    return write_json(fs, f"{quality_prefix(identity, run_id, source_date)}/"
                      f"page-{page_number:06d}.json", {
                          "schema_version": 1, "config_hash": config_hash,
                          "observations": observations,
                      })


def read_quality_contexts(fs, identity, run_id, source_date):
    result = []
    prefix = quality_prefix(identity, run_id, source_date)
    for key in sorted(fs.glob(f"{prefix}/page-*.json")):
        page = read_json(fs, key)
        result.extend(item["context"] for item in page["observations"] if "context" in item)
    return result
