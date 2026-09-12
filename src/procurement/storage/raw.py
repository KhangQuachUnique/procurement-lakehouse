import gzip
import json
from datetime import date
from typing import Any, BinaryIO, cast

import s3fs

from procurement.common.settings import settings


def save_raw_search_page(
    *,
    fs: s3fs.S3FileSystem,
    source: str,
    resource: str,
    source_date: date,
    run_id: str,
    page_number: int,
    response: dict[str, Any],
) -> str:
    key = (
        f"{settings.OBJECT_STORAGE_BUCKET}/_raw_search/{source}/{resource}/"
        f"source_date={source_date.isoformat()}/run_id={run_id}/"
        f"page-{page_number:06d}.json.gz"
    )
    payload = json.dumps(response, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    with fs.open(key, "wb") as raw_file:
        cast(BinaryIO, raw_file).write(gzip.compress(payload))
    return f"s3://{key}"
