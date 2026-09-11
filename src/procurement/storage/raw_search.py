import gzip
import json
from datetime import date
from typing import Any, BinaryIO, cast

import s3fs

from procurement.common.settings import settings


def _build_raw_search_key(
    *,
    source: str,
    resource: str,
    source_date: date,
    run_id: str,
    page_number: int,
) -> str:
    """Build the object key for a raw search API response."""

    return (
        f"{settings.OBJECT_STORAGE_BUCKET}/"
        f"_raw_search/"
        f"{source}/"
        f"{resource}/"
        f"source_date={source_date.isoformat()}/"
        f"run_id={run_id}/"
        f"page-{page_number:06d}.json.gz"
    )


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
    """Save one raw search API response as gzip-compressed JSON."""

    key = _build_raw_search_key(
        source=source,
        resource=resource,
        source_date=source_date,
        run_id=run_id,
        page_number=page_number,
    )

    json_bytes = json.dumps(
        response,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")

    compressed = gzip.compress(json_bytes)

    with fs.open(key, "wb") as raw_file:
        file = cast(BinaryIO, raw_file)
        file.write(compressed)

    return f"s3://{key}"
