import json
from collections.abc import Mapping
from typing import Any, BinaryIO, cast

import s3fs


def write_json(fs: s3fs.S3FileSystem, key: str, value: Mapping[str, Any]) -> str:
    content = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    with fs.open(key, "wb") as raw_file:
        cast(BinaryIO, raw_file).write(content)
    return f"s3://{key}"


def read_json(fs: s3fs.S3FileSystem, key: str) -> dict[str, Any] | None:
    if not fs.exists(key):
        return None
    with fs.open(key, "rb") as raw_file:
        return cast(dict[str, Any], json.load(raw_file))
