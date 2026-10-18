"""Driven adapter implementing JsonlRecordStreamPort for local JSONL files."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from procurement.profiling.ports.outbound.record_stream_port import (
    JsonlRecordStreamPort,
)


class JsonlRecordStreamAdapter(JsonlRecordStreamPort):
    """Streams decoded records from local JSONL files."""

    def stream_jsonl_records(
        self,
        file_path: str | Path,
        *,
        max_records: int | None = None,
        sample_step: int = 1,
    ) -> Iterator[tuple[int, dict[str, Any]]]:
        path = Path(file_path)
        if not path.is_file():
            raise FileNotFoundError(f"JSONL file not found: {path}")

        counter = 0
        yielded = 0

        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                counter += 1
                if sample_step > 1 and (counter % sample_step != 0):
                    continue

                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue

                yielded += 1
                yield counter, record

                if max_records is not None and yielded >= max_records:
                    break
