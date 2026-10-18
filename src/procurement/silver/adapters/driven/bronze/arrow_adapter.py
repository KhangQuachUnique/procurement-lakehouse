"""Optimized Arrow Bronze Repository Adapter for Silver.

Streams raw Bronze Parquet files via PyArrow with single-pass verification.
Decoupled from internal Bronze ingestion mechanics.
"""

import hashlib
import json
from collections.abc import Callable, Iterator
from datetime import date
from typing import Any

import pyarrow.parquet as pq

from procurement.common.catalog import get_resource
from procurement.ingestion.engine.metadata import calculate_content_hash
from procurement.silver.ports.outbound.bronze_repository import (
    BronzeFileDescriptor,
    BronzeRepositoryPort,
    CommittedBronzePartition,
)
from procurement.storage.committed import select_committed_days
from procurement.storage.control import list_run_manifests, read_day_manifest


def file_sha256(fs: Any, key: str) -> str:
    """Calculate file SHA-256 in 1MB blocks."""
    hasher = hashlib.sha256()
    with fs.open(key, "rb") as stream:
        while block := stream.read(1024 * 1024):
            hasher.update(block)
    return hasher.hexdigest()


class ArrowBronzeRepositoryAdapter(BronzeRepositoryPort):
    """PyArrow-based implementation of BronzeRepositoryPort."""

    def __init__(
        self,
        fs: Any,
        freeze_fn: Callable[..., Any] | None = None,
        records_fn: Callable[..., Any] | None = None,
        workers: int = 16,
    ) -> None:
        self.fs = fs
        self._freeze_fn = freeze_fn
        self._records_fn = records_fn
        self.workers = workers
        self._runs_cache: dict[str, list[Any]] = {}

    def clear_runs_cache(self, resource: str | None = None) -> None:
        """Clear cached run manifests for a resource or all resources."""
        if resource is not None:
            self._runs_cache.pop(resource, None)
        else:
            self._runs_cache.clear()

    def select_committed_partitions(
        self,
        resources: list[str],
        start: date,
        end: date,
    ) -> list[CommittedBronzePartition]:
        """Discover and freeze committed Bronze partitions."""
        if self._freeze_fn is not None:
            raw_partitions = self._freeze_fn(self.fs, resources, start, end)
            result = []
            for p in raw_partitions:
                files = tuple(
                    BronzeFileDescriptor(
                        table=f["table"],
                        key=f["key"],
                        sha256=f.get("sha256") or "",
                    )
                    for f in p["files"]
                )
                result.append(
                    CommittedBronzePartition(
                        resource=p["resource"],
                        source_date=p["source_date"],
                        run_id=p["run_id"],
                        expected_records=p["expected_records"],
                        files=files,
                        manifest=p.get("manifest", {}),
                    )
                )
            return result

        if start > end or not resources or len(set(resources)) != len(resources):
            raise ValueError("Select a nonempty unique resource set and an ordered date range")

        partitions = []
        for resource in sorted(resources):
            definition = get_resource(resource)
            if resource not in self._runs_cache:
                self._runs_cache[resource] = list_run_manifests(
                    self.fs, definition.identity, workers=self.workers
                )
            cached_runs = self._runs_cache[resource]
            for day in select_committed_days(
                self.fs, definition, start, end, workers=self.workers, runs=cached_runs
            ):
                manifest = read_day_manifest(
                    self.fs, definition.identity, day.run_id, day.source_date
                )
                if manifest is None or manifest.status.value != "success":
                    raise ValueError("Selected commit disappeared")
                files = tuple(
                    BronzeFileDescriptor(
                        table=table,
                        key=key,
                        sha256=file_sha256(self.fs, key),
                    )
                    for table, key in day.files
                )
                partitions.append(
                    CommittedBronzePartition(
                        resource=resource,
                        source_date=day.source_date.isoformat(),
                        run_id=day.run_id,
                        expected_records=day.expected_records,
                        files=files,
                        manifest=manifest.model_dump(mode="json"),
                    )
                )
        return partitions

    def stream_partition_records(
        self,
        partition: CommittedBronzePartition,
        selected_tables: tuple[str, ...] | None = None,
        verify_content_hash: bool = False,
    ) -> Iterator[tuple[dict[str, Any], int, dict[str, Any]]]:
        """Stream raw records from committed partition."""
        if self._records_fn is not None:
            # Reconstruct legacy partition dictionary for injected/mocked records_fn
            raw_partition = {
                "resource": partition.resource,
                "source_date": partition.source_date,
                "run_id": partition.run_id,
                "expected_records": partition.expected_records,
                "manifest": partition.manifest,
                "files": [
                    {"table": f.table, "key": f.key, "sha256": f.sha256} for f in partition.files
                ],
            }
            yield from self._records_fn(self.fs, raw_partition)
            return

        count = 0
        for file in partition.files:
            file_dict = {"table": file.table, "key": file.key, "sha256": file.sha256}
            with self.fs.open(file.key, "rb") as stream:
                ordinal = 0
                for batch in pq.ParquetFile(stream).iter_batches(batch_size=1024):
                    for row in batch.to_pylist():
                        if (
                            row["run_id"] != partition.run_id
                            or str(row["source_date"])[:10] != partition.source_date
                        ):
                            raise ValueError("Bronze lineage mismatch")

                        if verify_content_hash:
                            payload = (
                                json.loads(row["payload"])
                                if isinstance(row["payload"], str)
                                else row["payload"]
                            )
                            if calculate_content_hash(payload) != row["content_hash"]:
                                raise ValueError("Bronze content hash mismatch")

                        yield file_dict, ordinal, row
                        ordinal += 1
                        count += 1

        if count != partition.expected_records:
            raise ValueError(
                f"Bronze count mismatch: expected {partition.expected_records}, got {count}"
            )
