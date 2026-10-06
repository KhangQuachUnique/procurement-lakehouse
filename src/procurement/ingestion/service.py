"""Ingestion service: core business use case for materializing a resource for a day."""

import re
from collections import defaultdict
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import dlt
import pyarrow.parquet as pq
import s3fs

from procurement.bronze.dlt_writer import (
    DltBronzeWriter,
    create_bronze_destination,
)
from procurement.bronze.verification import calculate_file_sha256
from procurement.bronze.writer import BufferedBronzeWriter
from procurement.common.dates import api_day_window
from procurement.ingestion.contracts import (
    MaterializeDayRequest,
    MaterializeDayResult,
)
from procurement.ingestion.engine.models import ResourceSpec
from procurement.ingestion.engine.stats import PageStats
from procurement.metadata.contracts import MetadataService
from procurement.metadata.models import (
    CommitFileDescriptor,
    ErrorDescriptor,
    PageRecordDescriptor,
    PartitionIdentity,
)


def _pipeline_component(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", value).strip("_")


class IngestionService:
    """Orchestrates materializing a single resource partition for a single source date."""

    def __init__(
        self,
        *,
        metadata: MetadataService,
        spec_factory: Callable[[str], ResourceSpec],
        bucket: str,
        access_key: str,
        secret_key: str,
        endpoint_url: str | None = None,
        fs: s3fs.S3FileSystem | None = None,
        pipelines_dir: str | None = None,
    ):
        self.metadata = metadata
        self._spec_factory = spec_factory
        self.bucket = bucket
        self.access_key = access_key
        self.secret_key = secret_key
        self.endpoint_url = endpoint_url
        self.pipelines_dir = pipelines_dir

        if fs is not None:
            self.fs = fs
        elif (
            ":\\" in str(bucket)
            or ":/" in str(bucket)
            or str(bucket).startswith("file://")
            or "\\" in str(bucket)
            or (str(bucket).startswith("/") and not str(bucket).startswith("/procurement"))
        ):
            import fsspec

            self.fs = fsspec.filesystem("file", auto_mkdir=True)
        else:
            s3_client_kwargs: dict[str, Any] = {}
            if endpoint_url:
                s3_client_kwargs["endpoint_url"] = endpoint_url
            self.fs = s3fs.S3FileSystem(
                key=access_key,
                secret=secret_key,
                client_kwargs=s3_client_kwargs,
            )

    def materialize_day(self, request: MaterializeDayRequest) -> MaterializeDayResult:
        identity = PartitionIdentity(
            source=request.source,
            resource=request.resource,
            source_date=request.source_date,
        )

        begin_res = self.metadata.begin_or_reuse(
            identity,
            refresh=request.refresh,
            request_id=request.request_id,
            owner_id=request.owner_id,
            dagster_run_id=request.dagster_run_id,
        )

        if begin_res.reused:
            commit = begin_res.commit
            return MaterializeDayResult(
                source=request.source,
                resource=request.resource,
                source_date=request.source_date,
                reused=True,
                status="success",
                record_count=commit.record_count if commit else 0,
                file_count=commit.file_count if commit else 0,
                commit_id=commit.id if commit else None,
                attempt_id=begin_res.attempt.id if begin_res.attempt else None,
                metrics={"reused": True},
            )

        attempt = begin_res.attempt
        assert attempt is not None
        assert begin_res.lease is not None
        generation = begin_res.lease.generation
        spec = self._spec_factory(request.resource)
        run_id_str = str(attempt.id)

        try:
            pipeline_name = "_".join(
                (
                    _pipeline_component(spec.pipeline_name),
                    _pipeline_component(request.resource),
                    request.source_date.strftime("%Y%m%d"),
                    _pipeline_component(run_id_str[:8]),
                )
            )

            destination = create_bronze_destination(
                source_partition_date=request.source_date,
                run_id=run_id_str,
                bucket=self.bucket,
                access_key=self.access_key,
                secret_key=self.secret_key,
                endpoint_url=self.endpoint_url,
                dataset=spec.dataset_name,
            )

            dlt_pipeline_opts: dict[str, Any] = {}
            if self.pipelines_dir:
                dlt_pipeline_opts["pipelines_dir"] = self.pipelines_dir

            pipeline = dlt.pipeline(
                pipeline_name=pipeline_name,
                destination=destination,
                dataset_name=spec.dataset_name,
                **dlt_pipeline_opts,
            )

            raw_writer = DltBronzeWriter(lambda: pipeline)
            buffered_writer = BufferedBronzeWriter(raw_writer)

            window_from, window_to = api_day_window(request.source_date)
            page_num = 0
            total_records = 0

            while True:
                # Renew heartbeat before each fetch
                self.metadata.renew_lease(
                    attempt.id,
                    owner_id=request.owner_id,
                    generation=generation,
                )

                page_start = datetime.now(UTC)
                page_data = spec.fetch_page(
                    page_number=page_num,
                    page_size=request.page_size,
                    window_from=window_from,
                    window_to=window_to,
                )

                items = page_data.get("page", {}).get("content", [])
                stats = PageStats()
                errors: list[Any] = []

                bronze_items = list(
                    spec.iter_records(
                        search_items=items,
                        run_id=run_id_str,
                        source_date=request.source_date,
                        search_page=page_num,
                        errors=errors,
                        stats=stats,
                    )
                )

                table_batches: dict[str, list[Any]] = defaultdict(list)
                for item in bronze_items:
                    table_batches[item.table].append(item.record)

                if errors:
                    for err in errors:
                        raw_id = getattr(err, "error_id", None)
                        try:
                            err_uuid = UUID(hex=str(raw_id)) if raw_id else uuid4()
                        except (ValueError, TypeError):
                            err_uuid = uuid4()
                        self.metadata.record_error(
                            ErrorDescriptor(
                                id=err_uuid,
                                attempt_id=attempt.id,
                                stage=getattr(err, "stage", "extract"),
                                error_type=getattr(err, "error_type", "Error"),
                                message=getattr(err, "message", str(err)),
                                page_number=page_num,
                                source_record_id=getattr(err, "source_id", None),
                                http_status=getattr(err, "http_status", None),
                                details={},
                                occurred_at=getattr(err, "occurred_at", datetime.now(UTC)),
                            )
                        )
                    self.metadata.record_page(
                        PageRecordDescriptor(
                            attempt_id=attempt.id,
                            page_number=page_num,
                            page_size=request.page_size,
                            status="failed",
                            search_items=len(items),
                            bronze_records=0,
                            error_count=len(errors),
                            started_at=page_start,
                            completed_at=datetime.now(UTC),
                        )
                    )
                    buffered_writer.abort()
                    raise RuntimeError(
                        f"Extraction failed on page {page_num} with {len(errors)} error(s)"
                    )

                buffered_writer.write_page(table_batches)
                page_records = sum(len(records) for records in table_batches.values())
                total_records += page_records

                self.metadata.record_page(
                    PageRecordDescriptor(
                        attempt_id=attempt.id,
                        page_number=page_num,
                        page_size=request.page_size,
                        status="success",
                        search_items=len(items),
                        bronze_records=page_records,
                        error_count=0,
                        started_at=page_start,
                        completed_at=datetime.now(UTC),
                    )
                )

                total_pages = page_data.get("page", {}).get("totalPages", 1)
                page_num += 1
                if page_num >= total_pages or not items:
                    break

            buffered_writer.flush()

            # Inspect and verify written parquet files on storage
            if hasattr(self.fs, "invalidate_cache"):
                self.fs.invalidate_cache()
            clean_bucket = str(self.bucket).removeprefix("file://").replace("\\", "/")
            files_pattern = (
                f"{clean_bucket}/bronze/{spec.dataset_name}/*/"
                f"source_date={request.source_date.isoformat()}/run_id={run_id_str}/*.parquet"
            )
            found_keys = sorted(self.fs.glob(files_pattern))

            commit_files: list[CommitFileDescriptor] = []
            for file_num, raw_key in enumerate(found_keys):
                full_key = str(raw_key).replace("\\", "/")
                # Extract relative object key (without bucket prefix)
                object_key = (
                    full_key.removeprefix(f"{clean_bucket}/")
                    .removeprefix(f"{self.bucket}/")
                    .lstrip("/")
                )
                # Table name is typically: bronze/{dataset}/{table}/source_date=...
                parts = object_key.split("/")
                table_name = parts[2] if len(parts) > 2 else request.resource

                with self.fs.open(full_key, "rb") as f:
                    content_bytes = f.read()
                    sha256 = calculate_file_sha256(content_bytes)
                    f.seek(0)
                    pq_file = pq.ParquetFile(f)
                    row_count = pq_file.metadata.num_rows

                commit_files.append(
                    CommitFileDescriptor(
                        commit_id=attempt.id,  # temporary placeholder; publish_commit assigns commit_id
                        file_number=file_num,
                        table_name=table_name,
                        bucket=self.bucket,
                        object_key=object_key,
                        row_count=row_count,
                        size_bytes=len(content_bytes),
                        sha256=sha256,
                        schema_version=1,
                    )
                )

            commit = self.metadata.publish_commit(
                attempt.id,
                owner_id=request.owner_id,
                generation=generation,
                expected_base_commit_id=attempt.base_commit_id,
                record_count=total_records,
                files=commit_files,
                verification={"files_verified": len(commit_files)},
            )

            return MaterializeDayResult(
                source=request.source,
                resource=request.resource,
                source_date=request.source_date,
                reused=False,
                status="success",
                record_count=commit.record_count,
                file_count=commit.file_count,
                commit_id=commit.id,
                attempt_id=attempt.id,
                metrics={"records": total_records, "files": len(commit_files)},
            )

        except Exception as exc:
            self.metadata.fail_attempt(
                attempt.id,
                owner_id=request.owner_id,
                generation=generation,
                reason=str(exc),
            )
            raise
