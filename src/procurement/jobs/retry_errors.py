import argparse
from datetime import date

from procurement.common.logging_config import configure_logging
from procurement.common.settings import settings
from procurement.ingestion.retry.runner import retry_resource_errors
from procurement.ingestion.sources.muasamcong.client import MuasamcongClient
from procurement.ingestion.sources.muasamcong.khlcnt.resource import create_khlcnt_spec
from procurement.storage.object_store import create_s3_filesystem


def retry_khlcnt_errors(
    source_date: date, *, run_id: str | None = None, max_attempts: int = 3
) -> str:
    if not settings.MUASAMCONG_TOKEN:
        raise RuntimeError("MUASAMCONG_TOKEN is missing")
    fs = create_s3_filesystem()
    with MuasamcongClient(token=settings.MUASAMCONG_TOKEN) as client:
        spec = create_khlcnt_spec(client)
        return retry_resource_errors(
            fs=fs,
            spec=spec,
            source_date=source_date,
            original_run_id=run_id,
            max_attempts=max_attempts,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Retry unresolved record-level ingestion errors")
    parser.add_argument("--resource", choices=["khlcnt"], default="khlcnt")
    parser.add_argument("--source-date", type=date.fromisoformat, required=True)
    parser.add_argument("--run-id")
    parser.add_argument("--max-attempts", type=int, default=3)
    return parser.parse_args()


def main() -> None:
    configure_logging()
    args = parse_args()
    if args.resource == "khlcnt":
        retry_khlcnt_errors(args.source_date, run_id=args.run_id, max_attempts=args.max_attempts)


if __name__ == "__main__":
    main()
