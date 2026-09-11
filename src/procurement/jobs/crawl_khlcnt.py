import argparse
from datetime import date

from procurement.common.logging_config import configure_logging
from procurement.common.settings import settings
from procurement.ingestion.engine.daily_runner import run_daily_resource
from procurement.ingestion.muasamcong.client import MuasamcongClient
from procurement.ingestion.muasamcong.resources.khlcnt import create_khlcnt_spec
from procurement.ingestion.range_runner import run_daily_range
from procurement.storage.object_store import create_s3_filesystem


def crawl_khlcnt(
    start: date, end: date, *, page_size: int = 50, force: bool = False
) -> str:
    if not settings.MUASAMCONG_TOKEN:
        raise RuntimeError("MUASAMCONG_TOKEN is missing")
    filesystem = create_s3_filesystem()
    with MuasamcongClient(token=settings.MUASAMCONG_TOKEN) as client:
        spec = create_khlcnt_spec(client)
        return run_daily_range(
            start,
            end,
            resource=spec.identity.resource,
            run_day=lambda run_id, source_date: run_daily_resource(
                fs=filesystem,
                spec=spec,
                run_id=run_id,
                source_date=source_date,
                page_size=page_size,
                force=force,
            ),
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Crawl Muasamcong KHLCNT data")
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument("--page-size", type=int, default=50)
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def main() -> None:
    configure_logging()
    args = parse_args()
    crawl_khlcnt(
        args.start_date,
        args.end_date,
        page_size=args.page_size,
        force=args.force,
    )


if __name__ == "__main__":
    main()
