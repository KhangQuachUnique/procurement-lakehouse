import argparse
from datetime import date, datetime
from zoneinfo import ZoneInfo

from procurement.common.logging_config import configure_logging
from procurement.common.settings import settings
from procurement.ingestion.batch_runner import run_batch_range
from procurement.ingestion.engine.daily_runner import run_daily_resource
from procurement.ingestion.sources.muasamcong.client import MuasamcongClient
from procurement.ingestion.sources.muasamcong.notify_contractor.resource import (
    create_notify_contractor_spec,
)
from procurement.storage.object_store import create_s3_filesystem

VIETNAM_TZ = ZoneInfo("Asia/Ho_Chi_Minh")


def _validate_closed_range(start: date, end: date, *, today: date) -> None:
    if start > end:
        raise ValueError("start must be before or equal to end")
    if end >= today:
        raise ValueError("Only closed source dates can be crawled")


def crawl_notify_contractor(
    start: date,
    end: date,
    *,
    page_size: int = 50,
) -> str:
    today_vn = datetime.now(VIETNAM_TZ).date()
    _validate_closed_range(start, end, today=today_vn)

    if not settings.MUASAMCONG_TOKEN:
        raise RuntimeError("MUASAMCONG_TOKEN is missing")

    filesystem = create_s3_filesystem()
    with MuasamcongClient(token=settings.MUASAMCONG_TOKEN) as client:
        spec = create_notify_contractor_spec(client)
        return run_batch_range(
            start,
            end,
            fs=filesystem,
            identity=spec.identity,
            run_day=lambda run_id, source_date: run_daily_resource(
                fs=filesystem,
                spec=spec,
                run_id=run_id,
                source_date=source_date,
                page_size=page_size,
            ),
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Crawl closed-day Muasamcong contractor-notice data"
    )
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument("--page-size", type=int, default=50)
    return parser.parse_args()


def main() -> None:
    configure_logging()
    args = parse_args()
    crawl_notify_contractor(
        args.start_date,
        args.end_date,
        page_size=args.page_size,
    )


if __name__ == "__main__":
    main()
