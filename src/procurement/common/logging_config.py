import logging

from procurement.common.errors import sanitize_error_message
from procurement.common.settings import settings

QUIET_LOGGERS = (
    "aiobotocore",
    "asyncio",
    "botocore",
    "dlt",
    "fsspec",
    "httpcore",
    "httpx",
    "s3fs",
    "urllib3",
)


class RedactingFormatter(logging.Formatter):
    """Sanitize the rendered record, including arguments and chained tracebacks."""

    def format(self, record: logging.LogRecord) -> str:
        return sanitize_error_message(super().format(record))


def configure_logging() -> None:
    """Configure project logs while keeping dependency output quiet."""

    logging.basicConfig(
        level=settings.LOG_LEVEL.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    for handler in logging.getLogger().handlers:
        handler.setFormatter(RedactingFormatter("%(asctime)s %(levelname)s %(name)s %(message)s"))

    for logger_name in QUIET_LOGGERS:
        logging.getLogger(logger_name).setLevel(logging.WARNING)
