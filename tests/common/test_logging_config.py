import logging

from procurement.common.logging_config import QUIET_LOGGERS, configure_logging


def test_dependency_loggers_are_limited_to_warnings() -> None:
    configure_logging()

    for logger_name in QUIET_LOGGERS:
        assert logging.getLogger(logger_name).level == logging.WARNING


def test_formatter_redacts_arguments_and_chained_traceback():
    import sys

    from procurement.common.logging_config import RedactingFormatter

    try:
        try:
            raise ValueError("https://example.test?token=inner-secret")
        except ValueError as exc:
            raise RuntimeError("Authorization: Bearer outer-secret") from exc
    except RuntimeError:
        record = logging.LogRecord(
            "test",
            logging.ERROR,
            __file__,
            1,
            "request=%s",
            ("https://example.test?token=argument-secret",),
            sys.exc_info(),
        )
    rendered = RedactingFormatter().format(record)
    for secret in ("inner-secret", "outer-secret", "argument-secret"):
        assert secret not in rendered
    assert "RuntimeError" in rendered
    assert "[REDACTED]" in rendered
