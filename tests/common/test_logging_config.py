import logging

from procurement.common.logging_config import QUIET_LOGGERS, configure_logging


def test_dependency_loggers_are_limited_to_warnings() -> None:
    configure_logging()

    for logger_name in QUIET_LOGGERS:
        assert logging.getLogger(logger_name).level == logging.WARNING
