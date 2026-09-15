from datetime import date

import pytest

import procurement.jobs.crawl_all as crawl_all_module
from procurement.common.resources import ResourceIdentity
from procurement.jobs.crawl_all import ResourceJob, crawl_all


class _ImmediateFuture:
    def __init__(self, value=None, error: Exception | None = None) -> None:
        self._value = value
        self._error = error

    def result(self):
        if self._error is not None:
            raise self._error
        return self._value


class _InlineProcessPoolExecutor:
    worker_counts: list[int] = []

    def __init__(self, *, max_workers: int) -> None:
        self.worker_counts.append(max_workers)

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def submit(self, fn, *args) -> _ImmediateFuture:
        try:
            return _ImmediateFuture(value=fn(*args))
        except Exception as exc:  # noqa: BLE001
            return _ImmediateFuture(error=exc)


def _install_inline_executor(monkeypatch) -> None:
    _InlineProcessPoolExecutor.worker_counts.clear()
    monkeypatch.setattr(
        crawl_all_module,
        "ProcessPoolExecutor",
        _InlineProcessPoolExecutor,
    )
    # Reverse completion order to verify the returned summary still follows business order.
    monkeypatch.setattr(
        crawl_all_module,
        "as_completed",
        lambda futures: reversed(list(futures)),
    )
    monkeypatch.setattr(crawl_all_module, "configure_logging", lambda: None)


def _identity(resource: str) -> ResourceIdentity:
    return ResourceIdentity(source="muasamcong", resource=resource)


def test_crawl_all_uses_two_workers_and_continues_after_crash(monkeypatch) -> None:
    calls: list[tuple[str, date, date, int]] = []

    def successful(resource: str, run_id: str):
        def crawl(start: date, end: date, *, page_size: int = 50) -> str:
            calls.append((resource, start, end, page_size))
            return run_id

        return crawl

    def crashed(start: date, end: date, *, page_size: int = 50) -> str:
        calls.append(("khlcnt", start, end, page_size))
        raise RuntimeError("source unavailable")

    jobs = (
        ResourceJob(_identity("project"), successful("project", "run-project")),
        ResourceJob(_identity("khlcnt"), crashed),
        ResourceJob(
            _identity("notify_contractor"),
            successful("notify_contractor", "run-notify"),
        ),
        ResourceJob(
            _identity("contractor_result"),
            successful("contractor_result", "run-result"),
        ),
    )

    _install_inline_executor(monkeypatch)
    monkeypatch.setattr(crawl_all_module, "_resource_jobs", lambda: jobs)
    monkeypatch.setattr(crawl_all_module, "_today_vn", lambda: date(2026, 9, 15))
    monkeypatch.setattr(crawl_all_module, "create_s3_filesystem", lambda: object())
    monkeypatch.setattr(crawl_all_module, "_read_run_status", lambda *_args: "success")

    results = crawl_all(2022, page_size=25)

    assert _InlineProcessPoolExecutor.worker_counts == [2]
    assert [item[0] for item in calls] == [
        "project",
        "khlcnt",
        "notify_contractor",
        "contractor_result",
    ]
    assert all(item[1] == date(2022, 1, 1) for item in calls)
    assert all(item[2] == date(2022, 12, 31) for item in calls)
    assert all(item[3] == 25 for item in calls)

    assert [(item.resource, item.status) for item in results] == [
        ("project", "success"),
        ("khlcnt", "crashed"),
        ("notify_contractor", "success"),
        ("contractor_result", "success"),
    ]
    assert results[1].run_id is None
    assert results[1].error == "source unavailable"


def test_crawl_all_uses_persisted_run_status(monkeypatch) -> None:
    def crawl(start: date, end: date, *, page_size: int = 50) -> str:
        return "run-a"

    jobs = (ResourceJob(_identity("project"), crawl),)

    _install_inline_executor(monkeypatch)
    monkeypatch.setattr(crawl_all_module, "_resource_jobs", lambda: jobs)
    monkeypatch.setattr(crawl_all_module, "_today_vn", lambda: date(2026, 9, 15))
    monkeypatch.setattr(crawl_all_module, "create_s3_filesystem", lambda: object())
    monkeypatch.setattr(
        crawl_all_module,
        "_read_run_status",
        lambda _fs, _identity, run_id: "partial_failed" if run_id == "run-a" else "unknown",
    )

    results = crawl_all(2022)

    assert _InlineProcessPoolExecutor.worker_counts == [1]
    assert results[0].run_id == "run-a"
    assert results[0].status == "partial_failed"


def test_year_range_requires_fully_closed_year() -> None:
    today = date(2026, 9, 15)

    assert crawl_all_module._year_range(2025, today=today) == (
        date(2025, 1, 1),
        date(2025, 12, 31),
    )

    with pytest.raises(ValueError, match="fully closed"):
        crawl_all_module._year_range(2026, today=today)

    with pytest.raises(ValueError, match="fully closed"):
        crawl_all_module._year_range(2027, today=today)


def test_crawl_all_rejects_invalid_page_size(monkeypatch) -> None:
    monkeypatch.setattr(crawl_all_module, "_today_vn", lambda: date(2026, 9, 15))

    with pytest.raises(ValueError, match="page_size"):
        crawl_all(2022, page_size=0)
