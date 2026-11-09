"""Regression coverage for the CLI split that previously recrawled Dagster commits."""

import importlib.util
from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from procurement.cli import ingest
from procurement.common.catalog import SUPPORTED_RESOURCES
from procurement.ingestion.contracts import MaterializeDayResult


def options(*extra):
    return ingest.build_parser().parse_args([
        '--resource', 'project', '--start-date', '2026-01-01',
        '--end-date', '2026-01-31', *extra,
    ])


def test_january_dagster_commits_are_reused_in_cli_plan(monkeypatch):
    monkeypatch.setattr(ingest, 'today_vn', lambda: date(2026, 11, 3))
    metadata = Mock()
    metadata.get_snapshot.side_effect = lambda identities: [
        SimpleNamespace(partition=SimpleNamespace(identity=identity)) for identity in identities
    ]
    monkeypatch.setattr(ingest, 'get_metadata_service', lambda: metadata)
    crawler = Mock(side_effect=AssertionError('dry-run must not construct ingestion service'))
    monkeypatch.setattr(ingest, 'get_ingestion_service', crawler)
    report = ingest.run_ingest(options('--dry-run'))
    assert report['metadata'] == 'postgresql'
    assert report['resources'] == [{
        'resource': 'project', 'committed_days': 31, 'reused_days': 31, 'planned_dates': [],
    }]
    crawler.assert_not_called()


def test_plan_includes_only_missing_days_unless_refresh(monkeypatch):
    monkeypatch.setattr(ingest, 'today_vn', lambda: date(2026, 11, 3))
    metadata = Mock()
    metadata.get_snapshot.side_effect = lambda identities: [
        SimpleNamespace(partition=SimpleNamespace(identity=identity))
        for identity in identities if identity.source_date.day != 15
    ]
    monkeypatch.setattr(ingest, 'get_metadata_service', lambda: metadata)
    report = ingest.run_ingest(options('--dry-run'))
    assert report['resources'][0]['planned_dates'] == ['2026-01-15']
    refreshed = ingest.run_ingest(options('--dry-run', '--refresh'))
    assert len(refreshed['resources'][0]['planned_dates']) == 31
    assert refreshed['resources'][0]['reused_days'] == 0


def test_current_year_stops_at_yesterday_in_vietnam(monkeypatch):
    monkeypatch.setattr(ingest, 'today_vn', lambda: date(2026, 11, 3))
    args = ingest.build_parser().parse_args(['--resource', 'all', '--year', '2026'])
    days = ingest.resolve_dates(args)
    assert days[0] == date(2026, 1, 1)
    assert days[-1] == date(2026, 11, 2)


@pytest.mark.parametrize('scope', [
    ['--year', '2027'], ['--date', '2026-11-03'],
    ['--start-date', '2026-01-01', '--end-date', '2026-11-03'],
])
def test_future_or_unclosed_dates_fail_before_connections(monkeypatch, scope):
    monkeypatch.setattr(ingest, 'today_vn', lambda: date(2026, 11, 3))
    forbidden = Mock(side_effect=AssertionError('must validate dates first'))
    monkeypatch.setattr(ingest, 'get_ingestion_service', forbidden)
    monkeypatch.setattr(ingest, 'get_metadata_service', forbidden)
    args = ingest.build_parser().parse_args(['--resource', 'project', *scope])
    with pytest.raises(ValueError, match='closed'):
        ingest.run_ingest(args)
    forbidden.assert_not_called()


def test_database_failure_does_not_fallback_to_legacy_crawler(monkeypatch):
    monkeypatch.setattr(ingest, 'today_vn', lambda: date(2026, 11, 3))
    monkeypatch.setattr(ingest, 'get_metadata_service', Mock(side_effect=OSError('DB offline')))
    crawler = Mock()
    monkeypatch.setattr(ingest, 'get_ingestion_service', crawler)
    with pytest.raises(OSError, match='DB offline'):
        ingest.run_ingest(options('--dry-run'))
    crawler.assert_not_called()


def test_all_resources_use_same_new_service_and_unique_request_ids(monkeypatch):
    monkeypatch.setattr(ingest, 'today_vn', lambda: date(2026, 11, 3))
    service = Mock()
    service.materialize_day.side_effect = lambda req: MaterializeDayResult(
        req.source, req.resource, req.source_date, reused=True, status='success',
    )
    monkeypatch.setattr(ingest, 'get_ingestion_service', lambda: service)
    args = ingest.build_parser().parse_args(['--resource', 'all', '--date', '2026-01-01'])
    report = ingest.run_ingest(args)
    requests = [call.args[0] for call in service.materialize_day.call_args_list]
    assert [req.resource for req in requests] == list(SUPPORTED_RESOURCES)
    assert all(not req.refresh for req in requests)
    assert len({req.request_id for req in requests}) == len(SUPPORTED_RESOURCES)
    assert report['reused_days'] == len(SUPPORTED_RESOURCES)


@pytest.mark.parametrize('module', [
    'procurement.jobs.ingest', 'procurement.jobs.runner',
    'procurement.tools.recover_result_2024', 'procurement.tools.compact_bronze',
    'procurement.tools.bronze_transfer', 'procurement.tools.repair_bronze_quality',
    'procurement.tools.audit_bronze_quality', 'procurement.tools.watch_bid_opening',
    'procurement.cli.compact_bronze', 'procurement.cli.bronze_transfer',
    'procurement.cli.repair_bronze_quality', 'procurement.cli.watch_bid_opening',
])
def test_retired_entry_points_are_removed(module):
    assert importlib.util.find_spec(module) is None
