from procurement.ingestion.engine.stats import PageStats


def test_stats_support_arbitrary_resource_entity_names() -> None:
    stats = PageStats(search_items=2)
    stats.record("notice")
    stats.error("result_detail")

    metadata = stats.to_metadata()

    assert metadata["record_counts"] == {"notice": 1}
    assert metadata["error_counts"] == {"result_detail": 1}
    assert metadata["notice_records"] == 1
    assert metadata["result_detail_errors"] == 1


def test_stats_can_resume_legacy_khlcnt_checkpoint() -> None:
    stats = PageStats.from_metadata(
        {
            "search_items": 3,
            "plan_records": 2,
            "bid_package_records": 4,
            "plan_errors": 1,
            "bid_package_errors": 2,
            "total_errors": 3,
        }
    )

    assert stats.search_items == 3
    assert stats.record_counts == {"plan": 2, "bid_package": 4}
    assert stats.error_counts == {"plan": 1, "bid_package": 2}
    assert stats.total_errors == 3
