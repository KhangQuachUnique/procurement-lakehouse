"""Unit tests for the Bronze bid-opening to Silver transformer."""

from datetime import UTC, datetime

import pytest

from procurement.silver.application.revisions import assemble, validate
from procurement.silver.domain.protocols import RawRecordEnvelope
from procurement.silver.domain.schema import metadata
from procurement.silver.domain.transformers.bid_opening import (
    BidOpeningTransformer,
    classify_opening_shape,
    infer_collection_status,
)


@pytest.mark.parametrize(
    ("parts", "expected"),
    [
        (
            ["notify", "roundmng", "bid_open", "lot_open_detail"],
            "ldt_single_envelope",
        ),
        (
            [
                "notify",
                "roundmng",
                "bid_open_technical",
                "lot_open_detail_technical",
                "bid_open_financial",
                "lot_open_detail_financial",
            ],
            "ldt_two_envelope",
        ),
        (
            [
                "notify",
                "roundmng",
                "bid_open_technical",
                "lot_open_detail_technical",
                "bid_open_financial",
                "lot_open_detail_financial",
                "cancellation_notice",
            ],
            "ldt_two_envelope",
        ),
        (
            [
                "notify",
                "roundmng",
                "bid_open_technical",
                "lot_open_detail_technical",
            ],
            "ldt_two_envelope",
        ),
        (
            [
                "notify",
                "roundmng",
                "bid_open",
                "lot_open_detail",
                "cancellation_notice",
            ],
            "ldt_single_envelope",
        ),
        (["opening_out"], "khac_single_envelope"),
        (
            ["opening_out_technical", "opening_out_financial"],
            "khac_two_envelope",
        ),
        (["opening_out_technical"], "khac_two_envelope"),
        (["opening_adb_wb"], "adb_wb_single_envelope"),
    ],
)
def test_classify_all_bronze_shapes(parts, expected):
    payload = {name: {} for name in parts}

    assert classify_opening_shape(payload) == expected


def make_envelope(payload):
    return RawRecordEnvelope(
        namespace="muasamcong",
        table_name="bid_opening_detail",
        resource="bid_opening",
        source_date="2026-01-02",
        run_id="test-run",
        row_ordinal=0,
        file_key="test.parquet",
        file_sha256="a" * 64,
        source_id="IB2600000001",
        source_version="00",
        content_hash="b" * 64,
        ingested_at=datetime(2026, 1, 2, tzinfo=UTC),
        payload=payload,
    )


def make_notice():
    return {
        "id": "notice-1",
        "notifyNo": "IB2600000001",
        "notifyVersion": "00",
        "bidName": "Test opening",
        "processApply": "LDT",
        "bidMode": "1_MTHS",
        "bidNo": "BP2600000001",
        "bidOpenDate": "2026-01-02T09:00:00+07:00",
        "publicDate": "2026-01-01",
    }


def test_ldt_single_opening_revision():
    payload = {
        "notify": {"bidNoContractorResponse": {"bidNotification": make_notice()}},
        "roundmng": {
            "bidoBidroundMngViewDTO": {
                "successBidOpenDate": "2026-01-02T09:15:00+07:00",
                "countContractor": 3,
                "isMultiLot": True,
            }
        },
        "bid_open": {},
        "lot_open_detail": [],
    }

    result = BidOpeningTransformer().transform(make_envelope(payload))

    assert result.quarantine is None
    assert result.revision is not None
    assert result.typed_attributes is not None

    attrs = result.typed_attributes

    assert attrs["notify_no"] == "IB2600000001"
    assert attrs["opening_shape"] == "ldt_single_envelope"
    assert attrs["package_no"] == "BP2600000001"
    assert attrs["total_bidders"] == 3
    assert attrs["is_multi_lot"] is True
    assert attrs["scheduled_open_date"] == "2026-01-02T09:00:00+07:00"
    assert attrs["actual_open_date"] == "2026-01-02T09:15:00+07:00"

    schema_cols = set(metadata.tables["silver.opening_revision"].columns.keys())
    assert schema_cols - set(attrs) == {"observation_id", "is_current"}


def test_cancelled_opening_without_round():
    payload = {
        "notify": {"bidNoContractorResponse": {"bidNotification": make_notice()}},
        "roundmng": {"bidoBidroundMngViewDTO": None},
        "bid_open": {},
        "lot_open_detail": [],
        "cancellation_notice": {
            "bidCancelingResponse": {
                "cancelDate": "2026-01-03T10:00:00+07:00",
                "cancelDecisionDate": "2026-01-03",
                "cancelDecisionNo": "QD-001",
                "cancelReason": "Cancelled by decision",
            }
        },
    }

    result = BidOpeningTransformer().transform(make_envelope(payload))

    assert result.quarantine is None
    assert result.typed_attributes is not None

    attrs = result.typed_attributes

    assert attrs["opening_shape"] == "ldt_single_envelope"
    assert attrs["actual_open_date"] is None
    assert attrs["cancel_date"] == "2026-01-03T10:00:00+07:00"
    assert attrs["cancel_decision_date"] == "2026-01-03"
    assert attrs["cancel_decision_no"] == "QD-001"
    assert attrs["cancel_reason"] == "Cancelled by decision"


def test_ldt_single_opening_phase():
    payload = {
        "notify": {"bidNoContractorResponse": {"bidNotification": make_notice()}},
        "roundmng": {
            "bidoBidroundMngViewDTO": {
                "successBidOpenDate": "2026-01-02T09:15:00+07:00",
                "countContractor": 2,
            }
        },
        "bid_open": {
            "bidSubmissionByContractorViewResponse": {"bidSubmissionDTOList": []}
        },
        "lot_open_detail": [],
    }

    result = BidOpeningTransformer().transform(make_envelope(payload))

    phases = [
        child for child in result.children if child.typed_table == "opening_phase"
    ]

    assert len(phases) == 1

    phase = phases[0].typed_attributes
    assert phase is not None
    assert phase["phase"] == "single"
    assert phase["submission_shape"] == "list"
    assert phase["reported_contractor_count"] == 2
    assert phase["opened_at"] == "2026-01-02T09:15:00+07:00"


def test_ldt_two_envelope_pending_financial():
    payload = {
        "notify": {
            "bidNoContractorResponse": {
                "bidNotification": {
                    **make_notice(),
                    "bidMode": "1_HTHS",
                }
            }
        },
        "roundmng": {
            "bidoBidroundMngViewDTO": {
                "successBidOpenDate": "2026-01-02T09:15:00+07:00",
                "successBidOpenDateTc": None,
                "countContractor": 2,
            }
        },
        "bid_open_technical": {
            "bidSubmissionByContractorViewResponse": {"bidSubmissionDTOList": []}
        },
        "lot_open_detail_technical": [],
    }

    result = BidOpeningTransformer().transform(make_envelope(payload))

    phases = {
        child.typed_attributes["phase"]: child.typed_attributes
        for child in result.children
        if child.typed_table == "opening_phase" and child.typed_attributes is not None
    }

    assert len(phases) == 2
    assert phases["technical"]["submission_shape"] == "list"
    assert phases["financial"]["submission_shape"] == "not_fetched"
    assert phases["financial"]["opened_at"] is None


def test_ldt_single_opening_submission():
    payload = {
        "notify": {"bidNoContractorResponse": {"bidNotification": make_notice()}},
        "roundmng": {
            "bidoBidroundMngViewDTO": {
                "successBidOpenDate": "2026-01-02T09:15:00+07:00",
                "countContractor": 1,
            }
        },
        "bid_open": {
            "bidSubmissionByContractorViewResponse": {
                "bidSubmissionDTOList": [
                    {
                        "id": "submission-001",
                        "contractorCode": "CT001",
                        "contractorName": "Contractor A",
                        "bidPrice": 1000000,
                        "bidFinalPrice": 950000,
                        "bidPriceUnit": "VND",
                        "saleNumber": "5",
                        "techResult": 1,
                        "createdDateBidOpen": "2026-01-02T09:20:00+07:00",
                    }
                ]
            }
        },
        "lot_open_detail": [],
    }

    result = BidOpeningTransformer().transform(make_envelope(payload))

    submissions = [
        child for child in result.children if child.typed_table == "opening_submission"
    ]

    assert len(submissions) == 1

    attrs = submissions[0].typed_attributes
    assert attrs is not None

    assert attrs["source_submission_id"] == "submission-001"
    assert attrs["contractor_code"] == "CT001"
    assert attrs["bid_price"] == "1000000.000000"
    assert attrs["final_bid_price"] == "950000.000000"
    assert attrs["currency"] == "VND"
    assert attrs["phase"] == "single"
    assert attrs["revision_id"] == attrs["opening_revision_id"]


def test_opening_phase_and_submission_assemble():
    payload = {
        "notify": {"bidNoContractorResponse": {"bidNotification": make_notice()}},
        "roundmng": {
            "bidoBidroundMngViewDTO": {
                "successBidOpenDate": "2026-01-02T09:15:00+07:00",
                "countContractor": 1,
                "isMultiLot": False,
            }
        },
        "bid_open": {
            "bidSubmissionByContractorViewResponse": {
                "bidSubmissionDTOList": [
                    {
                        "id": "submission-001",
                        "contractorCode": "CT001",
                        "contractorName": "Contractor A",
                        "bidPrice": 1000000,
                        "bidFinalPrice": 950000,
                        "bidPriceUnit": "VND",
                        "saleNumber": "5",
                        "createdDateBidOpen": "2026-01-02T09:20:00+07:00",
                    }
                ]
            }
        },
        "lot_open_detail": [],
    }

    result = BidOpeningTransformer().transform(make_envelope(payload))

    assert result.quarantine is None
    assert result.revision is not None

    tables = assemble([result.to_dict()])

    assert len(tables["observation"]) == 1
    assert len(tables["opening_revision"]) == 1
    assert len(tables["opening_phase"]) == 1
    assert len(tables["opening_submission"]) == 1

    revision = tables["opening_revision"][0]
    phase = tables["opening_phase"][0]
    submission = tables["opening_submission"][0]

    assert revision["revision_id"] == result.revision.revision_id
    assert revision["is_current"] is True

    assert phase["revision_id"] == revision["revision_id"]
    assert phase["submission_shape"] == "list"

    assert submission["revision_id"] == revision["revision_id"]
    assert submission["opening_revision_id"] == revision["revision_id"]
    assert submission["phase"] == phase["phase"]
    assert submission["contractor_code"] == "CT001"
    assert submission["bid_price"] == "1000000.000000"

    report = validate(tables)

    assert report["accepted"] == 1
    assert report["quarantined"] == 0
    assert report["blocking_issues"] == 0


def test_ldt_lot_submission_assemble():
    payload = {
        "notify": {"bidNoContractorResponse": {"bidNotification": make_notice()}},
        "roundmng": {
            "bidoBidroundMngViewDTO": {
                "successBidOpenDate": "2026-01-02T09:15:00+07:00",
                "countContractor": 1,
            }
        },
        "bid_open": {
            "bidSubmissionByContractorViewResponse": {
                "bidSubmissionDTOList": [
                    {
                        "id": "sub-001",
                        "contractorCode": "CT001",
                        "bidPrice": 1000000,
                    }
                ]
            }
        },
        "lot_open_detail": [
            {
                "id": "lot-detail-001",
                "bidNo": "BP2600000001",
                "lotNo": "PP2600000001",
                "lotName": "Lot A",
                "contractorCode": "CT001",
                "lotPrice": 1000000,
                "lotFinalPrice": 950000,
                "passedYn": "Y",
            }
        ],
    }

    result = BidOpeningTransformer().transform(make_envelope(payload))

    assert result.quarantine is None

    tables = assemble([result.to_dict()])

    assert len(tables["opening_revision"]) == 1
    assert len(tables["opening_submission"]) == 1
    assert len(tables["opening_lot_submission"]) == 1

    submission = tables["opening_submission"][0]
    lot = tables["opening_lot_submission"][0]

    assert lot["revision_id"] == submission["revision_id"]
    assert lot["submission_id"] == submission["submission_id"]
    assert lot["lot_no"] == "PP2600000001"
    assert lot["lot_price"] == "1000000.000000"
    assert lot["lot_final_price"] == "950000.000000"
    assert lot["passed_yn"] is True

    report = validate(tables)
    assert report["blocking_issues"] == 0


def test_adb_nested_lot_submission():
    notice = {
        **make_notice(),
        "processApply": "ADB",
    }

    payload = {
        "opening_adb_wb": {
            "bidNoContractorResponse": {
                "bidNotification": notice,
            },
            "bidoBidroundMngViewDTO": {
                "successBidOpenDate": "2026-01-02T09:00:00",
                "countContractor": 1,
            },
            "bidSubmissionByContractorViewResponse": {
                "bidSubmissionDTOList": [
                    {
                        "id": "sub-adb-001",
                        "contractorCode": "CT001",
                        "bidPrice": "1000000",
                        "bidPriceUnit": "VND",
                        "lotNoValueDTOList": [
                            {
                                "lotNo": "LOT001",
                                "lotPrice": "1000000",
                                "lotPriceUnit": "VND",
                            }
                        ],
                    }
                ],
                "bidoLotOpenDetailDTOS": [],
            },
        }
    }

    result = BidOpeningTransformer().transform(make_envelope(payload))

    tables = assemble([result.to_dict()])

    assert len(tables["opening_submission"]) == 1
    assert len(tables["opening_lot_submission"]) == 1

    submission = tables["opening_submission"][0]
    lot = tables["opening_lot_submission"][0]

    assert lot["submission_id"] == submission["submission_id"]
    assert lot["revision_id"] == submission["revision_id"]
    assert lot["lot_no"] == "LOT001"
    assert lot["lot_price"] == "1000000.000000"


def test_khac_bid_open_views_are_expanded_without_duplicate_parent():
    notice = {
        **make_notice(),
        "processApply": "KHAC",
    }

    payload = {
        "opening_out": {
            "bidoNotifyContractorP": notice,
            "bidoBidroundOutMngViewDTO": {
                "successBidOpenDate": "2026-01-02T09:15:00",
                "countContractor": 1,
            },
            "bidSubmissionByContractorViewResponse": {
                "bidSubmissionDTOList": [
                    {
                        "id": "sub-khac-001",
                        "contractorCode": "CT001",
                        "lotNoValueDTOList": [
                            {
                                "lotNo": "BP2600000001",
                                "bidOpenView": [
                                    {
                                        "lotNo": "PP001",
                                        "bidOpenId": "sub-khac-001",
                                        "contractorCode": "CT001",
                                        "lotPrice": 1000000,
                                    },
                                    {
                                        "lotNo": "PP002",
                                        "bidOpenId": "sub-khac-001",
                                        "contractorCode": "CT001",
                                        "lotPrice": 2000000,
                                    },
                                ],
                            }
                        ],
                    }
                ]
            },
        }
    }

    result = BidOpeningTransformer().transform(make_envelope(payload))
    tables = assemble([result.to_dict()])

    assert len(tables["opening_submission"]) == 1
    assert len(tables["opening_lot_submission"]) == 2

    submission = tables["opening_submission"][0]
    lots = tables["opening_lot_submission"]

    assert {row["lot_no"] for row in lots} == {"PP001", "PP002"}

    assert all(row["submission_id"] == submission["submission_id"] for row in lots)

    assert all(".bidOpenView[" in row["source_path"] for row in lots)

    assert validate(tables)["blocking_issues"] == 0


def make_adb_auxiliary_payload(auxiliary_lots):
    return {
        "opening_adb_wb": {
            "bidNoContractorResponse": {
                "bidNotification": {
                    **make_notice(),
                    "processApply": "ADB",
                }
            },
            "bidoBidroundMngViewDTO": {
                "successBidOpenDate": "2026-01-02T09:15:00",
                "countContractor": 1,
            },
            "bidSubmissionByContractorViewResponse": {
                "bidSubmissionDTOList": [
                    {
                        "id": "sub-001",
                        "contractorCode": "CT001",
                        "bidPrice": "1000000",
                        "bidPriceUnit": "VND",
                        "lotNoValueDTOList": [
                            {
                                "lotNo": "LOT001",
                                "lotPrice": "1000000",
                                "lotPriceUnit": "VND",
                            }
                        ],
                    }
                ],
                "bidoLotOpenDetailDTOS": auxiliary_lots,
            },
        }
    }


def test_matching_auxiliary_lot_not_duplicated():
    payload = make_adb_auxiliary_payload(
        [
            {
                "id": "aux-001",
                "lotNo": "LOT001",
                "contractorCode": "CT001",
                "lotPrice": "1000000",
            }
        ]
    )

    result = BidOpeningTransformer().transform(make_envelope(payload))
    tables = assemble([result.to_dict()])

    assert result.quarantine is None
    assert len(tables["opening_submission"]) == 1
    assert len(tables["opening_lot_submission"]) == 1

    assert not any(issue.code == "unmatched_auxiliary_lot" for issue in result.issues)

    assert validate(tables)["blocking_issues"] == 0


def test_unmatched_auxiliary_lot_creates_issue():
    payload = make_adb_auxiliary_payload(
        [
            {
                "id": "aux-002",
                "lotNo": "LOT002",
                "contractorCode": "CT001",
                "lotPrice": "2000000",
            }
        ]
    )

    result = BidOpeningTransformer().transform(make_envelope(payload))
    tables = assemble([result.to_dict()])

    assert result.quarantine is None

    # Only the original nested lot is currently materialized.
    assert len(tables["opening_lot_submission"]) == 1
    assert tables["opening_lot_submission"][0]["lot_no"] == "LOT001"

    unmatched = [
        issue for issue in result.issues if issue.code == "unmatched_auxiliary_lot"
    ]

    assert len(unmatched) == 1
    assert unmatched[0].severity == "error"
    assert "bidoLotOpenDetailDTOS[0]" in unmatched[0].path


def test_collection_status_single_complete():
    payload = {
        "notify": {},
        "roundmng": {"bidoBidroundMngViewDTO": {}},
        "bid_open": {
            "bidSubmissionByContractorViewResponse": {"bidSubmissionDTOList": []}
        },
        "lot_open_detail": [],
    }

    assert (
        infer_collection_status(
            payload,
            "ldt_single_envelope",
            make_notice(),
        )
        == "complete"
    )


def test_collection_status_awaiting_financial():
    payload = {
        "notify": {},
        "roundmng": {"bidoBidroundMngViewDTO": {"successBidOpenDateTc": None}},
        "bid_open_technical": {
            "bidSubmissionByContractorViewResponse": {"bidSubmissionDTOList": []}
        },
        "lot_open_detail_technical": [],
    }

    assert (
        infer_collection_status(
            payload,
            "ldt_two_envelope",
            make_notice(),
        )
        == "awaiting_financial"
    )


def test_collection_status_cancellation_unresolved():
    payload = {
        "notify": {},
        "roundmng": {"bidoBidroundMngViewDTO": {}},
        "bid_open": {
            "bidSubmissionByContractorViewResponse": {"bidSubmissionDTOList": []}
        },
        "lot_open_detail": [],
        "cancellation_notice": {},
    }

    assert (
        infer_collection_status(
            payload,
            "ldt_single_envelope",
            make_notice(),
        )
        is None
    )


def test_collection_status_missing_ldt_lots_is_unresolved():
    payload = {
        "notify": {},
        "roundmng": {"bidoBidroundMngViewDTO": {}},
        "bid_open": {
            "bidSubmissionByContractorViewResponse": {"bidSubmissionDTOList": []}
        },
        # Missing lot_open_detail
    }

    assert (
        infer_collection_status(
            payload,
            "ldt_single_envelope",
            make_notice(),
        )
        is None
    )


def make_ldt_two_status_payload(published: bool):
    payload = {
        "notify": {},
        "roundmng": {
            "bidoBidroundMngViewDTO": {
                "successBidOpenDateTc": ("2026-01-02T10:00:00" if published else None),
            }
        },
        "bid_open_technical": {
            "bidSubmissionByContractorViewResponse": {
                "bidSubmissionDTOList": [{"id": "sub-t", "contractorCode": "CT001"}]
            }
        },
        "lot_open_detail_technical": [{"lotNo": "LOT001", "contractorCode": "CT001"}],
    }

    if published:
        payload["bid_open_financial"] = {
            "bidSubmissionByContractorViewResponse": {
                "bidSubmissionDTOList": [{"id": "sub-f", "contractorCode": "CT001"}]
            }
        }
        payload["lot_open_detail_financial"] = [
            {"lotNo": "LOT001", "contractorCode": "CT001"}
        ]

    return payload


def test_ldt_two_pending_with_valid_technical_lots():
    payload = make_ldt_two_status_payload(published=False)

    assert (
        infer_collection_status(payload, "ldt_two_envelope", make_notice())
        == "awaiting_financial"
    )


def test_ldt_two_missing_technical_lots():
    payload = make_ldt_two_status_payload(published=False)
    del payload["lot_open_detail_technical"]

    assert infer_collection_status(payload, "ldt_two_envelope", make_notice()) is None


def test_ldt_two_published_with_complete_components():
    payload = make_ldt_two_status_payload(published=True)

    assert (
        infer_collection_status(payload, "ldt_two_envelope", make_notice())
        == "complete"
    )


def test_ldt_two_published_missing_financial_lots():
    payload = make_ldt_two_status_payload(published=True)
    del payload["lot_open_detail_financial"]

    assert infer_collection_status(payload, "ldt_two_envelope", make_notice()) is None


def test_auxiliary_lot_conflicting_price_is_blocking():
    payload = make_adb_auxiliary_payload(
        [
            {
                "id": "aux-001",
                "lotNo": "LOT001",
                "contractorCode": "CT001",
                "lotPrice": "2000000",
            }
        ]
    )

    result = BidOpeningTransformer().transform(make_envelope(payload))
    tables = assemble([result.to_dict()])

    assert result.quarantine is None
    assert len(tables["opening_lot_submission"]) == 1

    lot = tables["opening_lot_submission"][0]

    # Keep the original primary source value.
    assert lot["lot_price"] == "1000000.000000"

    conflicts = [
        issue for issue in result.issues if issue.code == "conflicting_auxiliary_lot"
    ]
    assert len(conflicts) == 1
    assert conflicts[0].severity == "error"

    assert validate(tables)["blocking_issues"] == 1


def test_auxiliary_lot_enriches_missing_field():
    payload = make_adb_auxiliary_payload(
        [
            {
                "id": "aux-001",
                "lotNo": "LOT001",
                "contractorCode": "CT001",
                "lotPrice": "1000000",
                "lotFinalPrice": "950000",
                "passedYn": "Y",
            }
        ]
    )

    result = BidOpeningTransformer().transform(make_envelope(payload))
    tables = assemble([result.to_dict()])

    assert result.quarantine is None
    assert len(tables["opening_lot_submission"]) == 1

    lot = tables["opening_lot_submission"][0]

    assert lot["lot_price"] == "1000000.000000"
    assert lot["lot_final_price"] == "950000.000000"
    assert lot["passed_yn"] is True

    assert any(issue.code == "auxiliary_lot_enriched" for issue in result.issues)

    assert validate(tables)["blocking_issues"] == 0


def test_khac_dual_technical_zero_vs_financial_value():
    notice = {
        **make_notice(),
        "processApply": "KHAC",
        "bidMode": "1_HTHS",
    }

    def make_part(phase: str):
        is_financial = phase == "financial"

        submission_id = "sub-fin" if is_financial else "sub-tech"
        price = "1000000" if is_financial else "0"

        return {
            "bidoNotifyContractorP": dict(notice),
            "bidoBidroundOutMngViewDTO": {
                "successBidOpenDate": "2026-01-02T09:00:00",
                "successBidOpenDateTc": "2026-01-02T10:00:00",
                "countContractor": 1,
            },
            "bidSubmissionByContractorViewResponse": {
                "bidSubmissionDTOList": [
                    {
                        "id": submission_id,
                        "contractorCode": "CT001",
                        "lotNoValueDTOList": [
                            {
                                "lotNo": "BP001",
                                "bidOpenView": [
                                    {
                                        "lotNo": "PP001",
                                        "bidOpenId": submission_id,
                                        "contractorCode": "CT001",
                                        "lotPrice": price,
                                        "lotFinalPrice": price,
                                    }
                                ],
                            }
                        ],
                    }
                ],
                "bidoLotOpenDetailDTOS": [
                    {
                        "id": f"aux-{phase}",
                        "lotNo": "PP001",
                        "bidOpenId": "sub-tech",
                        "contractorCode": "CT001",
                        "lotPrice": "1000000",
                        "lotFinalPrice": "1000000",
                    }
                ],
            },
        }

    payload = {
        "opening_out_technical": make_part("technical"),
        "opening_out_financial": make_part("financial"),
    }

    result = BidOpeningTransformer().transform(make_envelope(payload))
    tables = assemble([result.to_dict()])

    assert result.quarantine is None

    assert len(tables["opening_submission"]) == 2
    assert len(tables["opening_lot_submission"]) == 2

    lots = {row["phase"]: row for row in tables["opening_lot_submission"]}

    assert lots["technical"]["lot_price"] == "0.000000"
    assert lots["financial"]["lot_price"] == "1000000.000000"

    assert not any(issue.severity == "error" for issue in result.issues)

    assert any(
        issue.code == "technical_zero_vs_financial_value" for issue in result.issues
    )

    assert validate(tables)["blocking_issues"] == 0
