"""Schemas and Iceberg DDLs for Spark Silver resource: bid_opening."""

from __future__ import annotations

from pyspark.sql import types as T

_NOTIFY_ROOT_SCHEMA = T.StructType(
    [
        T.StructField("notifyNo", T.StringType()),
        T.StructField("notifyVersion", T.StringType()),
        T.StructField("bidName", T.StringType()),
        T.StructField("processApply", T.StringType()),
        T.StructField("bidForm", T.StringType()),
        T.StructField("bidMode", T.StringType()),
        T.StructField("bidNo", T.StringType()),
        T.StructField("bidId", T.StringType()),
        T.StructField("bidOpenDate", T.StringType()),
        T.StructField("publicDate", T.StringType()),
        T.StructField("status", T.StringType()),
        T.StructField("cancelDate", T.StringType()),
        T.StructField("cancelDecisionDate", T.StringType()),
        T.StructField("cancelDecisionNo", T.StringType()),
        T.StructField("cancelReason", T.StringType()),
    ]
)

_ROUND_INFO_SCHEMA = T.StructType(
    [
        T.StructField("successBidOpenDate", T.StringType()),
        T.StructField("successBidOpenDateTc", T.StringType()),
        T.StructField("countContractor", T.StringType()),
        T.StructField("isMultiLot", T.StringType()),
    ]
)

_CANCEL_RESP_SCHEMA = T.StructType(
    [
        T.StructField("cancelDate", T.StringType()),
        T.StructField("cancelDecisionDate", T.StringType()),
        T.StructField("cancelDecisionNo", T.StringType()),
        T.StructField("cancelReason", T.StringType()),
    ]
)

_LOT_ITEM_FIELDS = [
    T.StructField("id", T.StringType()),
    T.StructField("bidOpenId", T.StringType()),
    T.StructField("bidNo", T.StringType()),
    T.StructField("lotNo", T.StringType()),
    T.StructField("lotName", T.StringType()),
    T.StructField("contractorCode", T.StringType()),
    T.StructField("contractorName", T.StringType()),
    T.StructField("lotPrice", T.StringType()),
    T.StructField("lotFinalPrice", T.StringType()),
    T.StructField("discountPercent", T.StringType()),
    T.StructField("saleNumber", T.StringType()),
    T.StructField("lotPriceUnit", T.StringType()),
    T.StructField("passedYn", T.StringType()),
    T.StructField("techScore", T.StringType()),
    T.StructField("evalPrice", T.StringType()),
    T.StructField("ranking", T.StringType()),
]

_LOT_ITEM_SCHEMA = T.StructType(_LOT_ITEM_FIELDS)

_NESTED_LOT_PARENT_SCHEMA = T.StructType(
    [
        *_LOT_ITEM_FIELDS,
        T.StructField("bidOpenView", T.ArrayType(_LOT_ITEM_SCHEMA)),
    ]
)

_SUBMISSION_ITEM_SCHEMA = T.StructType(
    [
        T.StructField("id", T.StringType()),
        T.StructField("lotNo", T.StringType()),
        T.StructField("contractorCode", T.StringType()),
        T.StructField("contractorName", T.StringType()),
        T.StructField("ventureCode", T.StringType()),
        T.StructField("ventureName", T.StringType()),
        T.StructField("bidPrice", T.StringType()),
        T.StructField("bidPriceUnit", T.StringType()),
        T.StructField("saleNumber", T.StringType()),
        T.StructField("bidFinalPrice", T.StringType()),
        T.StructField("techResult", T.StringType()),
        T.StructField("techScore", T.StringType()),
        T.StructField("createdDateBidOpen", T.StringType()),
        T.StructField("lotNoValueDTOList", T.ArrayType(_NESTED_LOT_PARENT_SCHEMA)),
    ]
)

_SUBMISSION_VIEW_RESP_SCHEMA = T.StructType(
    [
        T.StructField("bidSubmissionDTOList", T.ArrayType(_SUBMISSION_ITEM_SCHEMA)),
    ]
)

_BID_OPEN_PART_SCHEMA = T.StructType(
    [
        T.StructField("error", T.StringType()),
        T.StructField("success", T.StringType()),
        T.StructField("bidSubmissionByContractorViewResponse", _SUBMISSION_VIEW_RESP_SCHEMA),
    ]
)

_OPENING_OUT_PART_SCHEMA = T.StructType(
    [
        T.StructField("error", T.StringType()),
        T.StructField("success", T.StringType()),
        T.StructField("bidoNotifyContractorP", _NOTIFY_ROOT_SCHEMA),
        T.StructField("bidoBidroundOutMngViewDTO", _ROUND_INFO_SCHEMA),
        T.StructField("bidSubmissionByContractorViewResponse", _SUBMISSION_VIEW_RESP_SCHEMA),
    ]
)

_OPENING_ADB_PART_SCHEMA = T.StructType(
    [
        T.StructField("error", T.StringType()),
        T.StructField("success", T.StringType()),
        T.StructField(
            "bidNoContractorResponse",
            T.StructType([T.StructField("bidNotification", _NOTIFY_ROOT_SCHEMA)]),
        ),
        T.StructField("bidoBidroundMngViewDTO", _ROUND_INFO_SCHEMA),
        T.StructField("bidSubmissionByContractorViewResponse", _SUBMISSION_VIEW_RESP_SCHEMA),
    ]
)

BID_OPENING_PAYLOAD_SCHEMA = T.StructType(
    [
        T.StructField(
            "notify",
            T.StructType(
                [
                    T.StructField(
                        "bidNoContractorResponse",
                        T.StructType([T.StructField("bidNotification", _NOTIFY_ROOT_SCHEMA)]),
                    )
                ]
            ),
        ),
        T.StructField(
            "roundmng",
            T.StructType([T.StructField("bidoBidroundMngViewDTO", _ROUND_INFO_SCHEMA)]),
        ),
        T.StructField(
            "cancellation_notice",
            T.StructType([T.StructField("bidCancelingResponse", _CANCEL_RESP_SCHEMA)]),
        ),
        T.StructField("bid_open", _BID_OPEN_PART_SCHEMA),
        T.StructField("bid_open_technical", _BID_OPEN_PART_SCHEMA),
        T.StructField("bid_open_financial", _BID_OPEN_PART_SCHEMA),
        T.StructField("lot_open_detail", T.ArrayType(_LOT_ITEM_SCHEMA)),
        T.StructField("lot_open_detail_technical", T.ArrayType(_LOT_ITEM_SCHEMA)),
        T.StructField("lot_open_detail_financial", T.ArrayType(_LOT_ITEM_SCHEMA)),
        T.StructField("opening_out", _OPENING_OUT_PART_SCHEMA),
        T.StructField("opening_out_technical", _OPENING_OUT_PART_SCHEMA),
        T.StructField("opening_out_financial", _OPENING_OUT_PART_SCHEMA),
        T.StructField("opening_adb_wb", _OPENING_ADB_PART_SCHEMA),
    ]
)

OPENING_REVISION_DDL = """
    revision_id STRING,
    entity_id STRING,
    notify_no STRING,
    notify_version STRING,
    source_version STRING,
    source_date DATE,
    ingested_at TIMESTAMP,
    run_id STRING,
    content_hash STRING,
    business_number STRING,
    title STRING,
    process_apply STRING,
    bid_form STRING,
    bid_mode STRING,
    package_no STRING,
    package_id STRING,
    opening_shape STRING,
    is_multi_lot BOOLEAN,
    scheduled_open_date TIMESTAMP,
    technical_open_date TIMESTAMP,
    financial_open_date TIMESTAMP,
    actual_open_date TIMESTAMP,
    cancel_date TIMESTAMP,
    cancel_decision_date DATE,
    cancel_decision_no STRING,
    cancel_reason STRING,
    collection_status STRING,
    total_bidders INT,
    public_date DATE,
    status_raw STRING,
    dq_status STRING
"""

OPENING_PHASE_DDL = """
    phase_id STRING,
    revision_id STRING,
    source_date DATE,
    phase STRING,
    source_path STRING,
    opened_at TIMESTAMP,
    reported_contractor_count INT,
    submission_shape STRING
"""

OPENING_SUBMISSION_DDL = """
    submission_id STRING,
    revision_id STRING,
    source_date DATE,
    ordinal INT,
    source_submission_id STRING,
    phase STRING,
    lot_no STRING,
    contractor_code STRING,
    contractor_name STRING,
    venture_code STRING,
    venture_name STRING,
    bid_price DECIMAL(38, 6),
    currency STRING,
    discount_rate DECIMAL(10, 4),
    final_bid_price DECIMAL(38, 6),
    tech_result STRING,
    tech_score DECIMAL(18, 6),
    submission_time TIMESTAMP
"""

OPENING_LOT_SUBMISSION_DDL = """
    lot_submission_id STRING,
    revision_id STRING,
    submission_id STRING,
    source_date DATE,
    phase STRING,
    source_lot_id STRING,
    bid_no STRING,
    lot_no STRING,
    lot_name STRING,
    contractor_code STRING,
    contractor_name STRING,
    lot_price DECIMAL(38, 6),
    lot_final_price DECIMAL(38, 6),
    discount_rate DECIMAL(10, 4),
    currency STRING,
    passed_yn BOOLEAN,
    tech_score DECIMAL(18, 6),
    evaluation_price DECIMAL(38, 6),
    evaluation_rank INT
"""

BID_OPENING_ICEBERG_DDLS: dict[str, str] = {
    "opening_revision": OPENING_REVISION_DDL,
    "opening_phase": OPENING_PHASE_DDL,
    "opening_submission": OPENING_SUBMISSION_DDL,
    "opening_lot_submission": OPENING_LOT_SUBMISSION_DDL,
}
