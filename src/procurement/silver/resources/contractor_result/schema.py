"""Schemas and Iceberg DDLs for Spark Silver resource: contractor_result."""

from __future__ import annotations

from pyspark.sql import types as T

_CONTRACTOR_SCHEMA = T.StructType(
    [
        T.StructField("id", T.StringType()),
        T.StructField("orgCode", T.StringType()),
        T.StructField("taxCode", T.StringType()),
        T.StructField("orgFullname", T.StringType()),
        T.StructField("newContractorName", T.StringType()),
        T.StructField("contractorName", T.StringType()),
        T.StructField("taxNation", T.StringType()),
        T.StructField("ventureCode", T.StringType()),
        T.StructField("ventureName", T.StringType()),
        T.StructField("isConsortium", T.StringType()),
        T.StructField("is_consortium", T.StringType()),
        T.StructField("role", T.StringType()),
        T.StructField("bidResult", T.StringType()),
        T.StructField("bidWiningPrice", T.StringType()),
    ]
)

_LOT_SCHEMA = T.StructType(
    [
        T.StructField("id", T.StringType()),
        T.StructField("lotNo", T.StringType()),
        T.StructField("bidNo", T.StringType()),
        T.StructField("lotName", T.StringType()),
        T.StructField("bidName", T.StringType()),
        T.StructField("lotPrice", T.StringType()),
        T.StructField("lotPriceUnit", T.StringType()),
        T.StructField("currency", T.StringType()),
        T.StructField("bidEstimatePriceUnit", T.StringType()),
        T.StructField("winningCode", T.StringType()),
        T.StructField("status", T.StringType()),
        T.StructField("contractorList", T.ArrayType(_CONTRACTOR_SCHEMA)),
    ]
)

_RESULT_ROOT_FIELDS = [
    T.StructField("id", T.StringType()),
    T.StructField("resultVersion", T.StringType()),
    T.StructField("notifyNo", T.StringType()),
    T.StructField("notifyVersion", T.StringType()),
    T.StructField("bidId", T.StringType()),
    T.StructField("bidNo", T.StringType()),
    T.StructField("planNo", T.StringType()),
    T.StructField("bidName", T.StringType()),
    T.StructField("procuringEntityCode", T.StringType()),
    T.StructField("procuringEntityName", T.StringType()),
    T.StructField("investorCode", T.StringType()),
    T.StructField("investorName", T.StringType()),
    T.StructField("decisionNo", T.StringType()),
    T.StructField("decisionDate", T.StringType()),
    T.StructField("decisionAgency", T.StringType()),
    T.StructField("bidPriceUnit", T.StringType()),
    T.StructField("bidEstimatePriceUnit", T.StringType()),
    T.StructField("publicDate", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("lotResultDTO", T.ArrayType(_LOT_SCHEMA)),
]

_RESULT_ROOT_SCHEMA = T.StructType(_RESULT_ROOT_FIELDS)

RESULT_PAYLOAD_SCHEMA = T.StructType(
    [
        T.StructField("bideContractorInputResultDTO", _RESULT_ROOT_SCHEMA),
        *_RESULT_ROOT_FIELDS,
    ]
)

RESULT_REVISION_DDL = """
    revision_id STRING,
    entity_id STRING,
    result_id STRING,
    result_version STRING,
    source_version STRING,
    source_date DATE,
    ingested_at TIMESTAMP,
    run_id STRING,
    content_hash STRING,
    notify_no STRING,
    notify_version STRING,
    business_number STRING,
    bid_id STRING,
    package_no STRING,
    plan_no STRING,
    title STRING,
    procuring_entity_code STRING,
    procuring_entity_name STRING,
    investor_code STRING,
    investor_name STRING,
    decision_no STRING,
    decision_date DATE,
    decision_agency STRING,
    total_winning_price DECIMAL(38, 6),
    currency STRING,
    public_date DATE,
    status_raw STRING,
    dq_status STRING
"""

AWARD_LOT_DDL = """
    award_lot_id STRING,
    revision_id STRING,
    source_date DATE,
    ordinal INT,
    source_lot_id STRING,
    lot_no STRING,
    lot_name STRING,
    lot_price DECIMAL(38, 6),
    winning_price DECIMAL(38, 6),
    currency STRING,
    winning_code STRING,
    status STRING
"""

AWARD_CONTRACTOR_DDL = """
    award_contractor_id STRING,
    award_lot_id STRING,
    revision_id STRING,
    source_date DATE,
    ordinal INT,
    contractor_code STRING,
    contractor_name STRING,
    tax_code STRING,
    country STRING,
    is_consortium BOOLEAN,
    venture_code STRING,
    venture_name STRING,
    role STRING,
    awarded_price DECIMAL(38, 6),
    bid_result INT,
    price_source STRING,
    currency STRING
"""

CONTRACTOR_RESULT_ICEBERG_DDLS: dict[str, str] = {
    "result_revision": RESULT_REVISION_DDL,
    "award_lot": AWARD_LOT_DDL,
    "award_contractor": AWARD_CONTRACTOR_DDL,
}
