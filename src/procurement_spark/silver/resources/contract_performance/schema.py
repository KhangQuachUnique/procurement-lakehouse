"""Bronze JSON schemas and Silver Iceberg DDLs for resource: contract_performance."""

from __future__ import annotations

from pyspark.sql.types import StringType, StructField, StructType

CONTRACT_DTO_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("contractCode", StringType(), True),
        StructField("contractNo", StringType(), True),
        StructField("contractVersion", StringType(), True),
        StructField("contractName", StringType(), True),
        StructField("bidName", StringType(), True),
        StructField("investorCode", StringType(), True),
        StructField("investorName", StringType(), True),
        StructField("contractorCode", StringType(), True),
        StructField("contractorName", StringType(), True),
        StructField("priceAfter", StringType(), True),  # STRING -> DECIMAL(38,6)
        StructField("priceAfterUnit", StringType(), True),
        StructField("notifyId", StringType(), True),
        StructField("notifyNo", StringType(), True),
        StructField("notifyVersion", StringType(), True),
        StructField("resultId", StringType(), True),
        StructField("resultVersion", StringType(), True),
        StructField("contractSignDate", StringType(), True),
        StructField("contractEffectiveDate", StringType(), True),
        StructField("publicDate", StringType(), True),
    ]
)

CONTRACT_PAYLOAD_SCHEMA = StructType(
    [
        StructField("contract", CONTRACT_DTO_SCHEMA, True),
    ]
)

PERFORMANCE_DTO_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("idNo", StringType(), True),
        StructField("version", StringType(), True),
        StructField("contractCode", StringType(), True),
        StructField("contractVersion", StringType(), True),
        StructField("investorCode", StringType(), True),
        StructField("investorName", StringType(), True),
        StructField("contractorCode", StringType(), True),
        StructField("contractorName", StringType(), True),
        StructField("contractStatus", StringType(), True),
        StructField("contractQualityPer", StringType(), True),
        StructField("isContractComplete", StringType(), True),
        StructField("publicDate", StringType(), True),
        StructField("completePlanDate", StringType(), True),
        StructField("completeRealityDate", StringType(), True),
        StructField("completeAdjustedDate", StringType(), True),
    ]
)

PERFORMANCE_PAYLOAD_SCHEMA = StructType(
    [
        StructField("performance", PERFORMANCE_DTO_SCHEMA, True),
        StructField(
            "resolution",
            StructType([StructField("status", StringType(), True)]),
            True,
        ),
    ]
)

CONTRACT_PERF_ICEBERG_DDLS: dict[str, str] = {
    "contract_revision": """
        revision_id STRING NOT NULL,
        entity_id STRING NOT NULL,
        source_contract_id STRING,
        source_version STRING,
        source_date DATE NOT NULL,
        ingested_at TIMESTAMP,
        run_id STRING,
        content_hash STRING,
        contract_code STRING,
        contract_no STRING,
        contract_version STRING,
        contract_name STRING,
        package_name STRING,
        investor_code STRING,
        investor_name STRING,
        contractor_code STRING,
        contractor_name STRING,
        price_after_tax DECIMAL(38, 6),
        currency STRING,
        notice_source_id STRING,
        notice_no STRING,
        notice_version STRING,
        result_source_id STRING,
        result_version STRING,
        signed_date DATE,
        effective_date DATE,
        public_date DATE,
        published_at_raw STRING,
        dq_status STRING NOT NULL
    """,
    "contract_performance_revision": """
        revision_id STRING NOT NULL,
        entity_id STRING NOT NULL,
        source_evaluation_id STRING,
        source_version STRING,
        source_date DATE NOT NULL,
        ingested_at TIMESTAMP,
        run_id STRING,
        content_hash STRING,
        evaluation_no STRING,
        evaluation_version STRING,
        contract_code STRING,
        contract_version STRING,
        investor_code STRING,
        investor_name STRING,
        contractor_code STRING,
        contractor_name STRING,
        progress_status_code STRING,
        quality_status_code STRING,
        is_completed BOOLEAN,
        public_date DATE,
        planned_completion_date DATE,
        actual_completion_date DATE,
        adjusted_completion_date DATE,
        published_at_raw STRING,
        dq_status STRING NOT NULL
    """,
}
