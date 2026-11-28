"""Bronze JSON schema and Silver Iceberg DDL for resource: project (Dự án đầu tư)."""

from __future__ import annotations

from pyspark.sql.types import ArrayType, StringType, StructField, StructType

# Explicit Bronze JSON schema for project_detail.
# CRITICAL: All monetary fields (investTotal) are read as StringType so from_json
# never loses precision through IEEE-754 double before DECIMAL(38, 6) casting.
PROJECT_DTO_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("no", StringType(), True),
        StructField("name", StringType(), True),
        StructField("version", StringType(), True),
        StructField("investorCode", StringType(), True),
        StructField("investorName", StringType(), True),
        StructField("investTotal", StringType(), True),
        StructField("investTotalUnit", StringType(), True),
        StructField("investUnit", StringType(), True),
        StructField("decisionNo", StringType(), True),
        StructField("decisionDate", StringType(), True),
        StructField("publicDate", StringType(), True),
        StructField("provCode", StringType(), True),
        StructField("provName", StringType(), True),
        StructField("districtCode", StringType(), True),
        StructField("districtName", StringType(), True),
        StructField("plocation", StringType(), True),
        StructField("status", StringType(), True),
    ]
)

PROJECT_PAYLOAD_SCHEMA = StructType(
    [
        StructField("projectDTO", PROJECT_DTO_SCHEMA, True),
        # Top-level fallback fields present in some muasamcong payloads
        StructField("id", StringType(), True),
        StructField("no", StringType(), True),
        StructField("name", StringType(), True),
        StructField("pversion", StringType(), True),
        StructField("version", StringType(), True),
        StructField("investorCode", StringType(), True),
        StructField("investorName", StringType(), True),
        StructField("investTotal", StringType(), True),
        StructField("investTotalUnit", StringType(), True),
        StructField("investUnit", StringType(), True),
        StructField("decisionNo", StringType(), True),
        StructField("decisionDate", StringType(), True),
        StructField("publicDate", StringType(), True),
        StructField("provCode", StringType(), True),
        StructField("provName", StringType(), True),
        StructField("districtCode", StringType(), True),
        StructField("districtName", StringType(), True),
        StructField("plocation", StringType(), True),
        StructField("status", StringType(), True),
    ]
)

PLOCATION_ITEM_SCHEMA = ArrayType(
    StructType(
        [
            StructField("provCode", StringType(), True),
            StructField("provName", StringType(), True),
            StructField("districtCode", StringType(), True),
            StructField("districtName", StringType(), True),
            StructField("wardCode", StringType(), True),
            StructField("wardName", StringType(), True),
        ]
    )
)

PROJECT_ICEBERG_DDLS: dict[str, str] = {
    "project_revision": """
        revision_id STRING NOT NULL,
        entity_id STRING NOT NULL,
        source_id STRING,
        source_version STRING,
        source_date DATE NOT NULL,
        ingested_at TIMESTAMP,
        run_id STRING,
        content_hash STRING,
        project_no STRING,
        title STRING,
        investor_code STRING,
        investor_name STRING,
        total_investment DECIMAL(38, 6),
        currency STRING,
        decision_no STRING,
        decision_date DATE,
        public_date DATE,
        public_date_raw STRING,
        prov_code STRING,
        district_code STRING,
        status_raw STRING,
        dq_status STRING NOT NULL
    """,
    "project_location": """
        location_id STRING NOT NULL,
        revision_id STRING NOT NULL,
        source_date DATE NOT NULL,
        ordinal INT NOT NULL,
        province_code STRING,
        province_name STRING,
        district_code STRING,
        district_name STRING,
        ward_code STRING,
        ward_name STRING,
        location_name STRING
    """,
}
