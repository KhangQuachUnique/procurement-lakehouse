"""Bronze JSON schemas and Silver Iceberg DDLs for resource: khlcnt (Plan + Package)."""

from __future__ import annotations

from pyspark.sql.types import ArrayType, StringType, StructField, StructType

# 1. Bronze JSON schema for khlcnt_plan_detail (root: bidPoBidpPlanProjectDetailView)
PLAN_DTO_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("planNo", StringType(), True),
        StructField("name", StringType(), True),
        StructField("investorName", StringType(), True),
        StructField("pid", StringType(), True),
        StructField("pno", StringType(), True),
        StructField("planType", StringType(), True),
        StructField("investTotal", StringType(), True),  # STRING -> DECIMAL(38,6)
        StructField("investTotalUnit", StringType(), True),
        StructField("decisionDate", StringType(), True),
        StructField("decisionNo", StringType(), True),
        StructField("decisionAgency", StringType(), True),
        StructField("bidPack", StringType(), True),
        StructField("location", StringType(), True),
        StructField("publicDate", StringType(), True),
        StructField("status", StringType(), True),
    ]
)

PLAN_PAYLOAD_SCHEMA = StructType(
    [
        StructField("bidPoBidpPlanProjectDetailView", PLAN_DTO_SCHEMA, True),
    ]
)

# 2. Bronze JSON schema for khlcnt_bid_package_detail (top-level Package DTO)
PACKAGE_LOCATION_ITEM_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("provCode", StringType(), True),
        StructField("provName", StringType(), True),
        StructField("districtCode", StringType(), True),
        StructField("districtName", StringType(), True),
        StructField("wardCode", StringType(), True),
        StructField("wardName", StringType(), True),
    ]
)

PACKAGE_LOT_ITEM_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("lotNo", StringType(), True),
        StructField("lotName", StringType(), True),
        StructField("lotPrice", StringType(), True),  # STRING -> DECIMAL(38,6)
        StructField("lotPriceUnit", StringType(), True),
        StructField("medicineCode", StringType(), True),
        StructField("quantity", StringType(), True),  # STRING -> DECIMAL(38,6)
        StructField("uom", StringType(), True),
        StructField("pricePlan", StringType(), True),  # STRING -> DECIMAL(38,6)
    ]
)

PACKAGE_PAYLOAD_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("bidNo", StringType(), True),
        StructField("bidName", StringType(), True),
        StructField("planId", StringType(), True),
        StructField("planNo", StringType(), True),
        StructField("bidPrice", StringType(), True),  # STRING -> DECIMAL(38,6)
        StructField("bidPriceUnit", StringType(), True),
        StructField("processApply", StringType(), True),
        StructField("bidField", StringType(), True),
        StructField("bidForm", StringType(), True),
        StructField("bidMode", StringType(), True),
        StructField("capitalDetail", StringType(), True),
        StructField("ctype", StringType(), True),
        StructField("cperiod", StringType(), True),
        StructField("cperiodUnit", StringType(), True),
        StructField("isDomestic", StringType(), True),
        StructField("isInternet", StringType(), True),
        StructField("bidStartUnit", StringType(), True),
        StructField("bidStartYear", StringType(), True),
        StructField("bidStartQuarter", StringType(), True),
        StructField("bidStartMonth", StringType(), True),
        StructField("createdDate", StringType(), True),
        StructField("originalCreateDate", StringType(), True),
        StructField("exchangeDate", StringType(), True),
        StructField("bidLocation", ArrayType(PACKAGE_LOCATION_ITEM_SCHEMA), True),
        StructField("bidpBidLotList", ArrayType(PACKAGE_LOT_ITEM_SCHEMA), True),
    ]
)

# 3. Silver Iceberg DDLs
KHLCNT_ICEBERG_DDLS: dict[str, str] = {
    "plan_revision": """
        revision_id STRING NOT NULL,
        entity_id STRING NOT NULL,
        source_id STRING,
        source_version STRING,
        source_date DATE NOT NULL,
        ingested_at TIMESTAMP,
        run_id STRING,
        content_hash STRING,
        plan_no STRING,
        title STRING,
        investor_name STRING,
        source_project_id STRING,
        project_no STRING,
        plan_type STRING,
        total_investment DECIMAL(38, 6),
        currency STRING,
        decision_date DATE,
        decision_no STRING,
        decision_agency STRING,
        bid_pack INT,
        location STRING,
        public_date DATE,
        status_raw STRING,
        dq_status STRING NOT NULL
    """,
    "package_revision": """
        revision_id STRING NOT NULL,
        entity_id STRING NOT NULL,
        source_id STRING,
        source_date DATE NOT NULL,
        ingested_at TIMESTAMP,
        run_id STRING,
        content_hash STRING,
        package_no STRING,
        title STRING,
        plan_no STRING,
        source_plan_id STRING,
        bid_price DECIMAL(38, 6),
        currency STRING,
        process_apply STRING,
        bid_field STRING,
        bid_form STRING,
        bid_mode STRING,
        capital_detail STRING,
        contract_type STRING,
        execution_period INT,
        execution_period_unit STRING,
        is_domestic BOOLEAN,
        is_internet BOOLEAN,
        bid_start_unit STRING,
        bid_start_year INT,
        bid_start_quarter STRING,
        bid_start_month INT,
        public_date DATE,
        dq_status STRING NOT NULL
    """,
    "package_location": """
        location_id STRING NOT NULL,
        revision_id STRING NOT NULL,
        source_date DATE NOT NULL,
        location_ordinal INT NOT NULL,
        province_code STRING,
        province_name STRING,
        district_code STRING,
        district_name STRING,
        ward_code STRING,
        ward_name STRING
    """,
    "package_lot": """
        lot_id STRING NOT NULL,
        revision_id STRING NOT NULL,
        source_date DATE NOT NULL,
        source_lot_id STRING,
        lot_ordinal INT NOT NULL,
        lot_no STRING,
        lot_name STRING,
        lot_price DECIMAL(38, 6),
        currency STRING,
        medicine_code STRING,
        quantity DECIMAL(38, 6),
        uom STRING,
        planned_unit_price DECIMAL(38, 6)
    """,
}
