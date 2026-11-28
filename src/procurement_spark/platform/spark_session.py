"""SparkSession factory configured for SeaweedFS S3A + Apache Iceberg REST Catalog + WAP."""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

from procurement_spark.platform.config import SparkSilverSettings


def build_spark_session(
    settings: SparkSilverSettings,
    *,
    local_unit_test: bool = False,
) -> SparkSession:
    """Create a SparkSession for either full S3A+Iceberg execution or lightweight local unit tests."""
    if settings.java_home:
        os.environ["JAVA_HOME"] = settings.java_home
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    builder = (
        SparkSession.builder.appName(settings.spark_app_name)
        .master(settings.spark_master)
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.sql.shuffle.partitions", str(settings.spark_shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        .config("spark.ui.enabled", "false")
    )

    if settings.spark_driver_host and not local_unit_test:
        os.environ.setdefault("SPARK_LOCAL_IP", settings.spark_driver_host)
        builder = builder.config("spark.driver.host", settings.spark_driver_host)

    if settings.spark_local_dir and not local_unit_test:
        os.makedirs(settings.spark_local_dir, exist_ok=True)
        builder = builder.config("spark.local.dir", settings.spark_local_dir)

    if local_unit_test:
        return builder.getOrCreate()

    if not settings.spark_use_bundled_jars:
        packages = ",".join(
            [
                settings.iceberg_spark_package,
                settings.iceberg_aws_bundle_package,
                settings.hadoop_aws_package,
            ]
        )
        builder = builder.config("spark.jars.packages", packages)

    cat = settings.iceberg_catalog_name
    builder = (
        builder.config("spark.executor.memory", settings.spark_executor_memory)
        .config("spark.executor.cores", str(settings.spark_executor_cores))
        # Iceberg SQL extensions + REST Catalog
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{cat}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{cat}.type", "rest")
        .config(f"spark.sql.catalog.{cat}.uri", settings.iceberg_catalog_uri)
        .config(f"spark.sql.catalog.{cat}.warehouse", settings.iceberg_warehouse)
        .config(f"spark.sql.catalog.{cat}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{cat}.s3.endpoint", settings.s3_endpoint)
        .config(f"spark.sql.catalog.{cat}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{cat}.s3.access-key-id", settings.s3_access_key)
        .config(f"spark.sql.catalog.{cat}.s3.secret-access-key", settings.s3_secret_key)
        .config(f"spark.sql.catalog.{cat}.client.region", settings.s3_region)
        # Enable Write-Audit-Publish (WAP) branching natively
        .config("spark.wap.enabled", "true")
        # Hadoop S3A for reading raw Bronze Parquet directly from SeaweedFS
        .config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", settings.s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", settings.s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Map s3:// scheme to S3AFileSystem so Iceberg remove_orphan_files can list s3:// warehouse paths
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.endpoint", settings.s3_endpoint)
        .config("spark.hadoop.fs.s3.access.key", settings.s3_access_key)
        .config("spark.hadoop.fs.s3.secret.key", settings.s3_secret_key)
        .config("spark.hadoop.fs.s3.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3.connection.ssl.enabled", "false")
        # Bound columnar batch sizes so wide raw-JSON Parquet rows never spike JVM heap
        .config("spark.sql.inMemoryColumnarStorage.batchSize", "1000")
        .config("spark.sql.parquet.columnarReaderBatchSize", "1024")
        # Bound Spark UI / SQL execution history so long-lived Drivers never leak heap
        .config("spark.ui.retainedStages", "100")
        .config("spark.sql.ui.retainedExecutions", "50")
    )
    return builder.getOrCreate()
