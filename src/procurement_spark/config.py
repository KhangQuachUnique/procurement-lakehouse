"""Explicit runtime configuration for the Spark Silver engine.

No implicit dotenv loading on import: callers explicitly load environment or call from_environment().
"""

from __future__ import annotations

import os
import socket
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict, Field


def detect_lan_ip(target_host: str = "1.1.1.1") -> str:
    """Detect this host's non-loopback LAN IPv4 address via UDP route lookup (sends no packets)."""
    probe_host = target_host if target_host and target_host not in {"localhost", "127.0.0.1"} else "1.1.1.1"
    for host in (probe_host, "1.1.1.1", "8.8.8.8"):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                sock.connect((host, 80))
                ip = sock.getsockname()[0]
                if ip and not ip.startswith("127."):
                    return str(ip)
        except OSError:
            continue
    return "127.0.0.1"


def _extract_spark_master_host(master_url: str) -> str | None:
    if not master_url.startswith("spark://"):
        return None
    parsed = urlparse(master_url)
    return parsed.hostname


def _default_spark_local_dir() -> str:
    """Return an SSD-backed scratch directory across OSes (avoids Linux /tmp tmpfs)."""
    if env_dir := os.getenv("SPARK_LOCAL_DIR") or os.getenv("SPARK_LOCAL_DIRS"):
        return env_dir
    if sys.platform.startswith("linux"):
        return "/var/tmp/spark-scratch"
    return str(Path(tempfile.gettempdir()) / "spark-scratch")


def _detect_java_home() -> str | None:
    if env_java := os.getenv("SPARK_JAVA_HOME") or os.getenv("JAVA_HOME"):
        return env_java
    for candidate in (
        "/usr/lib/jvm/java-21-openjdk",
        "/usr/lib/jvm/java-17-openjdk",
    ):
        if Path(candidate).is_dir():
            return candidate
    return None


class SparkSilverSettings(BaseModel):
    """Immutable runtime settings for Spark Silver jobs."""

    model_config = ConfigDict(frozen=True)

    # PostgreSQL Control Plane
    app_database_url: str = Field(min_length=1)

    # Object Storage (SeaweedFS S3 Gateway)
    s3_endpoint: str = "http://127.0.0.1:8333"
    s3_access_key: str = "procurement_admin"
    s3_secret_key: str = ""
    s3_bucket: str = "procurement-lakehouse"
    s3_region: str = "us-east-1"

    # Iceberg REST Catalog
    iceberg_catalog_name: str = "lake"
    iceberg_catalog_uri: str = "http://127.0.0.1:8181"
    iceberg_warehouse: str = "s3://warehouse"
    iceberg_namespace: str = "silver_spark"

    # Spark Runtime (in local[*] mode, driver heap is shared with all executor threads)
    spark_master: str = "local[6]"
    spark_driver_host: str | None = None
    spark_app_name: str = "procurement-silver-spark"
    spark_driver_memory: str = "4g"
    spark_executor_memory: str = "4g"
    spark_executor_cores: int = 6
    spark_shuffle_partitions: int = 24
    spark_local_dir: str = Field(default_factory=_default_spark_local_dir)
    spark_use_bundled_jars: bool = False
    java_home: str | None = None

    # Maven packages for Spark 4.0 + Iceberg + S3A
    iceberg_spark_package: str = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0"
    iceberg_aws_bundle_package: str = "org.apache.iceberg:iceberg-aws-bundle:1.10.0"
    hadoop_aws_package: str = "org.apache.hadoop:hadoop-aws:3.4.1"

    @classmethod
    def from_environment(
        cls,
        *,
        load_dotenv_file: bool = False,
        namespace: str | None = None,
        k3s: bool = False,
        spark_master: str | None = None,
        spark_driver_memory: str | None = None,
    ) -> SparkSilverSettings:
        if load_dotenv_file:
            from dotenv import find_dotenv, load_dotenv

            found = find_dotenv(usecwd=True)
            if found:
                load_dotenv(found)
            else:
                repo_env = Path(__file__).resolve().parents[2] / ".env"
                if repo_env.is_file():
                    load_dotenv(repo_env)

        db_url = os.getenv("APP_DATABASE_URL", "")
        if not db_url:
            raise ValueError("APP_DATABASE_URL must be set for Spark Silver control plane")

        resolved_master = spark_master or os.getenv("SPARK_MASTER", "local[6]")
        driver_host = os.getenv("SPARK_LOCAL_IP") or None
        default_driver_mem = os.getenv("SPARK_DRIVER_MEMORY", "4g")

        if k3s:
            if not spark_master:
                env_k3s = os.getenv("SPARK_K3S_MASTER")
                if env_k3s:
                    resolved_master = env_k3s
                else:
                    lan_ip = driver_host or detect_lan_ip()
                    resolved_master = f"spark://{lan_ip}:7077"
                    driver_host = lan_ip
            if not spark_driver_memory and "SPARK_DRIVER_MEMORY" not in os.environ:
                default_driver_mem = "3g"

        master_host = _extract_spark_master_host(resolved_master)
        if master_host and not driver_host:
            driver_host = detect_lan_ip(master_host)

        return cls(
            app_database_url=db_url,
            s3_endpoint=os.getenv("OBJECT_STORAGE_ENDPOINT", "http://127.0.0.1:8333"),
            s3_access_key=os.getenv("OBJECT_STORAGE_ACCESS_KEY", "procurement_admin"),
            s3_secret_key=os.getenv("OBJECT_STORAGE_SECRET_KEY", ""),
            s3_bucket=os.getenv("OBJECT_STORAGE_BUCKET", "procurement-lakehouse"),
            s3_region=os.getenv("OBJECT_STORAGE_REGION", "us-east-1"),
            iceberg_catalog_uri=os.getenv("ICEBERG_CATALOG_URI", "http://127.0.0.1:8181"),
            iceberg_warehouse=os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse"),
            iceberg_namespace=namespace or os.getenv("SPARK_SILVER_NAMESPACE", "silver_spark"),
            spark_master=resolved_master,
            spark_driver_host=driver_host,
            spark_driver_memory=spark_driver_memory or default_driver_mem,
            spark_executor_memory=os.getenv("SPARK_EXECUTOR_MEMORY", "4g"),
            spark_executor_cores=int(os.getenv("SPARK_EXECUTOR_CORES", "6")),
            spark_shuffle_partitions=int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "24")),
            spark_local_dir=_default_spark_local_dir(),
            spark_use_bundled_jars=os.getenv("SPARK_USE_BUNDLED_JARS", "").lower() in ("1", "true", "yes"),
            java_home=_detect_java_home(),
        )
