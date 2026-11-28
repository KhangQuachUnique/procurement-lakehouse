# Tổng Quan Kiến Trúc & Luồng Dữ Liệu (System Architecture & Data Flow)

- **Mô hình kiến trúc:** Medallion Lakehouse 3 Tầng ($Bronze \rightarrow Silver \rightarrow Two\text{-}Tier\ Gold$)
- **Định dạng lưu trữ:** Apache Iceberg 1.10 (Format v2, Write-Audit-Publish) trên SeaweedFS S3
- **Engine tính toán:** Apache Spark 4.0.0 (chạy cục bộ `local[N]` hoặc phân tán trên K3s Multi-Node)
- **Điều phối:** Dagster (Docker Compose với JVM 21 + PySpark 4.0.0 tích hợp sẵn)

---

## 1. Sơ Đồ Luồng Dữ Liệu End-to-End

```mermaid
flowchart TD
    subgraph S1["1. Nguồn Dữ Liệu (Ingestion)"]
        API["Cổng Mua Sắm Công Quốc gia<br/>(muasamcong.mof.gov.vn)"]
        INGEST["Ingestion Engine + Bid-Opening Watcher<br/>(httpx, Backoff, Rate Limit, dlt)"]
        API --> INGEST
    end

    subgraph S2["2. Tầng Bronze (Raw Parquet Lake)"]
        BRONZE_S3[("SeaweedFS S3 (:8333)<br/>procurement-lakehouse/raw/{resource}/source_date=YYYY-MM-DD/")]
        PG_META[("PostgreSQL 17 (:25432)<br/>Bronze Commits · Silver Receipts · Gold Watermarks")]
        INGEST -->|"Parquet thô"| BRONZE_S3
        INGEST -->|"Commit UUID & SHA-256"| PG_META
    end

    subgraph S3["3. Tầng Silver (Spark 4.0 + Iceberg WAP)"]
        SILVER_SPARK["Spark Silver Runner<br/>(src/procurement/silver/)"]
        SILVER_ICEBERG[("Apache Iceberg (:8181)<br/>lake.silver_spark<br/>21 Bảng Quan hệ Chuẩn hóa")]
        BRONZE_S3 --> SILVER_SPARK
        PG_META --> SILVER_SPARK
        SILVER_SPARK -->|"WAP Branch -> DQ Gate -> Fast-Forward"| SILVER_ICEBERG
    end

    subgraph S4["4. Tầng Gold Two-Tier (Spark 4.0 + Iceberg WAP)"]
        GOLD_CORE[("Tier 1: lake.gold_spark_core<br/>12 Bảng Atomic Core Entities<br/>(Incremental MERGE INTO)")]
        GOLD_MART[("Tier 2: lake.gold_spark<br/>9 dim_* + 8 fact_*_daily<br/>(Partitioned by months(report_date))")]
        DQ_GATE["Gold Quality & Reconciliation Gate<br/>(0 NULL/dup PK · award == supplier VND)"]
        SILVER_ICEBERG -->|"start-snapshot-id delta / full"| GOLD_CORE
        GOLD_CORE --> GOLD_MART
        GOLD_MART --> DQ_GATE
    end

    subgraph S5["5. Điều Phối & Tiêu Thụ BI"]
        DAGSTER["Dagster Webserver & Daemon (:3002)<br/>bronze_daily · spark_lakehouse_daily · maintenance"]
        PBI["Power BI Desktop (6 trang)<br/>(17 Parquet Marts)"]
        DQ_GATE --> PBI
        DAGSTER -.-> INGEST
        DAGSTER -.-> SILVER_SPARK
        DAGSTER -.-> GOLD_CORE
    end
```

---

## 2. Cấu Trúc Hạ Tầng (Infrastructure Topology)

```mermaid
flowchart LR
    subgraph DockerHost["Docker Compose Stack (Máy Chủ)"]
        S3["SeaweedFS S3 (:8333)<br/>+ Iceberg REST (:8181)"]
        PG["PostgreSQL 17 (:25432)<br/>procurement + dagster DBs"]
        DAG_LOC["Dagster Code Location (:4000)<br/>Java 21 + PySpark 4.0.0"]
        DAG_DAEMON["Dagster Daemon"]
        DAG_WEB["Dagster Webserver (:3002)"]
    end

    subgraph K3sCluster["Cụm K3s Spark Multi-Node (LAN hoặc Tailscale)"]
        SM["spark-master (:7077 / UI :8080)"]
        SW1["spark-worker (Master Node)"]
        SW2["spark-worker (Worker Linux / Windows WSL2)"]
    end

    S3 <--> DAG_LOC
    PG <--> DAG_LOC
    PG <--> DAG_DAEMON
    PG <--> DAG_WEB
    DAG_LOC <--> DAG_DAEMON
    DAG_LOC <--> DAG_WEB
    DAG_LOC -->|"Spark Driver"| SM
    SM <--> SW1
    SM <--> SW2
    SW1 <-->|"Read/Write Parquet"| S3
    SW2 <-->|"Read/Write Parquet"| S3
```

---

## 3. Cấu Trúc Mã Nguồn (`src/procurement/`)

```text
src/procurement/
├── ingestion/        # Cào API MuaSamCong (6 resources)
├── watcher/          # Theo dõi lịch mở thầu (bid_opening)
├── bronze/           # Ghi & xác thực Bronze Parquet
├── quality/          # Hợp đồng kiểm tra chất lượng đầu vào Bronze
├── spark/            # Hạ tầng Spark 4.0 + Iceberg 1.10 dùng chung (session, WAP, maintenance)
├── silver/           # Tầng Silver Spark (21 bảng Iceberg + receipts + DQ gate)
├── gold/             # Tầng Gold Spark Two-Tier (12 core + 17 marts + watermarks + DQ gate)
├── orchestration/    # Dagster jobs, schedules & sensors
├── metadata/         # PostgreSQL partition metadata service
├── storage/          # S3 / object storage helpers
├── common/           # Settings, catalog, dates, errors, logging
└── cli/              # Entrypoint `ingest` + Rich terminal UI
```

---

## 4. Nguyên Tắc Kiến Trúc Cốt Lõi

1. **Write-Audit-Publish (WAP) tuyệt đối:** Cả Silver và Gold đều ghi vào Iceberg branch tạm, kiểm tra chất lượng xong mới `fast_forward` vào `main`. Nếu lỗi, branch tạm bị `abort` nên bảng production không bao giờ bị nửa chừng hay hỏng dữ liệu.
2. **Two-Tier Gold & Incremental Snapshot Tracking:** Tầng Core lưu 12 thực thể nguyên tử có khóa merge (`CORE_MERGE_KEYS`), cho phép `gold run` hàng ngày chỉ đọc delta từ `start-snapshot-id` của Silver và `MERGE INTO` thay vì tính lại toàn bộ 5 năm.
3. **Pure-JVM Execution trên K3s:** Toàn bộ transform Silver/Gold chạy thuần Spark SQL / JVM (không dùng Python UDF/RDD trên worker), nên miễn nhiễm khác biệt phiên bản Python giữa host và K3s pod.
