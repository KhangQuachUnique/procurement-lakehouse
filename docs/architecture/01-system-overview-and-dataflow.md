# Tổng Quan Kiến Trúc & Luồng Dữ Liệu (System Architecture & Data Flow)

- **Trạng thái:** CHÍNH THỨC (Single Source of Truth cho Kiến trúc Tổng thể)
- **Mô hình kiến trúc:** Medallion Lakehouse (Bronze $\rightarrow$ Silver $\rightarrow$ Gold) + Hexagonal Architecture (Ports & Adapters)
- **Định dạng lưu trữ:** Apache Iceberg Format v2 trên SeaweedFS S3 Object Storage
- **Engine tính toán:** DuckDB (In-memory OLAP & SIMD Vectorized Engine)

---

## 1. Sơ đồ Luồng Dữ liệu Trực quan (End-to-End Data Flow)

Hệ sinh thái vận hành theo luồng 4 tầng tuần tự, được bảo đảm tính nhất quán và toàn vẹn bằng phân vùng chuẩn Ngày (`YYYY-MM-DD`):

```mermaid
flowchart TD
    subgraph S1["1. Nguồn Dữ Liệu (Raw Ingestion)"]
        API["Hệ thống Mạng Đấu thầu Quốc gia<br/>(Muasamcong VNEPS API)"]
        INGEST["Ingestion Engine<br/>(Async HTTP Client, Backoff & Rate Limit)"]
        API --> INGEST
    end

    subgraph S2["2. Tầng Bronze (Raw Parquet Lake)"]
        BRONZE_S3[("SeaweedFS S3: procurement-lakehouse<br/>raw/{resource}/source_date=YYYY-MM-DD/")]
        PG_META[("PostgreSQL Metadata Service<br/>(Partitions, Attempts, File Lineage)")]
        INGEST -->|"Ghi file Parquet thô"| BRONZE_S3
        INGEST -->|"Ghi nhận Commit UUID"| PG_META
    end

    subgraph S3["3. Tầng Silver (Relational Lakehouse)"]
        SILVER_TRANS["Silver Transformer & Normalizer<br/>(Pydantic Validation, 1-Hop Relations)"]
        SILVER_ICEBERG[("Apache Iceberg Format v2<br/>Catalog: lake.silver<br/>(23 Bảng Chuẩn hóa Quan hệ)")]
        JOURNAL[("Silver Change Journal<br/>(Ghi nhận Delta & Audit Trail)")]
        
        BRONZE_S3 --> SILVER_TRANS
        PG_META --> SILVER_TRANS
        SILVER_TRANS -->|"Atomic Commit Parquet"| SILVER_ICEBERG
        SILVER_TRANS -->|"Snapshot delta"| JOURNAL
    end

    subgraph S4["4. Tầng Gold (Business Dimensional Star Schema)"]
        GOLD_COMPUTE["Gold Analytical Compute Engine<br/>(DuckDB In-Memory OLAP)"]
        CLUSTERING["Entity Resolution<br/>(Union-Find Graph Clustering)"]
        GOLD_DIMS[("9 Bảng Chiều (Dimensions)<br/>lake.gold.dim_*")]
        GOLD_FACTS[("7 Bảng Sự kiện (Facts)<br/>lake.gold.fact_*_daily")]
        VIEWS[("Zero-Copy Reporting Views<br/>gold.*")]
        
        SILVER_ICEBERG --> GOLD_COMPUTE
        JOURNAL --> CLUSTERING
        CLUSTERING --> GOLD_COMPUTE
        GOLD_COMPUTE -->|"Upsert Where Not Exists"| GOLD_DIMS
        GOLD_COMPUTE -->|"Atomic Partition Overwrite"| GOLD_FACTS
        GOLD_DIMS --> VIEWS
        GOLD_FACTS --> VIEWS
    end

    subgraph S5["5. Giám sát & Báo cáo (Audit & Consumption)"]
        RECONCILE["Automated 3-Part Financial Reconciliation<br/>(So khớp Kế hoạch, Trúng thầu, Khóa ngoại)"]
        DAGSTER["Dagster Webserver & Scheduler (:3002)<br/>(Lineage 4 Cột, Daily Partitions)"]
        OPS["Ops FastAPI Dashboard (:8000)<br/>(Giám sát tiến độ & trạng thái)"]
        
        VIEWS --> RECONCILE
        RECONCILE --> DAGSTER
        VIEWS --> OPS
    end
```

---

## 2. Sơ đồ Cấu trúc Hạ tầng & Dịch vụ (Infrastructure Topology)

Toàn bộ hệ thống được container hóa và điều phối thống nhất thông qua `compose.yaml`:

```mermaid
flowchart LR
    subgraph StorageInfra["Hạ tầng Lưu trữ & Metadata"]
        S3["SeaweedFS S3 (:8333)<br/>Data Lake Parquet Bucket"]
        CATALOG["Iceberg REST Catalog (:8181)<br/>lakehouse Catalog Manager"]
        APP_PG["App PostgreSQL 17 (:25432)<br/>Ingestion Lineage & Metadata"]
        DAG_PG["Dagster PostgreSQL 17<br/>Runs & Event Logs Storage"]
    end

    subgraph Orchestration["Bộ điều phối Dagster Lakehouse"]
        CODE_LOC["Dagster Code Location (:4000)<br/>(gRPC Server thực thi Assets)"]
        DAEMON["Dagster Daemon<br/>(Lịch trình & Sensor chạy nền)"]
        WEBSERVER["Dagster Webserver (:3002)<br/>(UI Dashboard theo dõi Lineage)"]
    end

    subgraph Application["Ứng dụng Giám sát"]
        OPS_APP["Ops FastAPI Service (:8000)<br/>(Tiến độ thầu & Health API)"]
    end

    StorageInfra <--> CODE_LOC
    CODE_LOC <--> DAEMON
    CODE_LOC <--> WEBSERVER
    DAG_PG <--> DAEMON
    DAG_PG <--> WEBSERVER
    StorageInfra <--> OPS_APP
```

---

## 3. Các Nguyên Tắc Kiến Trúc Bất Biến (Core Principles)

1. **Phân vùng Ngày đồng nhất (Daily Partition Homogeneity):**
   - Mọi thực thể từ Bronze, Silver đến Gold đều sử dụng chung một khóa phân vùng duy nhất: ngày báo cáo/sự kiện (`YYYY-MM-DD`).
   - Xóa bỏ hoàn toàn tình trạng chạy lệch pha giữa Silver (theo ngày) và Gold (theo tháng).

2. **Tính lũy thừa & Ghi đè an toàn (Idempotency & Partition Overwrite):**
   - Mọi Job có thể chạy lại vô hạn lần cho cùng một ngày mà không sinh ra dữ liệu trùng lặp.
   - Gold Facts sử dụng cơ chế `DELETE WHERE report_date >= start AND report_date < end` kết hợp `INSERT INTO` trong cùng transaction ACID của Iceberg.
   - Gold Dimensions áp dụng cơ chế `INSERT ... WHERE NOT EXISTS`.

3. **Kiến trúc Lục giác (Hexagonal Architecture):**
   - Tầng Domain (`gold.domain`, `silver.domain`) thuần túy chứa logic nghiệp vụ và schema, độc lập 100% với database engine, storage SDK và web framework.
   - Giao tiếp hạ tầng thông qua các cổng chuẩn (`ports`) và adapter chuyên biệt (`adapters`).

4. **Zero-Copy Reporting Views (`gold.*`):**
   - Người dùng và hệ thống BI (Metabase, Looker, Superset) truy vấn thông qua các SQL View logic trong schema `gold.*`.
   - Các view này trỏ trực tiếp tới release vật lý đang active (ví dụ: `lake.gold.*` hoặc bản thử nghiệm `lake.gold_v2.*`), cho phép chuyển đổi phiên bản (blue-green promote) tức thì mà không cần copy hay di chuyển dữ liệu.
