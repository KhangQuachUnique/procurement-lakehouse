# THƯ VIỆN TÀI LIỆU KỸ THUẬT: PROCUREMENT LAKEHOUSE

- **Hệ thống:** Procurement Lakehouse (Hệ thống Hồ Dữ liệu & Phân tích Đấu thầu Quốc gia)
- **Mô hình:** Medallion Lakehouse 4 Tầng ($Bronze \rightarrow Silver \rightarrow Gold \rightarrow Audit$)
- **Hạ tầng chính:** Apache Iceberg Format v2 · SeaweedFS S3 · DuckDB Vectorized Engine · Dagster Orchestration

---

## 🌟 ĐỌC ĐẦU TIÊN (MASTER POST-MORTEM)

- 🚀 **[Hành trình Tiến hóa, Bài học Thất bại & 6 Cải tiến Cách mạng](evolution-and-lessons-learned.md)**:  
  *Bản đúc kết kỹ thuật thực chiến toàn diện*: Ghi lại chi tiết toàn bộ các "cơn ác mộng" kiến trúc ban đầu (khối hộp đen monolith, lệch phân vùng ngày-tháng, tràn bộ nhớ RAM, duplicate khi chạy lại) và phân tích sâu 6 giải pháp mang tính bước ngoặt đã đưa hệ thống về trạng thái chuẩn mực.

---

## 🏛️ ĐẶC TẢ KIẾN TRÚC & FLOW DỮ LIỆU TRỰC QUAN (`docs/architecture/`)

| Thứ tự | Tài liệu | Mô tả nội dung chính |
| :---: | :--- | :--- |
| **01** | **[Tổng quan Kiến trúc & Luồng Dữ liệu](architecture/01-system-overview-and-dataflow.md)** | Sơ đồ luồng dữ liệu trực quan bằng Mermaid (Bronze $\rightarrow$ Silver $\rightarrow$ Gold), topology 7 container Docker. |
| **02** | **[Đặc tả Kỹ thuật Các Tầng Medallion](architecture/02-medallion-layers-specification.md)** | Định nghĩa chi tiết hạt dữ liệu (Grain), cấu trúc 23 bảng Silver, 16 bảng Gold Star Schema (9 Dims + 7 Facts). |
| **03** | **[Phân hệ Thử nghiệm Contract Performance](architecture/03-pilot-contract-performance.md)** | Thiết kế cô lập cho phân hệ giải ngân, nghiệm thu và đánh giá chất lượng hợp đồng sau trúng thầu. |

---

## 🛠️ CẨM NANG THIẾT LẬP & VẬN HÀNH TOÀN DIỆN TỪ A $\rightarrow$ Z (`docs/operations-and-guides/`)

| Thứ tự | Cẩm nang hướng dẫn | Mô tả nội dung chính |
| :---: | :--- | :--- |
| **01** | **[Bước 1: Thiết lập Môi trường & Hạ tầng Docker](operations-and-guides/01-environment-setup-and-docker.md)** | Hướng dẫn cài đặt từ đầu: Docker Compose 7 containers, SeaweedFS S3, Iceberg Catalog, Python uv, chạy test. |
| **02** | **[Bước 2: Sổ tay Tra cứu Toàn bộ Lệnh CLI](operations-and-guides/02-cli-reference-and-workflows.md)** | Hướng dẫn toàn bộ lệnh CLI (`ingest`, `silver`, `gold`), ví dụ thực thi trọn vẹn một ngày dữ liệu cụ thể. |
| **03** | **[Bước 3: Sổ tay Vận hành Điều phối Dagster](operations-and-guides/03-dagster-orchestration-manual.md)** | Hướng dẫn sử dụng UI Dagster (`:3002`), mô hình Lineage 4 cột, chạy partition Ngày, kiểm tra metadata. |
| **04** | **[Bước 4: Chạy Backfill Lịch sử & Xử lý Sự cố](operations-and-guides/04-backfill-and-troubleshooting.md)** | Quy trình chạy bù dữ liệu 5 năm (2022–2025) và sổ tay xử lý nhanh các sự cố thường gặp trong 30 giây. |

---

## 📜 QUYẾT ĐỊNH KIẾN TRÚC CHÍNH THỨC (ADRs - `docs/decisions/`)

Toàn bộ các quyết định kỹ thuật cốt lõi được lưu giữ theo chuẩn Architectural Decision Records:

- **[ADR-0001: Khung dữ liệu Thử nghiệm Contract Performance](decisions/0001-contract-performance.md)**
- **[ADR-0002: Mô hình Dữ liệu Chiều Nghiệp vụ Tầng Gold](decisions/0002-gold-business-dimensional-model.md)**
- **[ADR-0003: Phân hệ Chuẩn hóa Slim Pilot cho Kết quả LCNT](decisions/0003-silver-result-slim-pilot.md)**
- **[ADR-0004: Kiến trúc Hexagonal & Hợp nhất Module Gold](decisions/0004-gold-hexagonal-architecture.md)**
- **[ADR-0005: Phân vùng Ngày Xuyên suốt & Cô lập Luồng Pilot](decisions/0005-dagster-daily-partition-lineage.md)**

---

## 📐 SƠ ĐỒ VẼ GỐC (`docs/schemas/`)

- **[Sơ đồ Star Schema Chi tiết (Draw.io)](schemas/gold-star-schemas-compact-approved-2026-11-06.drawio)**: File thiết kế kiến trúc gốc phục vụ kiểm tra tự động hợp đồng trong test suite (`test_contracts.py`).
