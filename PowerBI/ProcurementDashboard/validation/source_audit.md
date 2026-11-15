# Khảo sát nguồn

Chi tiết schema, kiểu, grain, PK/FK, currency, metric, allocation state, phiên bản địa lý và metadata nằm trong [source_profile.json](source_profile.json). Cardinality có hậu tố `approx_distinct` là ước lượng; kiểm tra PK/grain dùng COUNT DISTINCT chính xác.

| Bảng | Số dòng | Phạm vi ngày nguồn |
|---|---:|---|
| `dim_currency` | 4 | — |
| `dim_date` | 2,276 | 1202-10-18 → 2026-12-16 |
| `dim_duration_metric` | 7 | — |
| `dim_geography` | 65 | — |
| `dim_party` | 390,948 | — |
| `dim_procurement_method` | 389 | — |
| `dim_progress_status` | 4 | — |
| `dim_sector` | 7 | — |
| `dim_value_band` | 7 | — |
| `fact_award_daily` | 431,979 | 2022-09-16 → 2025-12-31 |
| `fact_bidding_competition_daily` | 241,978 | 2022-09-23 → 2025-12-31 |
| `fact_demand_daily` | 473,211 | 2022-09-16 → 2025-12-31 |
| `fact_duration_daily` | 156,434 | 2022-09-17 → 2025-12-31 |
| `fact_notice_daily` | 245,782 | 2022-09-17 → 2025-12-31 |
| `fact_savings_daily` | 574,693 | 2022-09-16 → 2025-12-31 |
| `fact_status_daily` | 19,773 | 2022-09-16 → 2025-12-31 |
| `fact_supplier_daily` | 1,472,830 | 2022-09-16 → 2025-12-31 |

Có đủ 17 bảng, tổng 4,010,387 dòng. Không phát hiện FK mồ côi hoặc FK ngày null trong các fact mới. PK và grain duy nhất, không null. Lịch model lọc về 1.461 ngày 2022–2025; nguồn lịch gốc giữ nguyên.

Parquet không có manifest/release ID/thời điểm xuất đáng tin cậy để chứng minh một snapshot chung. Thời gian sửa file không được dùng làm bằng chứng thời điểm xuất. Hash cuối khớp hash khảo sát: [source_integrity.json](source_integrity.json).
