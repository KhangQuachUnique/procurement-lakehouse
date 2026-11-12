# Khảo sát nguồn

Chi tiết schema, kiểu, grain, PK/FK, currency, metric, allocation state, phiên bản địa lý và metadata nằm trong [source_profile.json](source_profile.json). Cardinality có hậu tố `approx_distinct` là ước lượng; kiểm tra PK/grain dùng COUNT DISTINCT chính xác.

| Bảng | Số dòng | Phạm vi ngày nguồn |
|---|---:|---|
| `dim_currency` | 4 | — |
| `dim_date` | 2,376 | 1202-10-18 → 2027-04-02 |
| `dim_duration_metric` | 7 | — |
| `dim_geography` | 165 | — |
| `dim_party` | 2,077,733 | — |
| `dim_procurement_method` | 420 | — |
| `dim_progress_status` | 4 | — |
| `dim_sector` | 7 | — |
| `dim_value_band` | 1 | — |
| `fact_award_daily` | 207,680 | 2022-09-16 → 2026-03-08 |
| `fact_bidding_competition_daily` | 120 | 2025-01-07 → 2025-01-24 |
| `fact_demand_daily` | 248,558 | 2022-09-16 → 2026-07-16 |
| `fact_duration_daily` | 65,421 | 2022-09-17 → 2026-03-31 |
| `fact_notice_daily` | 35,022 | 2022-09-17 → 2026-03-08 |
| `fact_savings_daily` | 207,680 | 2022-09-16 → 2026-03-08 |
| `fact_status_daily` | 20,901 | 2022-09-16 → 2026-07-16 |
| `fact_supplier_daily` | 1,488,397 | 2022-09-16 → 2026-03-08 |

Có đủ 17 bảng, tổng 4,354,496 dòng. Không phát hiện FK mồ côi; 8.015 FK ngày null thuộc duration được giữ nguyên. Dimension PK duy nhất, không null. Grain duration thiếu ngày có lỗi; xem [chính sách xử lý](source_issue_policy.json).

Parquet không có manifest/release ID/thời điểm xuất đáng tin cậy để chứng minh một snapshot chung. Thời gian sửa file không được dùng làm bằng chứng thời điểm xuất. Hash cuối khớp hash khảo sát: [source_integrity.json](source_integrity.json).
