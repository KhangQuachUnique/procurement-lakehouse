# Cấu Trúc Thư Mục Kết Quả Khảo Sát Dữ Liệu Bronze (Profiling Results)

Thư mục này lưu trữ các kết quả khảo sát, kiểm định chất lượng, đánh giá schema và phân tích trôi dạt (drift) của dữ liệu Bronze trong hệ thống Procurement Lakehouse.

```
exports/profiling/
├── README.md                      # Hướng dẫn & quy chuẩn tổ chức kết quả profiling
├── latest/                        # Kết quả khảo sát toàn diện mới nhất (Master Artifacts)
│   ├── profile.json               # Metrics thống kê đệ quy JSON của 100% trường Bronze
│   └── profiling-report.md        # Báo cáo đánh giá chất lượng 100% trường & bảng ánh xạ Silver chuẩn
└── history/                       # Lưu trữ các mốc thời gian profiling (Snapshot / Baseline)
    └── 20261018_bronze_baseline/  # Baseline khảo sát ngày 2026-10-18 (5.82 triệu bản ghi)
        ├── profile.json
        └── profiling-report.md
```

---

## 1. Cấu Trúc Thư Mục Chi Tiết

### `latest/`
Chứa 2 artifact tổng hợp duy nhất từ lần chạy profiling gần nhất của module `procurement.profiling`:
- **`profile.json`**: Tập tin máy đọc (machine-readable) chứa metrics phân tích đệ quy cho từng trường (field path) của toàn bộ các bảng Bronze:
  - Số lượng bản ghi, null count, null rate, empty string count, distinct count.
  - Phân bố kiểu dữ liệu (data types distribution).
  - Giá trị mẫu thường gặp (top values).
  - Độ dài chuỗi tối thiểu/tối đa (min_length, max_length).
  - Đánh giá khóa ứng viên (candidate key check).
- **`profiling-report.md`**: Báo cáo Markdown tổng hợp toàn diện (2 phần):
  1. Khảo sát đầy đủ 100% trường dữ liệu của từng bảng Bronze vật lý dưới dạng bảng kèm chẩn đoán vấn đề chất lượng cụ thể (Sentinel values, Pipe codes, Whitespace padding, Nested JSON strings...).
  2. Bảng ánh xạ trường dữ liệu chi tiết từ Bronze sang 15 bảng chuẩn hóa tại tầng Silver (Pure Relational 1-Hop), trong đó KHLCNT phân rã thành đúng 4 bảng (`plan_revision`, `package_revision`, `package_lot`, `package_location`).

*(Lưu ý: Resource thực nghiệm `bid_opening` được tách riêng và lưu trữ tại `temp/bid_opening/` trong giai đoạn hoàn thiện).*

### `history/`
Lưu trữ các snapshot có gắn nhãn thời gian hoặc mốc phiên bản (`YYYYMMDD_<label>`). Mục đích:
- Phục vụ kiểm tra Schema Drift giữa các đợt thu thập dữ liệu (ingestion windows).
- Cung cấp dữ liệu baseline đối soát khi nâng cấp hợp đồng dữ liệu.
