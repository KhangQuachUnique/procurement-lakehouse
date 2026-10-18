# Cấu Trúc Thư Mục Kết Quả Khảo Sát Dữ Liệu Bronze (Profiling Results)

Thư mục này lưu trữ các kết quả khảo sát, kiểm định chất lượng, đánh giá schema và phân tích trôi dạt (drift) của dữ liệu Bronze trong hệ thống Lakehouse.

```
exports/profiling/
├── README.md                                  # Hướng dẫn & quy chuẩn tổ chức kết quả profiling
├── latest/                                    # Kết quả khảo sát toàn diện mới nhất
│   ├── profile.json                           # Metrics thống kê đệ quy JSON của tất cả các bảng Bronze
│   ├── profiling-report.md                    # Báo cáo đánh giá chất lượng chi tiết bằng tiếng Việt
│   ├── tbmt_analysis_report.md                # Báo cáo phân tích sâu & bảng ánh xạ TBMT (Standard, ADB, Reoffer)
│   ├── tbmt_field_profile.json                # Metrics chi tiết cho TBMT
│   ├── project_analysis_report.md             # Báo cáo phân tích sâu & bảng ánh xạ Dự án (16/09/2022 golive)
│   └── project_field_profile.json             # Metrics chi tiết cho Dự án
├── history/                                   # Lưu trữ các mốc thời gian profiling (Snapshot / Baseline)
│   └── 20261018_bronze_baseline/             # Baseline khảo sát ngày 2026-10-18 (5.82 triệu bản ghi)
│       ├── profile.json
│       └── profiling-report.md
└── resources/                                 # Hồ sơ profiling chuyên sâu theo từng Resource
    ├── notify_contractor/                     # Thông báo mời thầu (TBMT)
    │   ├── tbmt_analysis_report.md            # Báo cáo phân tích 18 blocks JSON & mapping Silver
    │   └── tbmt_field_profile.json            # Metrics chi tiết dạng JSON
    ├── project/                               # Dự án đầu tư & Kế hoạch LCNT
    │   ├── project_analysis_report.md         # Báo cáo phân tích 4 blocks, mốc 16/09/2022, plocation JSON array
    │   └── project_field_profile.json         # Metrics chi tiết dạng JSON
    └── contractor/                            # Nhà thầu & bên mời thầu
```

---

## 1. Cấu Trúc Thư Mục Chi Tiết

### `latest/`
Chứa các artifact tổng hợp mới nhất từ lần chạy profiling gần nhất của CLI `procurement.profiling`.
- **`profile.json`**: Tập tin máy đọc (machine-readable) chứa metrics phân tích đệ quy cho từng trường (field path):
  - Số lượng bản ghi, null count, null rate, empty string count, distinct count.
  - Phân bố kiểu dữ liệu (data types distribution).
  - Giá trị mẫu thường gặp (top values).
  - Độ dài chuỗi tối thiểu/tối đa (min_length, max_length).
  - Đánh giá khóa ứng viên (candidate key check).
- **`profiling-report.md`**: Báo cáo tổng hợp bằng Markdown phục vụ rà soát nghiệp vụ, phân tích trôi dạt schema qua các năm, kiến nghị entity Silver và bộ quy tắc kiểm định chất lượng (Data Quality Rules).
- **`tbmt_field_profile.json`**: Đánh giá thực nghiệm chi tiết từng trường của gói thông báo mời thầu, tỉ lệ null/rỗng và ánh xạ từ Bronze raw sang Silver schema.

### `history/`
Lưu trữ các snapshot có gắn nhãn thời gian hoặc mốc phiên bản (`YYYYMMDD_<label>`). Mục đích:
- Phục vụ kiểm tra Schema Drift giữa các đợt thu thập dữ liệu (ingestion windows).
- Đóng vai trò là tập luật cơ sở (Baseline) để so sánh khi phát hành các bản vá transformer hoặc thay đổi định dạng dữ liệu từ nguồn e-GP.

### `resources/`
Phân rã theo từng thực thể nghiệp vụ lớn của hồ sơ mua sắm công:
- `notify_contractor/`: Hồ sơ mời thầu (gồm standard, vk_adb, reoffer).
- `project/`: Hồ sơ dự án và kế hoạch lựa chọn nhà thầu.
- `contractor/`: Hồ sơ năng lực nhà thầu và bên mời thầu.

---

## 2. Cách Chạy Khảo Sát & Xuất Kết Quả

Module profiling tuân thủ kiến trúc Hexagonal độc lập (`src/procurement/profiling/`).

### Khảo sát tất cả các bảng Bronze hoạt động (Active Tables)
```bash
python -m procurement.profiling.adapters.driver.cli --resource all --max-records 1000 --sample-step 10
```
Mặc định CLI sẽ xuất trực tiếp kết quả vào `exports/profiling/latest/profile.json` và `exports/profiling/latest/profiling-report.md`.

### Khảo sát một resource cụ thể (Ví dụ: `notify_contractor`)
```bash
python -m procurement.profiling.adapters.driver.cli --resource notify_contractor --max-records 2000
```

### Lưu vào thư mục lịch sử (History Baseline)
```bash
python -m procurement.profiling.adapters.driver.cli \
  --resource all \
  --output-json exports/profiling/history/20261018_bronze_baseline/profile.json \
  --output-report exports/profiling/history/20261018_bronze_baseline/profiling-report.md
```
