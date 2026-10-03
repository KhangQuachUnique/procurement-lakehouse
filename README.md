# Procurement Lakehouse

Thu thập dữ liệu Mua Sắm Công vào Bronze Parquet, quản lý commit bằng manifest và theo dõi qua Ops. Silver/Gold hiện là thiết kế, chưa triển khai.

| Cần làm gì? | Hướng dẫn |
| --- | --- |
| Cấu hình, crawl/recovery, Ops, query, quality, profile, chuyển dữ liệu | [Vận hành](docs/operations.md) |
| Hiểu bảng/identity, validation, mô hình Silver và Gold | [Kiến trúc dữ liệu](docs/architecture.md) |
| Chạy tests, lint, build và thêm resource | [Công cụ phát triển](docs/development.md) |

## Bắt đầu

Cần Python 3.12–3.14, uv và Docker Compose. Chạy tại thư mục gốc repository:

```powershell
uv sync --locked --extra dev --extra ops
# Chỉ tạo .env khi chưa có; sau đó chỉnh cấu hình trong file
if (-not (Test-Path -LiteralPath .env)) { Copy-Item .env.example .env }
docker compose --env-file .env -f infra/docker/compose.yaml up -d object-storage
uv run --locked python -m procurement.jobs.ingest daily --dry-run
uv run --locked python -m procurement.jobs.ingest daily
uv run --locked uvicorn procurement.api.main:app --host 127.0.0.1 --port 8000
```

Sửa token/cấu hình storage trong `.env` trước khi crawl; bucket phải tồn tại và truy cập được. Ops tại `http://127.0.0.1:8000/ops`. Daily mặc định ngày hôm qua theo lịch Việt Nam; nên chạy lúc 08:00 hoặc muộn hơn. Xem [tham số ingestion](docs/operations.md#ingestion-và-recovery) trước khi mở rộng khoảng ngày.

Một ngày chỉ được coi là committed khi **DayManifest SUCCESS**. File Parquet của attempt FAILED có thể vẫn tồn tại; dùng Ops hoặc committed reader để chọn dữ liệu sử dụng.

## Các tác vụ thường dùng

Kiểm tra routing, chất lượng detail và sửa từng record: [Quality](docs/operations.md#kiểm-tra-và-sửa-chất-lượng-thông-báo).
Quy trình này tự audit, lập plan, repair và kiểm tra lại; chạy lại cùng lệnh để resume:

```powershell
.venv\Scripts\python.exe -m procurement.tools.repair_bronze_quality run --year 2025
.venv\Scripts\python.exe -m procurement.tools.profile_notify_fields --year 2025
```

Backfill hoặc retry các ngày ingestion chưa SUCCESS:

```powershell
uv run --locked python -m procurement.jobs.ingest backfill --year 2025 --continue-on-error
uv run --locked python -m procurement.jobs.ingest repair --year 2025 --continue-on-error
uv run --locked python -m procurement.jobs.ingest status --year 2025
uv run --locked python -m procurement.jobs.ingest verify --year 2025
```

`--continue-on-error` cho phép đi tiếp sau ngày lỗi nguồn; cuối lượt vẫn báo lỗi để repair. Lỗi xác thực/storage hoặc trạng thái chưa xác nhận sẽ dừng flow. Mọi lệnh dùng cùng planner, bỏ qua ngày đã SUCCESS nếu không bật `--refresh`.

Báo cáo theo lần chạy nằm trong `exports/`, cache/nghiên cứu trong `tmp/`.
`docs/` chỉ giữ ba tài liệu dùng lâu dài ở trên; mỗi công cụ có `--help`.
