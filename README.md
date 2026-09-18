# Procurement Lakehouse

Thu thập dữ liệu Mua Sắm Công vào Bronze Parquet, quản lý kết quả theo manifest và theo dõi qua Ops.

| Cần làm gì? | Hướng dẫn |
| --- | --- |
| Cài môi trường, cấu hình `.env`, chạy Docker | [Cài đặt và cấu hình](docs/setup.md) |
| Crawl, backfill, repair, verify, đặt lịch | [Jobs ingestion](docs/ingestion.md) |
| Xem coverage, run, attempt và lỗi qua UI/API | [Ops](docs/ops.md) |
| Query Bronze bằng DuckDB, đọc dữ liệu đã commit | [Bronze Explorer và đọc dữ liệu](docs/bronze-explorer.md) |
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

Mở Ops tại `http://127.0.0.1:8000/ops`. Lệnh `daily` mặc định xử lý ngày hôm qua theo lịch Việt Nam; nên chạy lúc 08:00 hoặc muộn hơn. Xem [tham số ingestion](docs/ingestion.md#tham-số) trước khi mở rộng khoảng ngày.

Một ngày chỉ được coi là committed khi **DayManifest SUCCESS**. File Parquet của attempt FAILED có thể vẫn tồn tại; dùng Ops hoặc committed reader để chọn dữ liệu sử dụng.

## Backfill cả năm

```powershell
uv run --locked python -m procurement.jobs.ingest backfill --year 2025 --continue-on-error
uv run --locked python -m procurement.jobs.ingest repair --year 2025 --continue-on-error
uv run --locked python -m procurement.jobs.ingest status --year 2025
```

`--continue-on-error` cho phép đi tiếp sau ngày lỗi nguồn; cuối lượt vẫn báo lỗi để repair. Lỗi xác thực/storage hoặc trạng thái chưa xác nhận sẽ dừng flow. Mọi lệnh dùng cùng planner, bỏ qua ngày đã SUCCESS nếu không bật `--refresh`.
