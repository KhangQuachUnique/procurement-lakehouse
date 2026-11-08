# Vận hành

[README](../README.md) · [Orchestration](orchestration.md) · [CLI inventory](cli-inventory.md)

## Docker

Chạy tại gốc repo, cấu hình `.env` trước khi khởi động:

```bash
docker compose up -d --build
docker compose ps
docker compose logs --tail 100 dagster-code-location
```

Dagster: http://localhost:3002 · Ops: http://localhost:8000 · SeaweedFS: http://localhost:8888.
Cổng 8333 là S3 API, cần xác thực; không phải giao diện quản trị.

## Bronze: CLI và Dagster dùng chung PostgreSQL

CLI duy nhất để ingest là `procurement.cli.ingest`. Nó dùng cùng `IngestionService`
và `bronze_meta` với asset Bronze trong Dagster. Parquet vẫn nằm trên S3.
Ngày đã có `current_commit_id` được tái sử dụng; `--refresh` mới yêu cầu crawl lại.
Không dùng Dagster materialization hoặc sự tồn tại của file Parquet để suy ra commit.

Sau khi cập nhật source, build image trước khi dùng CLI trong Docker:

```bash
docker compose build dagster-code-location
```

Xem kế hoạch backfill 2026, không crawl và không ghi metadata:

```bash
docker compose run --rm --no-deps dagster-code-location \
  python -m procurement.cli.ingest --resource all --year 2026 --dry-run
```

Chạy cùng phạm vi:

```bash
docker compose run --rm --no-deps dagster-code-location \
  python -m procurement.cli.ingest --resource all --year 2026
```

`--year` của năm hiện tại chỉ lấy đến hôm qua theo lịch Việt Nam. Năm đã kết thúc
lấy trọn năm; năm tương lai bị từ chối. Có thể chọn `--date YYYY-MM-DD` hoặc
`--start-date YYYY-MM-DD --end-date YYYY-MM-DD` thay cho `--year`.
Khoảng ngày bao gồm hai đầu và phải trước hôm nay. Không cần `--max-days`.

`--resource` nhận `project`, `khlcnt`, `notify_contractor`, `contractor_result`,
`bid_opening` hoặc `all`; các resource/ngày chạy tuần tự. `all` bao gồm cả
`bid_opening`, trong khi `lakehouse_daily` hiện chỉ có bốn resource Bronze còn lại.
Kiểm tra `--dry-run` nếu chưa muốn bổ sung dữ liệu bid opening.

Mặc định dừng sau ngày thất bại. `--continue-on-error` cho phép tiếp tục sau kết quả
thất bại đã được service trả về; lỗi ngoại lệ kết nối/lease vẫn dừng lệnh.
Exit code 0: thành công; 1: lỗi; 2: sai cú pháp CLI. JSON ở stdout, tiến độ ở stderr.
`--dry-run` chỉ kiểm tra commit PostgreSQL, không xác minh file hay lease đang chạy.
Khi thực thi, service kiểm tra và giữ lease PostgreSQL cho từng partition.
`--refresh` thất bại không thay thế commit cũ.

Chạy trên host: `uv sync --locked --extra ops --extra metadata`, đặt `APP_DATABASE_URL`
trỏ đến PostgreSQL ứng dụng và storage endpoint của host trong `.env`, rồi dùng
`uv run --locked python -m procurement.cli.ingest ...`. CLI ingest nạp `.env`.
Không dùng URL container `app-postgres` từ host.

## Các công cụ khác

Silver: `python -m procurement.cli.silver --help`.
Profiling: `python -m procurement.tools.profile_bronze --help`.
Explorer: `python -m procurement.tools.bronze_explorer --help`.
Đếm record: `python -m procurement.tools.count_records --help`.
Các công cụ đọc Bronze hiện dùng PostgreSQL, không tự chuyển sang S3 manifest khi
PostgreSQL lỗi. Công cụ đếm có `--cached` để xem báo cáo đã lưu, không truy vấn lại.

`python -m procurement.cli.import_metadata --help` là công cụ chuyển đổi một lần:
đọc manifest lịch sử trên S3 và nhập vào PostgreSQL. Không phải lệnh backfill.
Cần đối soát dữ liệu lịch sử trước khi dùng.

Các CLI compaction, transfer, quality audit/repair, watcher và recovery 2024 cũ
đã bị gỡ vì chưa dùng PostgreSQL làm nguồn trạng thái. Chưa có CLI thay thế tương
đương cho tất cả chức năng đó. Không dùng bản image cũ để tiếp tục các workflow này.
Thư viện lịch sử vẫn được giữ cho dữ liệu cũ và các phụ thuộc nội bộ;
việc gỡ CLI không chuyển đổi toàn bộ workflow Dagster phụ trợ.
