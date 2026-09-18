# Công cụ phát triển

[Mục lục](../README.md) · [Môi trường](setup.md)

## Dependency, lint và tests

```powershell
uv sync --locked --extra dev --extra ops
uv run --locked ruff check .
uv run --locked pytest
uv run --locked pytest tests/ingestion/engine/test_pagination.py
uv build
```

`ruff check .` kiểm code trong repo. Pytest mặc định bỏ marker integration; truyền đường dẫn để chạy một nhóm. `uv build` tạo wheel/sdist trong `dist/`. Khi chủ động đổi dependency, sửa pyproject rồi chạy `uv lock`, kiểm diff lockfile và chạy các checks liên quan.

## Integration với DLT/Parquet và S3

```powershell
# Filesystem local, không cần S3
uv run --locked pytest -m integration -k local

# Server S3 riêng cho test
docker run -d --rm --name procurement-integration -p 127.0.0.1:18333:8333 -e AWS_ACCESS_KEY_ID=integration -e AWS_SECRET_ACCESS_KEY=integration-test-only chrislusf/seaweedfs:4.46 mini -dir=/data
$env:TEST_S3_ENDPOINT = 'http://127.0.0.1:18333'
uv run --locked pytest -m integration
docker stop procurement-integration
```

Chờ S3 sẵn sàng trước khi chạy tests. `-m integration` chọn integration; `-k local` lọc case có tên local. Trên Linux/macOS dùng `export TEST_S3_ENDPOINT=http://127.0.0.1:18333`. Không đặt endpoint test trỏ vào storage vận hành: fixtures dùng credential test, tạo bucket UUID và xóa bucket đó sau test.

Trong `docker run`, `-d` chạy nền, `--rm` dọn container sau khi dừng, `--name` đặt tên, `-p` ánh xạ cổng localhost, `-e` đặt biến môi trường; `mini -dir=/data` là lệnh SeaweedFS. Integration dùng nguồn giả, không gọi MuaSamCong thật.

Chạy toàn bộ unit và integration khi endpoint test đã sẵn sàng:

```powershell
uv run --locked pytest -m 'integration or not integration'
```

## Build application image

```powershell
docker build -f infra/docker/Dockerfile -t procurement-lakehouse:local .
docker run --rm procurement-lakehouse:local python -m procurement.jobs.ingest --help
```

`-f` chọn Dockerfile, `-t` đặt image tag, `.` là build context. Image cài theo lockfile và chạy user không phải root; không chứa `.env`. Vận hành image với [Compose](setup.md#docker-compose).

[CI](../.github/workflows/ci.yml) khai báo lint/tests cho Linux/Windows, Python 3.12–3.14; integration S3, build package và image smoke. Chạy checks cục bộ liên quan trước khi đẩy thay đổi.

## Thêm resource

1. Tạo adapter trong `src/procurement/ingestion/sources/<source>/<resource>/`: search, detail extraction và factory trả ResourceSpec.
2. Đăng ký identity, table names và factory tại `common/catalog.py`. Ingest và Ops lấy danh sách từ catalog; không tạo CLI riêng cho từng resource.
3. Viết fixtures/tests cho ngày rỗng, pagination, detail lỗi, lineage và các bảng output. Dùng HTTPX MockTransport để tránh phụ thuộc nguồn thật trong unit tests.
4. Chạy integration để xác nhận Parquet và Day SUCCESS; cập nhật bảng resource trong [jobs ingestion](ingestion.md).

`jobs/runner.py` hiện nối client MuaSamCong; nguồn khác cần bổ sung cách tạo client tại đây. `daily_runner` quản lý lifecycle, `page_runner` xử lý một page, `storage/bronze.py` ghi DLT. Thay adapter không được đổi commit marker, schema/path manifest hoặc gộp state của hai attempt. Kiểm chính sách đọc/repair trong [ingestion](ingestion.md#manifest-và-dữ-liệu-đã-commit) khi thay đổi các phần này.
