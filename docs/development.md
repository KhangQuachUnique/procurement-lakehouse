# Công cụ phát triển

[README](../README.md) · [Vận hành](operations.md) · [Kiến trúc dữ liệu](architecture.md)

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

Integration dùng nguồn giả, không gọi MuaSamCong thật. Test transfer member thực >4 GiB bật riêng bằng `RUN_ZIP64_LARGE_TEST=1`, rồi chạy `pytest tests/integration/test_transfer.py -m integration -k actual_member`.

Chạy toàn bộ unit và integration khi endpoint test đã sẵn sàng:

```powershell
uv run --locked pytest -m 'integration or not integration'
```

## Build application image

```powershell
docker build -f infra/docker/Dockerfile -t procurement-lakehouse:local .
docker run --rm procurement-lakehouse:local python -m procurement.jobs.ingest --help
```

Image cài theo lockfile, chạy user không phải root và không chứa `.env`. Vận hành với [Compose](operations.md#cấu-hình).

[CI](../.github/workflows/ci.yml) khai báo lint/tests cho Linux/Windows, Python 3.12–3.14; integration S3, build package và image smoke. Chạy checks cục bộ liên quan trước khi đẩy thay đổi.

## Thêm resource

1. Tạo adapter trong `src/procurement/ingestion/sources/<source>/<resource>/`: search, detail extraction và factory trả ResourceSpec.
2. Đăng ký identity, table names và factory tại `common/catalog.py`. Ingest và Ops lấy danh sách từ catalog; không tạo CLI riêng cho từng resource.
3. Viết fixtures/tests cho ngày rỗng, pagination, detail lỗi, lineage và các bảng output. Dùng HTTPX MockTransport để tránh phụ thuộc nguồn thật trong unit tests.
4. Chạy integration để xác nhận Parquet và Day SUCCESS; cập nhật [bảng resource](architecture.md#bronze-hiện-tại).

`jobs/runner.py` hiện nối client MuaSamCong; nguồn khác cần bổ sung cách tạo client tại đây. `daily_runner` quản lý lifecycle, `page_runner` xử lý một page, `storage/bronze.py` ghi DLT. Thay adapter không được đổi commit marker, schema/path manifest hoặc gộp state của hai attempt. Kiểm [semantics Bronze](architecture.md#bronze-hiện-tại) khi thay đổi các phần này.

## Tài liệu

README là điểm vào; mỗi nội dung có một nơi chính trong vận hành, kiến trúc hoặc
phát triển. Khi hành vi đổi, sửa tại đó và kiểm liên kết/CLI `--help`. Không tạo
thêm báo cáo theo phiên/năm hoặc snapshot JSON trong `docs/`; kết quả công cụ đặt
ở `exports/`, thử nghiệm/cache ở `tmp/` (gitignored).


Resource tổng hợp `bid_opening` có fixtures một túi, hai túi và search đã loại token tại
`tests/ingestion/muasamcong/fixtures/`. Kiểm thay đổi writer bằng cả test buffer,
daily lifecycle và `pytest -m integration -k local`; một trang accepted chưa phải
persisted. Quality adapters giữ thao tác fetch riêng resource; contract assembly
không sửa payload. Khi đổi scheduler, kiểm seed transactional, namespace,
coverage gaps, idempotence, dữ liệu muộn và refresh FAILED/commit chưa rõ.
Kiểm thêm kỹ thuật hợp lệ khi chưa công bố tài chính, tài chính xuất hiện với
identity/version không đổi, null khi đã công bố phải fail và repair đủ hai phần.
