# Cài đặt và cấu hình

[Mục lục](../README.md) · [Jobs ingestion](ingestion.md) · [Ops](ops.md)

## Môi trường Python

Chạy từ gốc repository với Python 3.12–3.14 và uv:

```powershell
uv sync --locked --extra dev --extra ops
```

`--locked` yêu cầu dependency khớp `uv.lock`; `--extra dev` cài pytest/Ruff, `--extra ops` cài FastAPI/Uvicorn. Bỏ extra không cần dùng. `uv run --locked <lệnh>` chạy trong môi trường của project.

Các trang hướng dẫn dùng `python` sau khi activate môi trường:

```powershell
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
```

```bash
# Linux / macOS
source .venv/bin/activate
```

Nếu không activate, thay `python` bằng `uv run --locked python`. `python -m <module>` chạy module của project.

## File .env

Tạo `.env` từ [.env.example](../.env.example) nếu chưa có: PowerShell dùng `Copy-Item .env.example .env`, Linux/macOS dùng `cp .env.example .env`. Giữ file này ngoài Git. Biến môi trường của process được ưu tiên hơn `.env`.

| Biến | Mặc định ứng dụng | Cách dùng |
| --- | --- | --- |
| `MUASAMCONG_TOKEN` | Chưa đặt | Bắt buộc khi crawl. API quyết định token được chấp nhận; ứng dụng không chặn riêng chuỗi `fake_token` |
| `MUASAMCONG_BASE_URL` | `https://muasamcong.mpi.gov.vn` | Base URL nguồn |
| `MUASAMCONG_TIMEOUT_SECONDS` | `30` | Timeout HTTP, phải > 0; không phải deadline cả run |
| `MUASAMCONG_MAX_ATTEMPTS` | `3` | Tổng số lần thử mỗi request, từ 1 đến 10 |
| `MUASAMCONG_MAX_RETRY_DELAY_SECONDS` | `30` | Trần chờ mỗi lần retry, từ 0 đến 300 giây; Retry-After lớn hơn trần thì không retry sớm |
| `MUASAMCONG_MAX_INFLIGHT` | `3` | Tổng request HTTP đồng thời trong một flow, từ 1 đến 32; dùng chung cho mọi resource và cả retry |
| `KHLCNT_PACKAGE_WORKERS` | `3` | Số gói thầu lấy đồng thời trong một plan KHLCNT, từ 1 đến 32; không vượt trần HTTP chung |
| `INGESTION_RESOURCE_WORKERS` | `2` | Số resource chạy đồng thời, từ 1 đến 4; mỗi resource vẫn chạy lần lượt các ngày |
| `OPS_INDEX_PATH` | `data/ops/index.sqlite3` | SQLite index riêng cho một endpoint/bucket; đặt trên ổ đĩa local, không đặt trên S3/NFS |
| `OPS_SYNC_INTERVAL_SECONDS` | `5` | Thời gian nghỉ giữa các lượt đồng bộ; không phải cam kết độ trễ tối đa |
| `OPS_RECONCILE_INTERVAL_SECONDS` | `300` | Chu kỳ đọc lại toàn bộ nội dung để đối soát, ngoài cập nhật theo metadata |
| `OPS_SYNC_WORKERS` | `8` | Số lượt đọc object đồng thời của worker đồng bộ, từ 1 đến 32 |
| `OPS_STALE_AFTER_SECONDS` | `600` | Heartbeat quá ngưỡng này được Ops hiển thị stale; không tự kết luận worker đã chết |
| `OBJECT_STORAGE_ENDPOINT` | `http://localhost:8333` | S3 endpoint |
| `OBJECT_STORAGE_BUCKET` | `procurement-lakehouse` | Bucket chứa Bronze/control/errors |
| `OBJECT_STORAGE_ACCESS_KEY` | Chưa đặt | Access key có quyền phù hợp với bucket |
| `OBJECT_STORAGE_SECRET_KEY` | Chưa đặt | Secret của access key |
| `DLT_PIPELINES_DIR` | DLT tự chọn | Thư mục state local; có thể đặt `./data/dlt-pipelines` |
| `INGESTION_LOCK_DIR` | `data/locks` | Thư mục khóa chung cho các invocation trên cùng host |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |
| `APP_ENV` | `dev` | Nhãn môi trường; không tự chọn bucket hay credential khác |

Các biến riêng của Compose: `OBJECT_STORAGE_IMAGE` chọn image SeaweedFS, `OBJECT_STORAGE_API_PORT` chọn cổng S3 trên host; mẫu lần lượt là `chrislusf/seaweedfs:4.46` và `8333`. `OBJECT_STORAGE_BIND_ADDRESS` mặc định `127.0.0.1`.

Bucket phải tồn tại và truy cập được trước khi chạy jobs. Ingestion cần đọc/ghi; Ops và Explorer chỉ cần đọc. Trong container Compose, S3 endpoint được đặt thành `http://object-storage:8333`.

## Docker Compose

```powershell
# Storage
docker compose --env-file .env -f infra/docker/compose.yaml up -d object-storage

# Ops tại http://127.0.0.1:8000/ops
docker compose --env-file .env -f infra/docker/compose.yaml --profile ops up -d --build ops

# Chạy ingestion một lần
docker compose --env-file .env -f infra/docker/compose.yaml --profile ingestion run --rm --build ingestion python -m procurement.jobs.ingest daily

# Kiểm tra cấu hình, trạng thái và log
docker compose --env-file .env -f infra/docker/compose.yaml config --quiet
docker compose --env-file .env -f infra/docker/compose.yaml ps
docker compose --env-file .env -f infra/docker/compose.yaml logs --tail 100 ops

# Dừng stack, giữ các volume dữ liệu
docker compose --env-file .env -f infra/docker/compose.yaml down
```

| Tham số | Ý nghĩa |
| --- | --- |
| `--env-file .env` | Cấu hình dùng để thay biến trong Compose |
| `-f ...` | Chọn file Compose |
| `--profile ops` / `--profile ingestion` | Bật nhóm service tương ứng |
| `-d` | Chạy service nền |
| `--build` | Build application image trước khi chạy |
| `--rm` | Xóa container của job sau khi kết thúc, giữ named volume |
| `--quiet` | Kiểm cấu hình mà không in cấu hình đã resolve |
| `--tail 100` | Chỉ lấy 100 dòng log cuối |

Storage dùng volume `object-storage-data`; ingestion dùng `ingestion-state` cho state và khóa; Ops dùng `ops-state` cho SQLite index. Compose cố định index tại `/var/lib/procurement/ops/index.sqlite3`. Không dùng `down -v` khi cần giữ dữ liệu. Volume là persistence, vẫn cần backup riêng. Compose khởi động storage trước nhưng không bảo đảm bucket đã sẵn sàng; kiểm `/health/ready` và `/api/ops/sync` của Ops.
