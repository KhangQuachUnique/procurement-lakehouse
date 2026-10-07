# Dagster cho Bronze

Dagster điều phối thực thi; **DayManifest SUCCESS** và verifier hiện có vẫn quyết
định Bronze committed. Module `procurement.orchestration.definitions` có năm
asset `bronze_<resource>`, cùng daily partitions và pool `muasamcong_ingestion`.
Silver/Gold chưa được triển khai; xem [đánh giá modernization](modernization_review.md).

## Chạy local

Tại gốc repo, cấu hình token và storage trong `.env`. Không chạy cùng writer
CLI/systemd khác. Local Dagster phải dùng chung `INGESTION_LOCK_DIR` với CLI.

```powershell
uv sync --locked --extra dev --extra ops --extra orchestration
New-Item -ItemType Directory -Force data/dagster | Out-Null
if (-not (Test-Path data/dagster/dagster.yaml)) {
    Copy-Item infra/dagster/local/dagster.yaml data/dagster/dagster.yaml
}
$env:DAGSTER_HOME = (Resolve-Path data/dagster).Path
uv run --locked --no-sync dagster dev -m procurement.orchestration.definitions
```

Mở `http://127.0.0.1:3000`, chọn `bronze_project` và một partition đã đóng.
Materialize sẽ tái dùng attempt SUCCESS hiện có và kiểm dữ liệu. Muốn crawl lại,
đặt config của op tương ứng:

```yaml
ops:
  bronze_project:
    config:
      refresh: true
      page_size: 50
```

`DAGSTER_BRONZE_START_DATE` mặc định `2000-01-01`, chỉ định biên lịch partition,
không khẳng định nguồn có dữ liệu từ ngày đó. Đặt theo lịch sử cần backfill trước
khi khởi động code location. Ngày hiện tại Việt Nam chưa phải partition đã đóng.

Materialization metadata có source/resource/date/run_id/status, pages, records,
errors, tables, thời gian và cờ reuse. Check `committed_integrity` dùng verifier
file/count/lineage/hash. `quality_contract` áp dụng cho notify và bid opening;
`fail` hoặc `unresolved` làm run thất bại nhưng không đổi manifest đã commit.
Các check chạy kèm asset; không phải một job audit độc lập.

## Backfill và retry

Khi materialize một ngày, Dagster đọc run headers với 8 workers và chỉ đọc day
manifest của ngày đó; không duyệt toàn bộ các ngày trong range run cũ. Run headers
được đọc trực tiếp từ storage, không lấy trạng thái từ cache Ops. Metadata có
`manifest_selection_seconds`, `integrity_seconds`, `quality_seconds` (nếu áp dụng)
và `materialization_seconds` để đo lần thực thi hiện tại. `duration_seconds`
vẫn là thời lượng attempt gốc trong manifest, kể cả khi reuse.

Chọn resource và khoảng partition trong UI, launch backfill. Mỗi run chứa một
ngày; mỗi asset tương ứng một resource. Pool limit 1 áp dụng xuyên các run;
executor giới hạn một op đồng thời trong một run. Config local/production đặt
giới hạn run 1 để khởi đầu thận trọng.

Ngày SUCCESS được tái dùng theo mặc định; ngày FAILED tạo attempt mới. Với
`refresh: true`, mỗi lần thực thi chủ động tạo attempt mới. Dagster không tự retry
run hoặc op. HTTP retry vẫn do ingestion client quản lý.

Nếu gặp commit uncertain, thiếu manifest hoặc active/stale run: kiểm worker cũ,
heartbeat, manifest và file của đúng run_id trước. Không sửa status lịch sử bằng
tay. Sau khi chắc chắn worker đã dừng, dùng quy trình reconciliation/recovery hiện
có trong [vận hành](operations.md#ingestion-và-recovery); rồi materialize lại.
Dagster không tự quyết định một active run đã hết hiệu lực chỉ dựa trên tuổi.
Nếu manifest lịch sử vẫn RUNNING sau khi đã đối soát và xác nhận worker dừng,
truyền `reconciled_run_ids: ["<run_id đã đối soát>"]` vào config của đúng asset.
Đây là xác nhận tường minh của người vận hành, lưu trong run config và metadata;
không tự suy ra từ heartbeat hoặc thời gian. ID khác không bỏ qua blocker.
Không dùng tùy chọn này khi trạng thái commit/worker vẫn chưa xác định.

## Compose với PostgreSQL

Thêm `DAGSTER_POSTGRES_PASSWORD` riêng vào `.env`, rồi:

```powershell
docker compose --env-file .env -f infra/docker/compose.yaml -f infra/docker/compose.dagster.yaml up -d --build
docker compose --env-file .env -f infra/docker/compose.yaml -f infra/docker/compose.dagster.yaml ps
```

Stack dùng PostgreSQL, daemon, webserver và gRPC code location. UI chỉ bind
localhost:3000. `DefaultRunLauncher` chạy process trong code-location container;
không cần mount Docker socket. Các process dùng chung image, Dagster home và
storage metadata. Volume ingestion/watch giữ state cũ và khóa cùng host.
Muốn bật Ops cũ, thêm `--profile ops` vào lệnh Compose; cơ chế sync Ops chưa đổi.

Pool mặc định 1 khi chưa có override. Nếu metadata DB đã có cấu hình pool khác,
kiểm tra lại trong UI hoặc đặt rõ:

```powershell
docker compose --env-file .env -f infra/docker/compose.yaml -f infra/docker/compose.dagster.yaml exec dagster-code-location dagster instance concurrency set muasamcong_ingestion 1
```

Dagster webserver/daemon không nhận token nguồn; code location nhận cùng các
giới hạn API như ingestion. Với custom quality TOML, mount file vào code location
và đặt `NOTIFY_QUALITY_CONFIG` tới đường dẫn trong container bằng overlay riêng.
Backup PostgreSQL, Dagster logs và object storage độc lập. Không dùng `down -v`
trên deployment có dữ liệu cần giữ.

## Cutover

Lịch `bronze_daily_schedule` mặc định **STOPPED**, lúc 08:00
`Asia/Ho_Chi_Minh`; mỗi tick tạo ba run cho ba ngày đã đóng và refresh cả năm
resource. Run keys bao gồm ngày tick và partition để tick lặp không tạo bản sao,
nhưng ngày hôm sau vẫn refresh được cửa sổ lookback.

1. Đối chiếu config endpoint/bucket/quality/locks với deployment cũ. Kiểm pool=1.
2. Dừng writer cũ trước khi materialize thử; xác minh commit, checks và Parquet.
3. Trên host đang có timer, chạy `systemctl disable --now procurement-ingestion.timer`
   và chờ service đang chạy kết thúc. Dừng các cron/watcher/CLI write khác.
4. Chỉ sau đó bật schedule trong Dagster UI. Kiểm run, manifest và coverage ngày
   tiếp theo trước khi bỏ hẳn unit cũ.

Các unit systemd trong repo đã được đánh dấu deprecated, không dùng để cài mới.
Lệnh cũ thiếu resource nên không phải đường rollback hợp lệ. Nếu rollback: tắt
schedule Dagster, drain các run đang chạy, rồi dùng CLI một resource mỗi lần.
Không bật hai scheduler cùng lúc. Đợt thay đổi source code này không tự bật/tắt
dịch vụ hoặc schedule production.

## Kiểm thử

```powershell
uv run --locked --no-sync ruff check .
uv run --locked --no-sync pytest
uv run --locked --no-sync python -c "from dagster import Definitions; from procurement.orchestration.definitions import defs; Definitions.validate_loadable(defs)"
uv run --locked --no-sync pytest -m integration tests/integration/test_dagster_bronze.py
```

Integration test dùng HTTP fixture, DLT thật, manifests thật; chạy local và S3 khi
`TEST_S3_ENDPOINT` trỏ tới SeaweedFS test cô lập, credentials `integration` /
`integration-test-only`. Fixture tạo bucket riêng và dọn bucket của nó. Không
trỏ biến này tới storage production.
