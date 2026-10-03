# Vận hành

[README](../README.md) · [Kiến trúc dữ liệu](architecture.md) · [Phát triển](development.md)

Các lệnh chạy tại gốc repo, sau `uv sync --locked --extra dev --extra ops`.
Activate `.venv` hoặc thay `python` bằng `uv run --locked python`.

## Cấu hình

Tạo `.env` từ [.env.example](../.env.example) nếu chưa có. Process environment
ưu tiên hơn `.env`; không commit token/credential. Giá trị và giới hạn đầy đủ ở
[Settings](../src/procurement/common/settings.py), cấu hình container ở
[Compose](../infra/docker/compose.yaml).

| Nhóm | Cấu hình cần biết |
| --- | --- |
| Nguồn | `MUASAMCONG_TOKEN` phải được API chấp nhận; `MUASAMCONG_BASE_URL`, timeout mặc định 30 giây |
| HTTP | `MUASAMCONG_MAX_ATTEMPTS=3`, `MUASAMCONG_MAX_RETRY_DELAY_SECONDS=30`, `MUASAMCONG_MAX_INFLIGHT=3`, `MUASAMCONG_REQUEST_INTERVAL_SECONDS=0` |
| Workers | `INGESTION_RESOURCE_WORKERS=2`, `KHLCNT_PACKAGE_WORKERS=3` |
| Storage | `OBJECT_STORAGE_ENDPOINT`, `OBJECT_STORAGE_BUCKET`, `OBJECT_STORAGE_ACCESS_KEY`, `OBJECT_STORAGE_SECRET_KEY`; bucket phải tồn tại |
| State/khóa | `DLT_PIPELINES_DIR`, `INGESTION_LOCK_DIR` (mặc định `data/locks`); mọi writer cùng host/storage dùng chung thư mục khóa |
| Ops | `OPS_INDEX_PATH=data/ops/index.sqlite3`; sync 5 giây, reconcile 300 giây, 8 workers, stale sau 600 giây |
| Quality | `NOTIFY_QUALITY_CONFIG` chọn TOML routing/contract; bỏ trống dùng config trong package |

Ingestion cần quyền đọc/ghi bucket; Ops và Explorer chỉ cần đọc. Index SQLite nằm
trên đĩa local, riêng cho mỗi endpoint/bucket. Trong Compose, endpoint storage là
`http://object-storage:8333`; endpoint trên host mặc định `http://localhost:8333`.

```powershell
docker compose --env-file .env -f infra/docker/compose.yaml up -d object-storage
docker compose --env-file .env -f infra/docker/compose.yaml --profile ops up -d --build ops
docker compose --env-file .env -f infra/docker/compose.yaml --profile ingestion run --rm --build ingestion python -m procurement.jobs.ingest daily
docker compose --env-file .env -f infra/docker/compose.yaml config --quiet
docker compose --env-file .env -f infra/docker/compose.yaml logs --tail 100 ops
docker compose --env-file .env -f infra/docker/compose.yaml down
```

`down` giữ volume storage/state/index; `down -v` xóa chúng. Storage khởi động trước
không bảo đảm bucket đã sẵn sàng. Volume vẫn cần backup riêng.

## Ingestion và recovery

CLI chung: `python -m procurement.jobs.ingest <mode> --help`.
`--resource` nhận `all` (mặc định), `project`, `khlcnt`, `notify_contractor`,
`contractor_result`; xem [bảng dữ liệu](architecture.md#bronze-hiện-tại).

| Mode | Hành vi |
| --- | --- |
| `daily` | Ngày hôm qua, hoặc cửa sổ `--lookback-days` |
| `backfill`, `repair` | Crawl ngày thiếu/chưa SUCCESS trong khoảng chọn |
| `status` | Xem coverage theo manifest, chưa kiểm file |
| `verify` | Kiểm coverage, count, lineage và hash payload của dữ liệu đã commit |

```powershell
python -m procurement.jobs.ingest daily --lookback-days 3 --dry-run
python -m procurement.jobs.ingest daily --lookback-days 3 --refresh
python -m procurement.jobs.ingest backfill --year 2025 --continue-on-error
python -m procurement.jobs.ingest repair --year 2025 --continue-on-error
python -m procurement.jobs.ingest status --year 2025
python -m procurement.jobs.ingest verify --year 2025
```

| Tham số | Ý nghĩa |
| --- | --- |
| `--year` | Năm đã kết thúc; không dùng với daily hoặc cặp ngày |
| `--start-date`, `--end-date` | Khoảng bao gồm hai đầu, `YYYY-MM-DD`, trước hôm nay theo lịch Việt Nam |
| `--lookback-days` | Daily, mặc định 1; không bảo đảm bắt thay đổi ngoài cửa sổ |
| `--refresh` | Crawl lại cả ngày đã SUCCESS; lần mới fail vẫn giữ SUCCESS cũ làm effective |
| `--dry-run` | Đọc kế hoạch/khóa; không crawl hoặc verify Parquet |
| `--max-days` | Mặc định 31; `--year` cho phép trọn năm nếu không đặt trần khác |
| `--page-size` | Mặc định 50 search items, không phải số records Bronze |
| `--continue-on-error` | Tiếp tục sau lỗi nguồn đã ghi đầy đủ; cuối lượt vẫn báo lỗi |
| `--retry-stale` | Retry run stale/interrupted/unknown sau khi xác nhận worker cũ đã dừng |
| `--stale-after-minutes`, `--lock-dir` | Ngưỡng stale mặc định 10 phút; thư mục khóa chung |
| `--resource-workers`, `--khlcnt-package-workers` | Ghi đè số workers trong `.env` |
| `--source-max-inflight`, `--source-request-interval` | Trần HTTP và khoảng cách bắt đầu request trên toàn flow, kể cả retry |

Mỗi resource giữ thứ tự ngày/trang; KHLCNT có thể tải các package của một plan
đồng thời. Trần HTTP chỉ áp dụng trong một flow. Khóa OS chỉ điều phối cùng host,
không phải distributed lock. 401/403, lỗi storage/internal hoặc commit chưa xác
nhận dừng flow ngay cả với `--continue-on-error`. Ctrl+C chờ worker ghi trạng thái;
kill cứng có thể để lại RUNNING.

| Tình huống | Xử lý |
| --- | --- |
| FAILED/no_attempt | Xem errors trong Ops, sửa nguyên nhân rồi repair |
| SUCCESS 0 record | Ngày rỗng hợp lệ, không cần Parquet |
| RUNNING/stale/unknown | Kiểm worker và heartbeat; sau khi xác nhận đã dừng mới `repair --retry-stale` |
| 401/403 | Sửa token/credential rồi chạy invocation mới |
| 429/5xx/timeout | Retry có giới hạn; nếu vẫn lỗi thì kiểm nguồn/kết nối và repair |
| Count/hash/file mismatch | Khôi phục file hoặc crawl lại đúng resource/ngày bằng `repair --refresh` |
| Search chạm 10.000 items/ngày | Engine fail để tránh thiếu dữ liệu; chưa tự chia cửa sổ nhỏ hơn ngày |

JSON kết quả ở stdout, log ở stderr. Đọc `blocked`, `active_runs`, `missing_dates`,
`execution_errors`, `stopped_early`; dry-run exit 0 không bảo đảm kế hoạch không bị
chặn. Ingest exit: 0 hoàn tất, 1 lỗi/thiếu coverage, 2 sai tham số, 130 bị ngắt.
`verify` chưa kiểm đầy đủ resource còn thiếu coverage; không suy chất lượng file
từ `status`. Không sửa tay manifest FAILED/RUNNING thành SUCCESS.

## Theo dõi và đặt lịch

```powershell
uv run --locked uvicorn procurement.api.main:app --host 127.0.0.1 --port 8000
```

Ops mặc định ở `http://127.0.0.1:8000/ops`, không có đăng nhập tích hợp.
Tra lỗi theo Calendar → ngày → attempt → errors/pages, hoặc Runs → run → attempt.

| Đường dẫn | Nội dung |
| --- | --- |
| `/ops`, `/ops/calendar`, `/ops/errors` | Runs, coverage, lỗi; lọc resource/ngày/run |
| `/docs` | Schema API, filters và giới hạn phân trang |
| `/api/ops/sync` | Độ mới index, lỗi sync, `last_success_at`; ready không có nghĩa snapshot còn mới |
| `/health/live`, `/health/ready` | Process hoạt động; bucket đọc được, không thay verify hoặc kiểm quyền ghi |

Ops phục vụ từ SQLite; storage manifests vẫn là nguồn chuẩn. Sync lỗi giữ snapshot
cũ; kiểm timestamp trước khi kết luận coverage. `stale` chỉ là heartbeat quá hạn,
không chứng minh worker đã chết. Calendar và planner có thể khác vì planner còn
xét range run RUNNING: dùng ingest `--dry-run` để kiểm `active_runs`.

Rebuild index bằng `python -m procurement.ops.sync --once --rebuild` sau khi dừng
process Ops đang giữ khóa sync. Nếu DB hỏng hoặc đổi bucket, dùng `OPS_INDEX_PATH`
mới. Restart bình thường giữ index; không cần crawler ghi SQLite.

Đặt lịch `python -m procurement.jobs.ingest daily --lookback-days 3 --refresh` lúc
08:00 Việt Nam hoặc muộn hơn: API search window là 00:00–23:59:59.999Z của ngày
nguồn. Windows Task Scheduler dùng đường dẫn tuyệt đối tới Python, Start in là
gốc repo, không chạy chồng invocation. Linux dùng [service](../infra/systemd/procurement-ingestion.service)
và [timer](../infra/systemd/procurement-ingestion.timer), sửa user/đường dẫn trước
khi cài. Downtime dài hơn lookback cần backfill riêng; timer không tự bù toàn bộ.

## Đọc và đếm Bronze

```powershell
python -m procurement.tools.bronze_explorer
python -m procurement.tools.bronze_explorer --resource notify_contractor --year 2024
python -m procurement.tools.count_records --year 2025 --resource notify_contractor
python -m procurement.tools.count_records --start-year 2022 --end-year 2025
python -m procurement.tools.count_records --start-year 2022 --end-year 2025 --cached
```

Explorer mở DuckDB UI tại `http://localhost:4213` (`--port` để đổi), dùng `.env`.
Lần đầu có thể cần tải extension; restart để discover table mới. `count_records`
đếm Parquet metadata của effective SUCCESS, chưa kiểm hash từng payload.
Count đọc footer bằng range request, mặc định 16 workers (`--workers 1..32`),
đối chiếu tổng mỗi resource/ngày với manifest. Cache số dòng từng file theo
ETag/size và storage; kết quả gồm project, khlcnt, packagebid, noti, result.
`--cached` chỉ xem snapshot đã lưu kèm thời điểm, không gọi storage; bỏ cờ này
để cập nhật sau crawl/repair. JSON report và SQLite cache nằm ở
`exports/bronze-counts/`. Thiếu ngày SUCCESS hoặc lệch count được báo INCOMPLETE,
exit code 2; không coi snapshot cache là dữ liệu realtime.

```sql
SELECT source_id, source_version, source_date, run_id, payload, filename
FROM bronze_raw.notify_contractor_standard_detail
WHERE source_date = DATE '2025-01-01'
LIMIT 20;
```

`bronze_raw` chỉ đọc effective SUCCESS tại lúc mở: chọn run theo resource/ngày
chung cho cả ba bảng detail, không trộn lịch sử. View `bronze_raw.notify_contractor`
gộp ba bảng và thêm `detail_table`. Restart sau repair để cập nhật snapshot.
Ngày SUCCESS nhưng mất toàn bộ file sẽ được báo ở console và
`bronze_meta.selection_issues`; explorer vẫn mở các ngày khác. Kết quả query khi đó
không đầy đủ, không tự thay bằng run lịch sử. Committed reader/audit vẫn kiểm tra nghiêm ngặt.
`--history` mở thêm `bronze_history` chứa tất cả attempts/năm, kể cả FAILED.
Mặc định không union schema toàn bộ file để mở nhanh hơn; dùng `--union-by-name`
nếu gặp Parquet cũ khác schema. `--resource` và `--year` giảm phạm vi mở.
Downstream cần kiểm hash/count vẫn dùng committed reader:

```python
from datetime import date
from procurement.common.catalog import get_resource
from procurement.storage.committed import iter_committed_records, select_committed_days
from procurement.storage.object_store import create_s3_filesystem

fs = create_s3_filesystem()
selection = select_committed_days(
    fs, get_resource("notify_contractor"), date(2025, 1, 1), date(2025, 12, 31)
)
for table, record in iter_committed_records(fs, selection, verify_hash=True):
    pass  # Ghi staging; chỉ công bố kết quả sau khi đọc hết thành công.
```

Reader kiểm count cuối ngày; dừng iterator sớm chưa chứng minh dữ liệu đầy đủ.

## Kiểm tra và sửa chất lượng thông báo

Khác với ingest repair lấp ngày thiếu, quality repair sửa record lỗi trong cả
ngày đã SUCCESS. Routing và validator nằm ở [contracts.py](../src/procurement/quality/contracts.py)
và [notify.toml](../src/procurement/quality/notify.toml); xem [ba nhánh thông báo](architecture.md#thông-báo-và-validation).

```powershell
python -m procurement.tools.repair_bronze_quality run --year 2025
```

Lệnh tự snapshot → audit → plan → repair → audit lại → so sánh. Chạy lại cùng
lệnh/work-dir để resume. Trạng thái và đường dẫn report nằm trong
`exports/notify-quality-job-2025/job.json`; `needs_attention` nghĩa còn lỗi/thiếu
ngày, không phải toàn năm đạt. Mặc định 3 detail workers, 3 attempts/request;
giảm tải bằng `--detail-workers 2 --request-interval 0.5`.

Config, snapshot, plan và baseline được cố định. Repair chỉ fetch targets, giữ
payload/hash/ingested_at của records tốt, viết attempt mới cho cả ngày và giữ
lịch sử cũ. Baseline/version nguồn đổi phải audit/plan lại; commit chưa xác nhận
phải readback, không tự ghi FAILED đè. Warning không tự trở thành repair target;
workflow chưa có route đã kiểm chứng vẫn unresolved.

Rule không giới hạn năm: `CGTTRG` → reoffer; các bidForm khác với
`KHAC/WB/ADB` → vk_adb, `LDT/CPTPP/EVFTA/UKFTA` → standard. Không dùng
stepCode/isInternet/bidMode để chia endpoint; thiếu bidForm hoặc processApply không
nhận diện được ở nhánh ngoài reoffer vẫn unresolved. Đây là quy luật suy luận từ
91 workflow; response thực tế vẫn phải qua validator. Khi đổi config, dùng `--work-dir` mới để tạo audit/plan
mới, không resume job cũ. Báo cáo so sánh liệt kê cả ngày bị chặn trong plan và
ngày không được chọn nhưng vẫn còn `fail`/`unresolved`.

Khi cần điều tra routing mới, chạy riêng các bước đọc nguồn:

```powershell
python -m procurement.tools.notify_quality_evidence snapshot --year 2025 --output exports/notify-search-2025
python -m procurement.tools.notify_quality_evidence probe --snapshot exports/notify-search-2025 --output exports/notify-evidence-2025.json --max-requests 800
python -m procurement.tools.notify_quality_evidence export-config --evidence exports/notify-evidence-2025.json --output exports/notify-reviewed.toml
```

Snapshot resume bằng `--resume`. Probe thử tối đa ba ID mỗi workflow trên ba API;
đạt trên mẫu không bảo đảm mọi record đều đạt. Lỗi HTTP không tự chứng minh route
sai; nhiều endpoint đạt là ambiguous. Review evidence rồi mới dùng config.
Các công cụ `audit_bronze_quality` và `repair_bronze_quality plan/apply/report`
có `--help` cho thao tác từng bước. Audit exit 2: còn fail/unresolved/thiếu ngày;
exit 1: công cụ chạy lỗi.

## Thống kê trường theo năm

```powershell
python -m procurement.tools.profile_notify_fields --year 2025
```

Đọc toàn bộ ba bảng thông báo từ effective SUCCESS; cần đủ mọi ngày trong năm.
Kiểm count/hash/lineage, ghi thư mục mới `exports/notify-field-profile-<year>-<timestamp>`;
`--output` chọn thư mục mới, `--workers` mặc định 4. Chỉ khi có `summary.json` mới
hoàn tất; nếu bị ngắt hoặc lỗi, chạy lại vào thư mục mới.

| File | Nội dung |
| --- | --- |
| `summary.md/json`, `selection.json` | Tổng hợp; danh sách chính xác ngày/run/file đã đọc |
| `business-fields.csv` | Trường trực tiếp trong root nghiệp vụ; không gộp alias hoặc ghép root |
| `fields.csv` | Mọi JSON path, kể cả object/mảng; chưa decode JSON bên trong string |
| `records.csv` | Số paths/occurrences/trường nghiệp vụ và trường có giá trị của từng record, kèm lineage |

`present_records` đếm key tồn tại, kể cả null; `populated_records` loại null,
chuỗi trắng, list/object rỗng, nhưng giữ 0/false. Mẫu số là mọi record trong bảng,
kể cả thiếu root. Path mảng `[]` đếm mỗi record một lần; `occurrences` đếm từng
phần tử. Một record có thể vừa có null vừa có giá trị tại cùng path mảng; các
count này không loại trừ nhau. Container khác rỗng chưa chứng minh children đủ.

Thay đường dẫn bằng report vừa tạo; tìm trường có giá trị ở ít nhất 99% records:

```sql
SELECT "table", field, total_records, present_pct, populated_pct, always_populated
FROM read_csv_auto('exports/notify-field-profile-2025/business-fields.csv')
WHERE populated_records >= 0.99 * total_records
ORDER BY "table", populated_records DESC, field;
```

`always_populated` so count chính xác, không dùng phần trăm làm tròn. Tỷ lệ 100%
là căn cứ review rule, không tự biến thành điều kiện bắt buộc cho mọi workflow.

## Chuyển dữ liệu sang máy khác

Hai máy dùng cùng phiên bản project, cấu hình bucket riêng. Chỉ chọn năm đã kết
thúc theo lịch Việt Nam; export/inspect/import không gọi nguồn MuaSamCong.

```powershell
python -m procurement.tools.bronze_transfer export --year 2025 --output exports/bronze-2025.zip --dry-run
python -m procurement.tools.bronze_transfer export --year 2025 --output exports/bronze-2025.zip
python -m procurement.tools.bronze_transfer inspect --archive imports/bronze-2025.zip
python -m procurement.tools.bronze_transfer import --archive imports/bronze-2025.zip --dry-run
python -m procurement.tools.bronze_transfer import --archive imports/bronze-2025.zip
```

Chuyển ZIP từ máy gửi vào `imports/` máy nhận; không chuyển `.part`. Export có
`--resource` để giới hạn; không ghi đè file có sẵn. Năm thiếu ngày vẫn có thể xuất
thành công: kiểm `coverage_complete`, `missing_dates`. Inspect offline kiểm
checksum/count/lineage/hash. Import luôn kiểm lại gói và commit từng ngày:

- Ngày đích đã SUCCESS: bỏ qua, dù ZIP mới hơn; chưa SUCCESS và không bị khóa: nhập.
- RUNNING chặn hoặc object cùng key khác nội dung: dừng, không tự vượt/ghi đè.
- Import gián đoạn: chạy lại cùng ZIP, tái sử dụng object đúng checksum và bỏ qua
  ngày đã xong; ACK commit không rõ cần kiểm lại storage.

ZIP64 giữ run ID, raw Parquet và manifests của effective days; không chứa `.env`,
Ops index, heartbeat, DLT state hay toàn bộ lịch sử attempts. Đây là transfer,
không phải backup toàn bucket. Cần đĩa tạm đủ cho ít nhất file Parquet lớn nhất.
Export/import dùng cùng host lock với ingestion; giữ nguồn ổn định khi xuất.
Gói thiếu coverage vẫn có thể exit 0; kiểm `verified`/`destination_verified`, rồi
ingest `status`/`verify`. Muốn bổ sung ngày thiếu tại đích, chạy ingest repair riêng.

## Nơi lưu tài liệu và kết quả

`docs/` chỉ giữ hướng dẫn và thiết kế đang dùng. Báo cáo theo năm/lần chạy, CSV,
snapshot, probe evidence và cache nằm trong `exports/` hoặc `tmp/` (gitignored),
không nhân bản vào docs. Số liệu vận hành lấy từ report mới nhất và manifest,
không dùng con số chép trong tài liệu cũ làm trạng thái hiện tại.
