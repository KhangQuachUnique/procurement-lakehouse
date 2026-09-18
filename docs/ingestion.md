# Jobs ingestion

[Mục lục](../README.md) · [Cấu hình](setup.md) · [Theo dõi bằng Ops](ops.md)

## Chọn lệnh

CLI ingestion duy nhất: `python -m procurement.jobs.ingest <mode> [tham số]`.

| Mode | Mục đích | Có gọi nguồn/ghi storage? |
| --- | --- | --- |
| `daily` | Xử lý ngày hôm qua hoặc các ngày trong lookback | Có, cho ngày được chọn |
| `backfill` | Bổ sung dữ liệu trong khoảng ngày chỉ định | Có, cho ngày được chọn |
| `repair` | Chạy lại ngày thiếu hoặc chưa có SUCCESS | Có, cùng planner với backfill |
| `status` | Xem coverage theo manifest | Không |
| `verify` | Kiểm coverage và đọc Parquet đã commit để kiểm count, lineage, hash | Không |

Mặc định bỏ qua ngày đã có SUCCESS. `--refresh` yêu cầu crawl cả ngày đã SUCCESS. Ngày mới vẫn có thể được crawl trong một lệnh refresh. Mỗi resource/ngày được crawl tạo run mới; chạy tuần tự theo resource rồi ngày. Mặc định dừng khi execution lỗi. Dùng `--continue-on-error` cho backfill dài để ghi nhận ngày lỗi rồi tiếp tục các ngày/resource còn lại.

| `--resource` | Dữ liệu / bảng Bronze |
| --- | --- |
| `all` | Cả 4 resource dưới đây, là mặc định |
| `project` | Dự án: `project_detail` |
| `khlcnt` | Kế hoạch/gói thầu: `khlcnt_plan_detail`, `khlcnt_bid_package_detail` |
| `notify_contractor` | Thông báo: `notify_contractor_standard_detail`, `notify_contractor_reoffer_detail` |
| `contractor_result` | Kết quả: `contractor_result_detail` |

## Tham số

| Tham số | Mặc định | Ý nghĩa |
| --- | --- | --- |
| `--resource` | `all` | Chọn giá trị trong bảng trên |
| `--year` | Không có | Chọn trọn một năm đã đóng, ví dụ 2025; dùng với backfill/repair/status/verify, không kết hợp cặp ngày |
| `--start-date` | Không có | Ngày đầu `YYYY-MM-DD`, bắt buộc khi không dùng daily hoặc --year |
| `--end-date` | Không có | Ngày cuối, tính cả ngày này; đi cùng --start-date |
| `--lookback-days` | `1` | Số ngày trước hôm nay cho daily, phải > 0; mode khác không dùng |
| `--page-size` | `50` | Số item search mỗi trang, phải > 0; không phải số record Bronze. Nếu API trả page size khác, attempt sẽ fail |
| `--max-days` | `31`; với `--year` cho phép trọn năm | Trần số ngày mỗi invocation, phải > 0; có thể đặt thấp hơn để giới hạn --year; không tự chia job |
| `--continue-on-error` | Tắt | Tiếp tục sau ngày FAILED có error nguồn được ghi đầy đủ; cuối lượt vẫn trả code 1. Chỉ dùng với daily/backfill/repair |
| `--dry-run` | Tắt | Chỉ đọc manifest/heartbeat và in kế hoạch; không crawl, không verify Parquet |
| `--refresh` | Tắt | Chọn lại cả ngày đã SUCCESS; không gây ghi dữ liệu trong status/verify/dry-run |
| `--retry-stale` | Tắt | Cho phép retry ngày bị run stale/interrupted/unknown chặn, sau khi xác nhận worker cũ đã dừng |
| `--stale-after-minutes` | `10` | Ngưỡng tuổi heartbeat để coi là stale, phải > 0; không tự chứng minh worker đã chết |
| `--lock-dir` | `INGESTION_LOCK_DIR` | Đổi thư mục khóa; các invocation cùng storage cần dùng chung thư mục |
| `-h`, `--help` | — | Xem trợ giúp, không chạy job |

Khoảng ngày phải có `start <= end`, mọi ngày đều trước hôm nay theo `Asia/Ho_Chi_Minh`. `--year` không nhận năm hiện tại hoặc tương lai. Daily không nhận `--year`/`--start-date`/`--end-date`. API window hiện là `00:00:00.000Z` đến `23:59:59.999Z` của source_date; nên chạy daily lúc 08:00 Việt Nam hoặc muộn hơn.

## Ví dụ sử dụng

```powershell
# Xem kế hoạch, rồi bổ sung ngày thiếu trong 3 ngày trước
python -m procurement.jobs.ingest daily --lookback-days 3 --dry-run
python -m procurement.jobs.ingest daily --lookback-days 3

# Cập nhật lại các ngày đã SUCCESS để lấy thay đổi muộn
python -m procurement.jobs.ingest daily --lookback-days 3 --refresh

# Backfill một tháng cho cả 4 loại
python -m procurement.jobs.ingest backfill --start-date 2022-01-01 --end-date 2022-01-31

# Backfill cả năm, tiếp tục qua ngày lỗi nguồn; không cần --max-days 365
python -m procurement.jobs.ingest backfill --year 2025 --continue-on-error

# Chạy lại phần còn thiếu của năm, hoặc chỉ chọn một loại
python -m procurement.jobs.ingest repair --year 2025 --continue-on-error
python -m procurement.jobs.ingest repair --year 2025 --resource khlcnt --continue-on-error

# Repair một loại, một ngày
python -m procurement.jobs.ingest repair --resource khlcnt --start-date 2022-10-26 --end-date 2022-10-26

# Rà coverage cả năm
python -m procurement.jobs.ingest status --year 2022

# Xem kế hoạch repair cả năm, chưa thực thi
python -m procurement.jobs.ingest repair --year 2022 --dry-run

# Đọc lại Parquet trong một tháng
python -m procurement.jobs.ingest verify --start-date 2022-01-01 --end-date 2022-01-31
```

Với một năm dữ liệu, nên thực thi repair/backfill từng tháng để giới hạn thời gian và chi phí kiểm tra. Không dùng `--refresh` nếu chỉ muốn lấp ngày thiếu. Verification đọc toàn bộ payload của các ngày được chọn; resource còn thiếu coverage chưa được verify đầy đủ trong invocation đó.

## Đọc kết quả và xử lý lỗi

JSON ở stdout, log ở stderr. Mỗi resource có `dates`; các trường chính là `status`, `effective_run_id`, `planned`, `blocked`, `active_runs`. Sau execution có `missing_dates`, `blocked_dates`, `verified` và `execution_errors` ở cấp report. `verified` chứa số ngày/file/record đã kiểm. `planned_days`/`attempted_days` đếm cặp resource/ngày; `stopped_early=true` nghĩa còn kế hoạch chưa được thực thi vì flow phải dừng. Mỗi execution error có `reason` để phân biệt lỗi nguồn, xác thực, storage/internal hoặc trạng thái chưa xác nhận.

| Exit code | Ý nghĩa |
| --- | --- |
| `0` | Flow hoàn tất kiểm tra; hoặc status đủ coverage; hoặc dry-run lập được kế hoạch |
| `1` | Thiếu coverage, bị chặn, execution lỗi hoặc verification thất bại |
| `2` | Tham số không hợp lệ |
| `130` | Bị ngắt; kiểm manifest trước khi retry |

`status` không kiểm nội dung Parquet. Dry-run code 0 không có nghĩa kế hoạch không bị chặn; đọc `blocked`/`active_runs`. Refresh thất bại vẫn trả code 1 dù bản SUCCESS trước đó còn sử dụng được.

`--continue-on-error` không biến ngày lỗi thành SUCCESS và không retry ngày đó ngay trong cùng lượt. Ví dụ ngày 10 lỗi 404: ghi FAILED, tiếp tục ngày 11; lần repair sau thử lại ngày 10. 404 vẫn không nằm trong HTTP retry tự động. Lỗi 401/403, lỗi load/internal, thiếu manifest/error hoặc exception không xác nhận được trạng thái sẽ dừng toàn flow dù đã bật tùy chọn này. Ngày bị run đang chạy chặn vẫn được bỏ qua và báo thiếu coverage; không dùng tùy chọn tiếp tục để vượt khóa.

| Tình huống | Cách xử lý |
| --- | --- |
| FAILED/no_attempt | Sửa nguyên nhân trong Ops rồi dùng repair |
| Nguồn trả 0 item hợp lệ | Attempt mới SUCCESS với 0 record, không cần Parquet; không sửa tay manifest FAILED cũ |
| RUNNING hoặc `blocked=true` | Kiểm worker/container và heartbeat trước khi retry |
| Run cũ không có heartbeat | Liveness là unknown; cần xác nhận worker đã dừng |
| 401/403 | Kiểm credential, chạy invocation mới sau khi sửa |
| 429/5xx/timeout | Client retry có giới hạn; nếu vẫn lỗi, kiểm nguồn/kết nối rồi repair |
| Count/hash/file mismatch | Kiểm storage; dùng repair `--refresh` đúng ngày nếu cần tạo bản thay thế |
| Search đạt 10.000 item/ngày | Engine fail để tránh thiếu dữ liệu; chưa hỗ trợ tự chia sub-day window |

Sau khi xác nhận worker cũ đã dừng:

```powershell
python -m procurement.jobs.ingest repair --resource khlcnt --start-date 2022-10-26 --end-date 2022-10-26 --retry-stale
```

Heartbeat còn mới vẫn chặn retry. Sidecar `_ops/.../execution.json` dùng cho liveness; `finished` chỉ nói execution kết thúc, không nói dữ liệu SUCCESS. Khóa OS tự nhả khi process chết, chỉ bảo vệ các lệnh ingest dùng cùng thư mục khóa và endpoint/bucket trên một host. Không dùng cơ chế này làm distributed lock.

## Manifest và dữ liệu đã commit

Day SUCCESS là commit marker. Effective attempt là attempt SUCCESS có `started_at` mới nhất. Khi lần refresh mới fail, bản SUCCESS cũ vẫn effective. Recovery tạo run mới, bắt đầu page 0, giữ lịch sử cũ. Run FAILED/PARTIAL_FAILED vẫn có thể chứa một số ngày SUCCESS.

| Đường dẫn trong bucket | Nội dung |
| --- | --- |
| `bronze/muasamcong/<table>/source_date=<date>/run_id=<id>/*.parquet` | Payload và lineage của attempt |
| `_control/<source>/<resource>/run_id=<id>/run.json` | Tổng kết run |
| `_control/<source>/<resource>/run_id=<id>/source_date=<date>/day.json` | Trạng thái/commit ngày |
| Cùng thư mục ngày, `pages/page-000000.json` | Trạng thái page |
| `_errors/<source>/<resource>/run_id=<id>/source_date=<date>/page-000000.jsonl` | Error records |
| `_ops/<source>/<resource>/run_id=<id>/execution.json` | Heartbeat sidecar |

Không suy ra coverage từ số file, không xóa failed/old attempts khi còn cần đối soát. Cách đọc đúng effective attempt ở [Bronze Explorer và committed reader](bronze-explorer.md).

## Đặt lịch

Lệnh scheduler: `python -m procurement.jobs.ingest daily --lookback-days 3 --refresh`, lúc 08:00 Việt Nam. Lookback là cửa sổ cập nhật, không bảo đảm bắt mọi thay đổi cũ hơn. Sau downtime dài hơn lookback, backfill khoảng bị bỏ lỡ. Theo dõi exit code và tránh retry vô hạn.

- **Windows Task Scheduler:** executable `.venv\Scripts\python.exe` bằng đường dẫn tuyệt đối; arguments `-m procurement.jobs.ingest daily --lookback-days 3 --refresh`; Start in là gốc repo; không start instance mới khi instance cũ đang chạy.
- **Linux systemd:** sửa user/đường dẫn trong [service](../infra/systemd/procurement-ingestion.service) và [timer](../infra/systemd/procurement-ingestion.timer), cài vào `/etc/systemd/system/`. Chạy `sudo systemctl daemon-reload`, rồi `sudo systemctl enable --now procurement-ingestion.timer`. `--now` bật lịch ngay; xem lịch bằng `systemctl list-timers procurement-ingestion.timer`, log bằng `journalctl -u procurement-ingestion.service`. Timer dùng 01:00 UTC = 08:00 Việt Nam; Persistent không tự backfill toàn bộ thời gian downtime.
- **Docker:** dùng lệnh một lần trong [hướng dẫn Compose](setup.md#docker-compose), để scheduler gọi theo lịch; container batch không tự restart.
