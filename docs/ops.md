# Ops: theo dõi ingestion

[Mục lục](../README.md) · [Jobs và recovery](ingestion.md) · [Cấu hình](setup.md)

Ops đọc SQLite index để hiển thị coverage và tìm lỗi. Worker nền đồng bộ manifest/error/heartbeat từ object storage; manifest vẫn là nguồn chuẩn. Ops không khởi chạy crawl hay sửa trạng thái manifest. Chỉ danh sách page của một attempt được đọc trực tiếp khi mở trang chi tiết.

## Khởi động

Cài extra ops và cấu hình storage trong `.env`, rồi chạy:

```powershell
uv run --locked uvicorn procurement.api.main:app --host 127.0.0.1 --port 8000
```

`--host` chọn địa chỉ lắng nghe, `--port` chọn cổng. Khi phát triển có thể thêm `--reload` để tự tải lại khi code đổi. Cách chạy container nằm ở [Compose](setup.md#docker-compose). UI/API không có lớp đăng nhập tích hợp; cấu hình mặc định chỉ mở localhost.

| Địa chỉ | Dùng để làm gì? |
| --- | --- |
| `http://127.0.0.1:8000/ops` | Xem/filter các run |
| `/ops/calendar?resource=khlcnt&year=2022` | Lịch coverage theo loại/năm |
| `/ops/calendar/khlcnt/2022-10-26` | Xem các attempt của một ngày |
| `/ops/runs/<run_id>` | Xem run và các day attempt |
| `/ops/attempts/<run_id>/<YYYY-MM-DD>` | Xem lỗi và pages của attempt |
| `/ops/errors` | Tra error records |
| `/docs` | Swagger: xem schema và gọi thử API |
| `/health/live` | Process trả lời được |
| `/health/ready` | Bucket tồn tại và credential truy cập được; 503 khi không sẵn sàng |

Health ready không thay thế việc verify Parquet hay kiểm quyền ghi của ingestion.

## SQLite index và đồng bộ

Ops tự khởi động worker đồng bộ cùng ứng dụng. Mặc định nghỉ 5 giây giữa các lượt, đọc tối đa 8 object đồng thời. Lượt đầu xây dựng index; UI/API dữ liệu trả 503 với `Retry-After: 5` cho đến khi có snapshot đầy đủ, UI tự thử lại. Không biến index chưa xây xong thành `no_attempt`.

Mỗi lượt liệt kê metadata, tải các run/day/error/heartbeat mới hoặc thay đổi, rồi cập nhật dữ liệu và mốc đồng bộ trong cùng transaction. Chạy lại không tạo bản trùng. Các manifest RUNNING được đọc lại mỗi lượt; mặc định 300 giây đối soát toàn bộ nội dung, kể cả khi metadata không đổi. Entry mất khỏi listing phải được xác nhận không còn tồn tại trước khi xóa khỏi index. Các lần đọc object không tạo thành transaction trên storage: Ops phản ánh snapshot đã quan sát và sẽ hội tụ ở lượt tiếp theo khi crawler còn ghi.

Nếu storage lỗi hoặc manifest không hợp lệ, lượt đó không công bố dữ liệu dở dang. UI giữ snapshot trước, hiển thị thời điểm cập nhật và thông báo sync bị chậm. `/api/ops/sync` trả `ready`, `last_success_at`, `snapshot_started_at`, `last_error_at`, `last_error`, `objects`; API dữ liệu có header `X-Ops-Last-Sync` và `X-Ops-Sync-State`. `ready=true` nghĩa đã có snapshot, không bảo đảm nó còn mới: luôn xét thời gian cập nhật/lỗi. Trang Runs có Previous/Next và API hỗ trợ `offset`.

Một khóa OS chỉ cho một worker nền đồng bộ cùng file SQLite; nhiều process API trên cùng host đọc index chung. Mỗi request đọc một transaction snapshot riêng. Index nằm trên ổ đĩa local/named volume, không dùng chung file qua nhiều host. SQLite có bản vá WAL-reset dùng WAL; phiên bản cũ đi kèm Python dùng rollback journal để tránh lỗi đó. Xem [SQLite WAL](https://www.sqlite.org/wal.html#walreset). Phiên bản cũ có thể phải chờ transaction đọc kết thúc trước khi commit sync; việc tải object luôn diễn ra trước transaction ghi.

Restart giữ index và tiếp tục đồng bộ. Dựng lại từ storage khi cần, sau khi dừng process Ops đang giữ khóa đồng bộ:

```powershell
python -m procurement.ops.sync --once --rebuild
```

Lệnh đọc lại toàn bộ và công bố kết quả nguyên tử, không sửa storage. Nếu DB hỏng hoặc đổi endpoint/bucket, chọn `OPS_INDEX_PATH` mới rồi khởi động Ops để dựng lại; giữ DB cũ để kiểm tra. Không tái sử dụng index của bucket khác. Không cần crawler ghi SQLite; storage ghi xong nhưng Ops crash thì lượt đồng bộ sau sẽ bổ sung.

## Cách đọc trạng thái

| Trạng thái ngày | Ý nghĩa |
| --- | --- |
| `success` | Có ít nhất một Day SUCCESS; effective là SUCCESS có started_at mới nhất |
| `failed` | Chưa có SUCCESS, latest day attempt FAILED |
| `running` | Chưa có SUCCESS, latest day attempt RUNNING và heartbeat còn mới |
| `stale` | Manifest RUNNING nhưng heartbeat quá hạn; chưa chứng minh worker đã chết |
| `unknown` | Manifest RUNNING nhưng thiếu/không xác định được heartbeat |
| `interrupted` | Manifest còn RUNNING nhưng execution sidecar đã kết thúc/bị ngắt |
| `no_attempt` | Chưa có day attempt |

Run có các trạng thái manifest `running`, `success`, `failed`, `partial_failed`. API run/attempt giữ `status` gốc và thêm `execution_state` cho manifest RUNNING; UI hiển thị trạng thái execution này. Dừng mềm ghi FAILED với error stage `interrupted`; kill cưỡng bức có thể để lại RUNNING, được chiếu thành stale/unknown theo heartbeat. Ops không tự đổi manifest đó thành FAILED. Run mới fail hoặc mất heartbeat không làm mất ngày đã SUCCESS ở attempt trước. Ngày SUCCESS 0 record là kết quả rỗng hợp lệ, không phải thiếu crawl.

Calendar chiếu trạng thái day manifests; planner ingestion còn xét các range run RUNNING chưa kết thúc. Vì vậy một ngày trên Calendar là failed/no_attempt vẫn có thể bị planner chặn bởi run cũ. Dùng `ingest ... --dry-run` để xem `active_runs` và cách xử lý ở [recovery](ingestion.md#đọc-kết-quả-và-xử-lý-lỗi).

Luồng tìm lỗi: **Calendar → ngày → attempt → error/page**; hoặc **Runs → run → attempt** khi đã biết run_id. `no_attempt` không có error record. Coverage theo manifest chưa chứng minh nội dung file nguyên vẹn; dùng mode verify để kiểm thêm.

## Bộ lọc UI

Mọi trang nhận `source`, mặc định và hiện chỉ hỗ trợ `muasamcong`.

| Trang | Tham số |
| --- | --- |
| `/ops` | `resource`, `status`, `start_date`, `end_date`, `limit` mặc định 100 (1–500), `offset` mặc định 0 |
| `/ops/calendar` | `resource` mặc định notify_contractor; `year` mặc định năm hiện tại, từ 2000–2100 |
| `/ops/errors` | `resource`, `source_date`, `run_id`, `stage`, `error_type`, `limit` mặc định 200, từ 1–1000 |

`resource` nhận project/khlcnt/notify_contractor/contractor_result. `status` của Runs nhận các trạng thái run ở trên; khoảng ngày lọc run giao với khoảng source_date, không phải ngày process bắt đầu. `stage`/`error_type` lấy giá trị từ error thực tế, ví dụ pagination/PaginationInvariantError. Ngày dùng `YYYY-MM-DD`.

## API chỉ đọc

Tất cả endpoint dưới đây là GET và nhận `source=muasamcong` mặc định.

| Endpoint | Query bổ sung |
| --- | --- |
| `/api/ops/overview` | Không |
| `/api/ops/sync` | Trạng thái index và đồng bộ; dùng được cả khi đang dựng index |
| `/api/ops/resources` | Không |
| `/api/ops/resources/{resource}/dates` | `start_date`, `end_date` |
| `/api/ops/resources/{resource}/dates/{source_date}` | Không |
| `/api/ops/runs` | `resource`, `status`, `start_date`, `end_date`, `limit` mặc định 100, `offset` mặc định 0 |
| `/api/ops/runs/{run_id}` | Không |
| `/api/ops/attempts/{run_id}/{source_date}` | Không |
| `/api/ops/errors` | `resource`, `source_date`, `run_id`, `stage`, `error_type`, `limit` mặc định 200 |

API limit từ 1–1000; Runs hỗ trợ offset từ 0–100000, áp dụng sau lọc. Date list mặc định 30 ngày đến hôm qua, tối đa 366 ngày mỗi request. Runs sắp theo started_at rồi run_id giảm dần; errors theo occurred_at mới nhất. Các trang phân trang là các snapshot riêng nên dữ liệu mới có thể dịch vị trí dòng giữa hai lần tải. Giá trị filter không hợp lệ có thể trả 400/422; không tìm thấy run/attempt trong snapshot trả 404.

```powershell
Invoke-RestMethod 'http://127.0.0.1:8000/api/ops/resources/khlcnt/dates?start_date=2022-01-01&end_date=2022-12-31'
Invoke-RestMethod 'http://127.0.0.1:8000/api/ops/runs?resource=khlcnt&status=failed&limit=20'
Invoke-RestMethod 'http://127.0.0.1:8000/api/ops/errors?resource=khlcnt&source_date=2022-10-26&limit=50'
```

Nếu Ops không thấy dữ liệu, kiểm `/api/ops/sync`, endpoint/bucket/credential có trùng crawler không, rồi kiểm filter ngày/source/resource. Runs/Calendar/Errors không quét storage trong request; đồng bộ ban đầu và đối soát toàn bộ vẫn tốn thời gian theo số object trong lịch sử.
