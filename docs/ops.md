# Ops: theo dõi ingestion

[Mục lục](../README.md) · [Jobs và recovery](ingestion.md) · [Cấu hình](setup.md)

Ops đọc manifest/error trong object storage để hiển thị coverage và hỗ trợ tìm lỗi. Ops không khởi chạy crawl hay sửa trạng thái manifest.

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

## Cách đọc trạng thái

| Trạng thái ngày | Ý nghĩa |
| --- | --- |
| `success` | Có ít nhất một Day SUCCESS; effective là SUCCESS có started_at mới nhất |
| `failed` | Chưa có SUCCESS, latest day attempt FAILED |
| `running` | Chưa có SUCCESS, latest day attempt RUNNING |
| `no_attempt` | Chưa có day attempt |

Run có các trạng thái `running`, `success`, `failed`, `partial_failed`. Run mới fail không làm mất ngày đã SUCCESS ở attempt trước. Ngày SUCCESS 0 record là kết quả rỗng hợp lệ, không phải thiếu crawl.

Calendar chiếu trạng thái day manifests; planner ingestion còn xét các range run RUNNING chưa kết thúc. Vì vậy một ngày trên Calendar là failed/no_attempt vẫn có thể bị planner chặn bởi run cũ. Dùng `ingest ... --dry-run` để xem `active_runs` và cách xử lý ở [recovery](ingestion.md#đọc-kết-quả-và-xử-lý-lỗi).

Luồng tìm lỗi: **Calendar → ngày → attempt → error/page**; hoặc **Runs → run → attempt** khi đã biết run_id. `no_attempt` không có error record. Coverage theo manifest chưa chứng minh nội dung file nguyên vẹn; dùng mode verify để kiểm thêm.

## Bộ lọc UI

Mọi trang nhận `source`, mặc định và hiện chỉ hỗ trợ `muasamcong`.

| Trang | Tham số |
| --- | --- |
| `/ops` | `resource`, `status`, `start_date`, `end_date`, `limit` mặc định 100, từ 1–500 |
| `/ops/calendar` | `resource` mặc định notify_contractor; `year` mặc định năm hiện tại, từ 2000–2100 |
| `/ops/errors` | `resource`, `source_date`, `run_id`, `stage`, `error_type`, `limit` mặc định 200, từ 1–1000 |

`resource` nhận project/khlcnt/notify_contractor/contractor_result. `status` của Runs nhận các trạng thái run ở trên; khoảng ngày lọc run giao với khoảng source_date, không phải ngày process bắt đầu. `stage`/`error_type` lấy giá trị từ error thực tế, ví dụ pagination/PaginationInvariantError. Ngày dùng `YYYY-MM-DD`.

## API chỉ đọc

Tất cả endpoint dưới đây là GET và nhận `source=muasamcong` mặc định.

| Endpoint | Query bổ sung |
| --- | --- |
| `/api/ops/overview` | Không |
| `/api/ops/resources` | Không |
| `/api/ops/resources/{resource}/dates` | `start_date`, `end_date` |
| `/api/ops/resources/{resource}/dates/{source_date}` | Không |
| `/api/ops/runs` | `resource`, `status`, `start_date`, `end_date`, `limit` mặc định 100 |
| `/api/ops/runs/{run_id}` | Không |
| `/api/ops/attempts/{run_id}/{source_date}` | Không |
| `/api/ops/errors` | `resource`, `source_date`, `run_id`, `stage`, `error_type`, `limit` mặc định 200 |

API limit từ 1–1000, giới hạn số dòng trả về sau lọc; không phải phân trang bằng offset. Date list mặc định 30 ngày đến hôm qua, tối đa 366 ngày mỗi request. Runs sắp theo started_at mới nhất; errors theo occurred_at mới nhất. Giá trị filter không hợp lệ có thể trả 400/422; không tìm thấy run/attempt trả 404.

```powershell
Invoke-RestMethod 'http://127.0.0.1:8000/api/ops/resources/khlcnt/dates?start_date=2022-01-01&end_date=2022-12-31'
Invoke-RestMethod 'http://127.0.0.1:8000/api/ops/runs?resource=khlcnt&status=failed&limit=20'
Invoke-RestMethod 'http://127.0.0.1:8000/api/ops/errors?resource=khlcnt&source_date=2022-10-26&limit=50'
```

Nếu Ops không thấy dữ liệu, kiểm endpoint/bucket/credential có trùng môi trường crawler không, sau đó kiểm filter ngày/source/resource. Khi lịch sử lớn, thu hẹp khoảng ngày; limit nhỏ của Runs/Errors không bảo đảm storage chỉ phải đọc ít object.
