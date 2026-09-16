# Ops

Ops là lớp read-only để quan sát ingestion. `_control` và `_errors` trong object storage vẫn là source of truth; Ops không sửa manifest hay error cũ.

## Mental model

```text
Run
  -> Source date / Attempt
      -> Errors
      -> Pages
```

Một ngày có thể có nhiều attempt:

```text
2026-09-12
├── run A FAILED
├── run B FAILED
└── run C SUCCESS  <- effective attempt
```

Nếu có ít nhất một attempt `SUCCESS`, attempt thành công mới nhất là `effective_run_id`. Attempt fail cũ vẫn được giữ để audit.

## Trạng thái ngày

Calendar dùng bốn trạng thái:

```text
success     có ít nhất một attempt SUCCESS
failed      có attempt nhưng chưa từng SUCCESS và attempt mới nhất FAILED
running     có attempt nhưng chưa từng SUCCESS và attempt mới nhất RUNNING
no_attempt  không có attempt nào
```

Hai rule quan trọng:

```text
no_attempt != failed
SUCCESS với bronze_records = 0 vẫn là success
```

Không có dữ liệu từ upstream không tự động có nghĩa ingestion lỗi. Chỉ lỗi thực sự trong attempt mới được biểu diễn là `failed` và ghi vào `_errors`.

## Chạy Ops

```powershell
uvicorn procurement.api.main:app --reload
```

Mở:

```text
http://127.0.0.1:8000/
```

`/` redirect sang `/ops`.

## UI

### Runs

```text
GET /ops
```

Đây là màn hình mặc định. Có filter theo resource, run status, date range và limit.

```text
Runs
  -> Run detail
      -> Date attempt detail
          -> Errors
          -> Pages
```

Run detail hiển thị status, date range, success/failed dates, tổng bronze records, tổng error count, duration và danh sách day attempts.

### Calendar

```text
GET /ops/calendar
```

Calendar hiển thị một resource theo tháng:

```text
success     xanh
failed      đỏ
running     vàng
no_attempt  trung tính
```

Click một ngày để xem tất cả attempt của ngày đó và `effective_run_id`.

Các URL cũ:

```text
/ops/resources/{resource}
/ops/resources/{resource}/dates/{source_date}
```

được redirect sang Calendar để không làm gãy bookmark cũ.

### Attempt detail

```text
GET /ops/attempts/{run_id}/{source_date}
```

Errors được đặt trước Pages vì đây là màn hình debug vận hành.

### Errors

```text
GET /ops/errors
```

Chỉ hiển thị error records thực sự. `no_attempt` không xuất hiện trong error list.

## API

### Recent runs

```text
GET /api/ops/runs
```

Filters:

```text
resource
status
start_date
end_date
limit
```

### Run detail

```text
GET /api/ops/runs/{run_id}
```

Trả run summary và day attempts của run.

### Calendar data

```text
GET /api/ops/resources/{resource}/dates
```

Query:

```text
start_date=YYYY-MM-DD
end_date=YYYY-MM-DD
```

Window tối đa 366 ngày.

### Date detail

```text
GET /api/ops/resources/{resource}/dates/{source_date}
```

### Attempt detail

```text
GET /api/ops/attempts/{run_id}/{source_date}
```

### Errors

```text
GET /api/ops/errors
```

Filters:

```text
resource
source_date
run_id
stage
error_type
limit
```

## Performance

Calendar không còn glob toàn bộ:

```text
run_id=*/source_date=*/day.json
```

cho mỗi request. Flow hiện tại là:

```text
1. đọc run manifests giao với date window
2. chỉ đọc day manifests bên trong các run đó
3. project trạng thái ngày trong memory
```

Cách này giảm đáng kể số object phải đọc khi lịch sử attempts tăng.

`run detail` vốn đã đọc theo `run_id`, nên không scan attempts của run khác.

Nếu số lượng run manifests sau này đủ lớn để `run_id=*/run.json` trở thành bottleneck, bước tiếp theo là thêm một rebuildable Ops projection/index theo tháng hoặc một operational read store. Không cần đưa logic đó vào ingestion business data ngay từ bây giờ.

## Read-only rule

Ops không có API để sửa lịch sử:

```text
mark error resolved
force success
patch manifest status
```

Recovery luôn là attempt mới:

```text
run A FAILED
    -> recrawl
run B SUCCESS
```

Ops chỉ project `run B` thành effective attempt; `run A` vẫn được giữ nguyên để audit.
