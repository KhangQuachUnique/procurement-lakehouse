# Ops

Ops là lớp đọc trạng thái vận hành của ingestion. `_control` và `_errors` trong object storage vẫn là source of truth; Ops không sửa manifest hay error cũ.

## Mental model

```text
Resource
  -> Source date
      -> Attempt (run_id + source_date)
          -> Pages
          -> Errors
```

Một ngày có thể có nhiều attempt:

```text
2026-09-12
├── run A FAILED
├── run B FAILED
└── run C SUCCESS  <- effective attempt
```

Nếu có ít nhất một attempt `SUCCESS`, attempt thành công mới nhất là `effective_run_id`. Attempt fail cũ vẫn được giữ nguyên để audit.

## Health

Mỗi resource có một health projection:

```text
healthy   latest source date có SUCCESS, không còn failed date chưa recover
          và latest successful source date không cũ quá 1 ngày

degraded latest source date có SUCCESS nhưng còn failed date chưa recover
          hoặc dữ liệu đang stale

failed    latest source date chưa có SUCCESS
          hoặc resource chưa từng SUCCESS

no_data   chưa có attempt nào
```

## API

Chạy:

```powershell
uvicorn procurement.api.main:app --reload
```

### Overview

```text
GET /api/ops/overview
```

Trả health của toàn bộ resource.

### Resources

```text
GET /api/ops/resources
```

Resource hiện hỗ trợ:

```text
project
khlcnt
notify_contractor
contractor_result
```

### Timeline theo ngày

```text
GET /api/ops/resources/{resource}/dates
```

Mặc định trả 30 ngày đã đóng gần nhất. Có thể truyền:

```text
start_date=YYYY-MM-DD
end_date=YYYY-MM-DD
```

Window tối đa 366 ngày.

Mỗi ngày có trạng thái:

```text
success
failed
missing
```

`missing` nghĩa là trong window được hỏi không có attempt nào cho ngày đó.

### Chi tiết một ngày

```text
GET /api/ops/resources/{resource}/dates/{source_date}
```

Trả toàn bộ attempt của ngày và `effective_run_id` nếu đã có attempt thành công.

### Range run

```text
GET /api/ops/runs/{run_id}
```

Trả `RunManifest` dưới API read model cùng danh sách day attempts của run đó. Client không cần biết resource trước; service tự resolve run trong các resource hiện hỗ trợ.

### Attempt detail

```text
GET /api/ops/attempts/{run_id}/{source_date}
```

Trả:

```text
attempt summary
pages[]
errors[]
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

`limit` mặc định 200, tối đa 1000.

## Read-only rule

Ops hiện không có API:

```text
mark error resolved
force success
patch manifest status
```

Recovery luôn được biểu diễn bằng một attempt mới:

```text
run A FAILED
    -> recrawl
run B SUCCESS
```

Ops chỉ project `run B` thành effective attempt; `run A` không bị sửa.

## Storage strategy hiện tại

Hiện Ops đọc trực tiếp JSON/JSONL từ SeaweedFS qua repository layer:

```text
Object Storage
├── _control
└── _errors
       |
       v
ControlRepository / ErrorRepository
       |
       v
OpsService
       |
       v
FastAPI
```

Đây phù hợp với quy mô hiện tại. Nếu operational metadata tăng đủ lớn để glob object storage trở thành bottleneck, có thể thêm PostgreSQL làm read store/index cho runs, attempts, pages và errors; procurement business data vẫn ở Lakehouse.
