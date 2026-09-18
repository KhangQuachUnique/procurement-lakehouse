# Bronze Explorer và đọc dữ liệu

[Mục lục](../README.md) · [Cấu hình](setup.md) · [Trạng thái dữ liệu](ingestion.md#manifest-và-dữ-liệu-đã-commit)

## Mở DuckDB UI

Explorer dùng cấu hình S3 trong `.env` để tạo các view trên Parquet. Cần có dữ liệu Bronze và quyền đọc bucket. DuckDB đi kèm dependency project; lần đầu chạy có thể cần mạng để tải extension httpfs/ui.

```powershell
python -m procurement.tools.bronze_explorer
python -m procurement.tools.bronze_explorer --port 4214
```

| Tham số | Mặc định | Ý nghĩa |
| --- | --- | --- |
| `--port` | `4213` | Cổng UI, từ 1 đến 65535 |
| `-h`, `--help` | — | Hiển thị trợ giúp |

Mở `http://localhost:4213` với cổng mặc định. Terminal in các view trong schema `bronze_raw`; Ctrl+C dừng server. Restart Explorer khi có table mới để discover lại. Credential DuckDB được giữ trong process, không tạo persistent secret.

## Query dữ liệu vật lý

```sql
SELECT source_id, source_date, run_id, payload, filename
FROM bronze_raw.project_detail
LIMIT 20;

SELECT source_date, run_id, count(*) AS records
FROM bronze_raw.notify_contractor_standard_detail
WHERE source_date BETWEEN DATE '2022-10-01' AND DATE '2022-10-31'
GROUP BY source_date, run_id
ORDER BY source_date, run_id;
```

`filename` chỉ ra file gốc. Envelope Bronze có `source_id`, `source_version` (có thể null), `run_id`, `source_date`, `ingested_at`, `content_hash`, `payload`. Các table hiện có được liệt kê ở [jobs ingestion](ingestion.md#chọn-lệnh). Khi nhiều dataset có table trùng tên, view dùng `<dataset>__<table>`.

`bronze_raw` bao gồm các attempt vật lý, kể cả partial data của FAILED và nhiều lần crawl cùng ngày. Counts ở đây không phải committed counts. Để inspect một attempt cụ thể, lọc thêm `run_id` từ Ops; để đọc cho downstream, dùng committed reader dưới đây.

## Đọc đúng dữ liệu đã commit bằng Python

```python
from datetime import date

from procurement.common.catalog import get_resource
from procurement.storage.committed import (
    iter_committed_records,
    select_committed_days,
    verify_committed,
)
from procurement.storage.object_store import create_s3_filesystem

fs = create_s3_filesystem()
selection = select_committed_days(
    fs, get_resource("khlcnt"), date(2022, 10, 1), date(2022, 10, 31)
)
print(verify_committed(fs, selection))

for table, record in iter_committed_records(fs, selection, verify_hash=True):
    # Ghi vào staging của downstream; chỉ publish khi đọc hết và không có lỗi.
    pass
```

`select_committed_days` chọn cố định effective SUCCESS cho mỗi ngày và các bảng thuộc resource; thiếu ngày committed sẽ báo lỗi. `verify_committed` đọc hết dữ liệu, kiểm count/lineage/hash và trả số days/files/records. `iter_committed_records` mặc định `verify_hash=False`; đặt True để kiểm hash khi đọc.

Iterator kiểm count khi đọc hết ngày; dừng sớm không xác nhận được tính đầy đủ. SUCCESS 0 record không cần file. Không dùng SQL DISTINCT để thay thế chính sách chọn attempt. Nếu chỉ cần đối soát mà không viết Python, dùng `python -m procurement.jobs.ingest verify` với [tham số ngày](ingestion.md#tham-số).

## Lỗi thường gặp

| Lỗi | Kiểm tra |
| --- | --- |
| Không tìm thấy Bronze tables | Bucket/layout và việc đã có attempt ghi Parquet; ngày rỗng không sinh table |
| Không kết nối S3 | Endpoint, port, access key/secret và quyền bucket |
| Cổng UI bận | Dùng `--port` khác |
| Không tải được extension | Kết nối tới kho extension DuckDB hoặc cache extension của môi trường |
| Raw có nhiều record hơn manifest | Kiểm run_id; có thể đang đọc nhiều attempt hoặc partial files |
