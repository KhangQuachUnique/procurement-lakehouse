# Bronze Explorer

Dùng DuckDB UI để xem trực tiếp dữ liệu Bronze đang nằm trong SeaweedFS mà không cần cấu hình S3 thủ công mỗi lần.

## Cách chạy

Đảm bảo SeaweedFS đang chạy và `.env` đã có các biến object storage giống khi chạy ingestion:

```text
OBJECT_STORAGE_ENDPOINT
OBJECT_STORAGE_ACCESS_KEY
OBJECT_STORAGE_SECRET_KEY
OBJECT_STORAGE_BUCKET
```

Sau đó chạy:

```powershell
python -m procurement.tools.bronze_explorer
```

Script sẽ tự:

1. Đọc cấu hình object storage từ `.env`.
2. Kết nối SeaweedFS S3 API.
3. Tạo DuckDB S3 secret tạm thời trong memory.
4. Tự discover các table bên dưới `bronze/`.
5. Tạo view trong schema `bronze_raw`.
6. Mở DuckDB UI tại `http://localhost:4213`.

Không cần cài DuckDB CLI riêng. Package `duckdb` được cài cùng project.

Có thể đổi port:

```powershell
python -m procurement.tools.bronze_explorer --port 4214
```

Dừng explorer bằng `Ctrl+C` tại terminal đang chạy.

## Query mẫu

Xem TBMT:

```sql
SELECT *
FROM bronze_raw.notify_contractor_standard_detail
LIMIT 100;
```

Đếm số record theo ngày:

```sql
SELECT
    source_date,
    count(*) AS records
FROM bronze_raw.notify_contractor_standard_detail
GROUP BY source_date
ORDER BY source_date;
```

Tìm một entity theo `source_id`:

```sql
SELECT *
FROM bronze_raw.notify_contractor_standard_detail
WHERE source_id = '...';
```

Mỗi view có thêm cột `filename` để biết record đang đến từ Parquet object nào.

## Raw khác committed

`bronze_raw` cố ý scan các Parquet object đang tồn tại vật lý. Một attempt `FAILED` có thể đã ghi partial Bronze trước khi fail, nên raw view có thể thấy các record đó.

Semantics chính thức của pipeline vẫn là:

```text
DayManifest SUCCESS = committed
```

Khi cần kiểm tra ngày nào thực sự committed, failed hoặc missing thì dùng Ops. Bronze Explorer chỉ dùng để inspect payload, schema và dữ liệu vật lý đã crawl.

## Security

DuckDB secret được tạo theo kiểu temporary mặc định nên chỉ tồn tại trong process explorer hiện tại. Script không ghi access key/secret key vào source code hay persistent DuckDB secret store.
