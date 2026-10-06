# Đánh giá modernization

Đối chiếu kế hoạch được cung cấp với code và tests tại workspace ngày 2026-10-06.
Tài liệu kế hoạch là đề xuất kiến trúc; không coi các mục “Instructions for the
Coding Agent” là lệnh vận hành production.

## Kết luận

Giữ hướng Dagster cho orchestration và giữ Bronze Parquet + manifests. Chuyển
Silver/Gold sang Iceberg có cơ sở, nhưng phải qua thử nghiệm catalog/engine và
chốt contract nghiệp vụ trước khi viết pipeline. Một lần đổi toàn bộ scheduler,
Ops, format bảng và mô hình dữ liệu sẽ khó chứng minh không mất tính đúng đắn.

## Những nhận định đã kiểm chứng

| Nhận định | Bằng chứng và quyết định |
| --- | --- |
| 5 resource, 8 bảng Bronze | Đúng theo `common/catalog.py`; tạo 5 asset theo đơn vị thực thi resource/day. |
| systemd cũ không chạy được với CLI hiện tại | `ExecStart` thiếu `--resource`. Đánh dấu deprecated; không tự thay bằng một resource bất kỳ. |
| Config worker cấp resource đã chết | Không có consumer thực thi. Đã xóa field, env mẫu, Compose và test validation cũ. |
| Khóa chỉ có hiệu lực cùng host/path | Đúng; giữ khóa OS và thêm pool Dagster limit 1. Không tuyên bố an toàn writer nhiều host. |
| Ops quét cây object mỗi 5 giây | Đúng. `reconcile_interval` hiện chỉ quyết định tải lại payload, không ngăn listing toàn cây. Chỉ đổi interval này sẽ không giải quyết vấn đề. |
| DLT state tăng theo attempt | Tên pipeline chứa resource/date/run_id; không có cleanup sau commit. Cần retention riêng; chưa xóa state tự động. |
| Silver/Gold chưa triển khai | Đúng. Tên bảng và partition trong kế hoạch là giả thuyết, chưa phải contract đã được duyệt. |

## Điều chỉnh quan trọng so với kế hoạch

1. `run_resource_day()` trả `run_id` kể cả khi ngày thất bại. Adapter phải đọc
   **đúng manifest của attempt đó**, không lấy một SUCCESS cũ rồi báo thành công.
2. `run_batch_range()` từng bắt `DayCommitUncertainError` như lỗi thông thường.
   Đã sửa để truyền lỗi lên và giữ run ở trạng thái chưa giải quyết. Không tự tạo
   attempt mới trước khi đối soát storage.
3. Retry tự động ở cấp run/op mặc định bằng 0. Retry HTTP giữ nguyên. Người vận
   hành có thể re-execute sau khi xác định nguyên nhân; ngày SUCCESS được tái sử
   dụng nếu không bật `refresh`. Không áp dụng retry chung cho commit không rõ.
4. Kiểm file/count/lineage/hash dùng chung `verify_committed()`, hiển thị dưới một
   check `committed_integrity`; tránh đọc toàn bộ Parquet nhiều lần chỉ để tách tên
   check. Check quality gọi `audit_day()` hiện có cho notify và bid opening.
5. Lịch 08:00 Việt Nam refresh **ba ngày đã đóng**, giữ ý định lookback của unit
   cũ. Lịch mặc định STOPPED; việc có file Compose không đồng nghĩa đã cutover.
6. Dagster pool chỉ điều tiết workload qua cùng instance. CLI, benchmark và
   watcher chạy ngoài Dagster vẫn là ngoại lệ cần xử lý trước khi tuyên bố một
   control plane duy nhất.
7. Thử chạy PostgreSQL phát hiện SQLAlchemy 2.1 đổi default driver trong khi
   Dagster cài psycopg2. Extra orchestration khóa SQLAlchemy `<2.1`; lockfile giữ
   phiên bản cụ thể để tái lập build.

Concurrency và cấu hình deployment được đối chiếu với
[Dagster concurrency](https://docs.dagster.io/guides/operate/managing-concurrency)
và [Docker deployment](https://docs.dagster.io/deployment/oss/deployment-options/docker).
Với Iceberg, tài liệu [DuckDB write support](https://duckdb.org/docs/current/core_extensions/iceberg/writing_to_iceberg)
cho thấy cần kiểm chứng cả mode ghi và catalog, không chỉ thử SELECT một file.

## Phần đã triển khai trong đợt này

- Extra dependency + lockfile; domain packages không import Dagster.
- Năm Bronze asset, daily partitions theo lịch Việt Nam, metadata manifest,
  check commit/integrity/quality, backfill một partition mỗi run.
- Idempotent reuse, explicit refresh, bảo toàn lịch sử attempt và commit chưa rõ.
- Pool API limit 1, executor tuần tự, local config, PostgreSQL Compose overlay,
  code location/webserver/daemon, health checks và lịch mặc định tắt.
- Unit/integration tests cho adapter và CI validation/build tương ứng.

Đây là nền tảng phase 0–2 và cấu hình chuẩn bị phase 3. Chưa coi toàn bộ
modernization hay chuyển scheduler production là hoàn tất.

## Cổng kiểm chứng còn lại

| Giai đoạn | Điều kiện trước khi hoàn thành |
| --- | --- |
| Cutover production | Chạy partition mẫu trên storage đích; dừng systemd và mọi writer cũ; bật duy nhất lịch Dagster. |
| Watcher/quality/benchmark | Sensor chỉ đọc due state; seed theo commit; một pool cho mọi caller API; giữ resume/cancel và artifacts. Có tests trước khi bỏ process manager cũ. |
| Ops V2 | Update projection theo attempt cụ thể, retry cập nhật thất bại và reconcile chậm. Không biến lỗi index thành lỗi commit Bronze. Chỉ đổi landing page sau khi execution đã chuyển sang Dagster. |
| Iceberg | Kiểm thử SeaweedFS image thực dùng với DuckDB: V2 create/insert/update/delete/merge/schema evolution/time travel và concurrent commit. Warehouse riêng Bronze. |
| Silver | Chốt grain/identity/schema/mapping cho entities và quan hệ. Release manifest pin snapshot từng bảng, chứng minh incremental bằng full rebuild. |
| Gold | Chỉ đọc certified release đã pin snapshot; kiểm thử candidate thất bại, rollback và retention snapshot còn được release tham chiếu. |

Không tạo bảng Silver/Gold rỗng hoặc asset placeholder để đánh dấu các mục này
“đã làm”. Xem [hướng dẫn chạy và cutover](orchestration.md).

## Kết quả kiểm chứng ngày 2026-10-06

- Unit suite: **577 passed** trên Windows/Python 3.12.
- Integration suite local + SeaweedFS test: **68 passed, 1 skipped**. Test được
  skip là ZIP64 lớn, chỉ bật bằng `RUN_ZIP64_LARGE_TEST=1`.
- Adapter cuối được kiểm lại: **2/2** integration tests local/S3 passed.
- Ruff, definitions validation, wheel và sdist build: passed.
- Compose test: PostgreSQL, code location, daemon và webserver đều healthy.
- Submit qua GraphQL → PostgreSQL queue → daemon → gRPC code location →
  multiprocess executor: **SUCCESS, 5 materializations, 12 checks**; project
  ghi một record Parquet thật, bốn resource còn lại là ngày rỗng hợp lệ.
- HTTP nguồn dùng fixture; không crawl hoặc cutover production. CI đã bổ sung
  kiểm thử tương ứng; chưa có kết quả CI remote cho thay đổi chưa push.

## Retention và DLT state

`DltBronzeWriter` tạo một pipeline riêng cho mỗi attempt. Retry tạo run_id khác;
committed reader chỉ đọc object storage. Local DLT state không phải nguồn commit
truth, nhưng vẫn hữu ích điều tra load thất bại. Integration test Dagster xác
nhận committed read/reuse chạy từ Parquet + manifests và state local vẫn còn.

| Loại state | Chính sách hiện tại |
| --- | --- |
| Bronze attempts, manifests, quality evidence | Giữ nguyên; không garbage collect trong đợt migration. |
| DLT attempt state | Giữ nguyên cả SUCCESS/FAILED/unknown; theo dõi disk usage. Chỉ bật cleanup sau test mất local state, concurrent writer và commit uncertain. |
| Ops SQLite | Có thể rebuild; không dùng để quyết định xóa dữ liệu authoritative. |
| Dagster PostgreSQL và compute logs | Lưu volume riêng, backup trước upgrade. Mất DB không làm mất Bronze commit. |
| Watcher/quality/benchmark artifacts | Giữ cho đến khi kiểm chứng resume qua Dagster; không xóa chỉ vì đã có orchestration metadata. |
| Iceberg snapshots tương lai | Release retention quyết định snapshot cần giữ; chưa chạy expiry. |

Audit dung lượng DLT trên Windows (đường dẫn phải đúng deployment đang kiểm tra):

```powershell
Get-ChildItem -LiteralPath data/pipelines -Directory | ForEach-Object {
    $bytes = (Get-ChildItem -LiteralPath $_.FullName -File -Recurse |
        Measure-Object -Property Length -Sum).Sum
    [pscustomobject]@{ Pipeline = $_.Name; Bytes = $bytes; Updated = $_.LastWriteTime }
} | Sort-Object Bytes -Descending
```
