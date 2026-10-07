# Refactor tổ chức code và metadata PostgreSQL

Ngày: 2026-10-06. Nhánh triển khai: `refactor/isolated-core`.

Đây là thiết kế và kế hoạch chuyển đổi, chưa mô tả hệ thống đã chuyển sang DB.
Mốc kiểm kê: code trong worktree sandbox, bắt đầu từ checkpoint `f98e218`.
Các quyết định bên dưới thay thế hướng giữ manifest làm nguồn trạng thái chính
trong `docs/modernization_review.md` đối với đợt refactor này.

## Phạm vi

Refactor cả tổ chức code theo chức năng, không chỉ thay backend lưu manifest.
Triển khai đầu tiên: ingestion `project` cho một ngày, Bronze reader/writer,
metadata PostgreSQL và adapter Dagster. Giữ các extractor và quy tắc dữ liệu đã
được kiểm thử; không viết lại toàn bộ chỉ để thay tên thư mục.

Export/import, compact, quality repair, watcher, benchmark và Ops được lập bản đồ
ngay nhưng chuyển đổi sau core. Các module `processing/silver/*`,
`processing/releases.py`, `processing/warehouse.py`, `storage/iceberg.py` đã tồn
tại trong checkpoint: giữ nguyên, chưa xác nhận contract và chưa kết nối vào core
mới. Không triển khai thêm Silver/Gold trong đợt này.

Chỉ thay đổi trong worktree sandbox. Không chuyển DB, xóa manifest hay chạy
migration trên môi trường hiện tại. Không dùng chung bucket giữa writer cũ và mới.

## Các phụ thuộc cần tháo gỡ

| Hiện trạng có trong code | Tác động | Đích refactor |
| --- | --- | --- |
| `jobs/ingest.py` chứa parser, chọn ngày, kiểm liveness, lập và chạy kế hoạch | Khó tái sử dụng ngoài CLI | Parser ở CLI, lập kế hoạch ở ingestion, liveness ở metadata |
| `jobs/runner.py` tự tạo HTTP client/storage rồi chọn runner | Khó thay dependency khi test | Hàm lắp ghép dependency riêng, service nhận dependency qua tham số |
| `ingestion/engine/daily_runner.py` tạo DLT pipeline, crawl, ghi page/day/error | Một lỗi có thể đi qua nhiều tầng trong cùng hàm | Tách vòng crawl, writer và quản lý vòng đời attempt |
| `storage/committed.py` gọi `ingestion.coverage` | Reader dữ liệu phụ thuộc bộ chạy ingestion | Bronze reader lấy snapshot từ metadata |
| `orchestration/bronze.py` tự chọn manifest và kiểm trạng thái | Dagster và CLI có thể khác quy tắc reuse/commit | Cùng gọi `ingestion.service.materialize_day` |
| `orchestration/watcher.py` gọi `tools.watch_bid_opening` | Nghiệp vụ phụ thuộc entry point CLI | Watcher service riêng; CLI/Dagster cùng gọi service |
| `quality/repair.py` gọi `_create_pipeline` của daily runner | Repair phụ thuộc hàm nội bộ crawler | Writer DLT có API công khai ở Bronze |
| Hash dữ liệu nằm trong `ingestion/engine/metadata.py`, được storage/quality dùng | Tiện ích dữ liệu phụ thuộc ingestion | Hash/envelope thuộc Bronze |
| `storage/compaction.py` chứa cả plan, execution, recovery | Tầng storage đang điều phối nghiệp vụ | Package compaction riêng, chuyển sau core |
| `storage/transfer.py` chứa cả import/export/commit | Khó kiểm tra riêng từng luồng | Package transfer riêng, chuyển sau core |

## Cấu trúc đích

Các đường dẫn là đích dự kiến; không tạo hàng loạt file rỗng ở bước thiết kế.

```text
src/procurement/
  bootstrap.py                 # lắp client, DB, writer và service
  config.py                    # cấu hình; không đọc env ngầm trong nghiệp vụ
  common/                      # ngày, identity, cancellation; hàm thuần dùng chung
  ingestion/
    service.py                 # use case materialize một resource/ngày
    planning.py                # chọn ngày, refresh/reuse policy
    contracts.py               # ResourceSpec, request/result, source interfaces
    engine/                    # vòng crawl, pagination, page execution, stats
    sources/muasamcong/         # client/source adapters và extractor hiện có
  metadata/
    models.py                  # kiểu dữ liệu/trạng thái, không phải ORM
    errors.py                  # lỗi nghiệp vụ metadata
    contracts.py               # API transaction/lease/commit/query
    service.py                 # quy tắc vòng đời và điều kiện commit
    postgres/
      schema.py                # SQLAlchemy tables/constraints
      attempts.py              # truy vấn attempt/page/error
      leases.py                # claim, renew, fencing
      commits.py               # commit transaction và snapshot query
  bronze/
    models.py                  # BronzeRecord, file descriptor, read snapshot
    hashing.py                 # quy tắc hash giữ nguyên tương thích
    paths.py                   # đường dẫn file; không chọn attempt hiện hành
    writer.py                  # writer interface và buffer
    dlt_writer.py              # adapter DLT và tạo pipeline
    reader.py                  # đọc đúng danh sách file được cố định
    verification.py            # schema/count/lineage/hash
  quality/                     # rule evaluation; audit/repair chuyển sau
  infrastructure/
    database.py                # SQLAlchemy engine/session lifecycle
    object_store.py            # S3 filesystem factory
    logging.py                 # cấu hình logging, redaction
  orchestration/               # definitions, resources, assets, schedules/sensors
  cli/                         # parse args, gọi service, format output/exit code
  api/                         # HTTP routes và giao diện
  ops/                         # read services; backend DB chuyển sau
  compaction/                  # planner, executor, verification, recovery — sau core
  transfer/                    # export, import, archive validation — sau core
  watcher/                     # policy, store, service — sau core
  benchmark/                   # giữ chức năng, chuyển qua service mới sau core
  processing/                  # giữ code hiện có; chưa triển khai trong đợt này
migrations/                    # Alembic; chỉ schema ứng dụng
tests/
  unit/{ingestion,metadata,bronze,...}/
  integration/{metadata,bronze,ingestion}/
  architecture/                # kiểm chiều import
  deployment/                 # Dagster queue → worker → DB/S3 sandbox
```

SQL thuộc `metadata/postgres`, không rải trong ingestion, API hay Dagster.
`infrastructure` chỉ cung cấp kết nối/I/O dùng chung, không trở thành thư mục
chứa tất cả nghiệp vụ. HTTP client riêng của Mua Sắm Công vẫn ở source adapter.
Chỉ tách file khi có trách nhiệm có thể đặt tên và kiểm thử độc lập; không áp số
dòng tối đa, không thêm base repository hoặc framework plugin nếu chưa có nhu cầu.

## Chiều phụ thuộc

```text
CLI / API / Dagster ──→ ingestion.service
                              ├──→ ingestion.engine + source contracts
                              ├──→ metadata.service + metadata.contracts
                              └──→ bronze writer / reader / verification

bootstrap ──→ concrete source adapters, metadata.postgres, bronze.dlt_writer
metadata.postgres ──→ metadata.models/contracts + infrastructure.database
bronze.dlt_writer ──→ bronze.models/writer + DLT
```

- Nghiệp vụ không import Dagster, CLI hoặc API. Metadata không import ingestion.
- Bronze không import ingestion; kiểu envelope/hash chuyển về Bronze để tránh vòng.
- ORM không được trả ra khỏi metadata API: trả DTO bất biến hoặc kết quả rõ kiểu.
- Source extractor không tự ghi commit hay quyết định attempt nào được reuse.
- `bootstrap` được gọi ở entry point/resources; service không tự import bootstrap.
- API/CLI/Dagster không tạo transaction bằng cách gọi tuần tự các CRUD rời rạc.
  API metadata phải bao trọn các thao tác cần nguyên tử.
- Dependency được truyền qua constructor/tham số. Dùng `Protocol` tại ranh giới
  cần fake trong test; không bọc mọi hàm bằng interface.
- Di chuyển đồng thời callers và tests trong mỗi phần; các import cũ chỉ có shim
  tạm thời ở entry point nếu cần tương thích, có danh sách nơi dùng cần loại bỏ.

## Bản đồ chuyển đổi

| Code hiện tại | Đích | Đợt |
| --- | --- | --- |
| `common/settings.py`, `common/logging_config.py` | `config.py`, `infrastructure/logging.py` | Core |
| `common/catalog.py`, `common/resources.py`, `common/dates.py` | Giữ catalog/identity/ngày; tách factory khỏi catalog nếu cần | Core |
| `common/attempts.py`, `models/control.py`, `models/errors.py` | `metadata/models.py`, `metadata/service.py`; model JSON cũ chỉ phục vụ migration | Core |
| `common/errors.py` | Giữ redaction chung; error DTO về metadata, phân loại lỗi crawl ở ingestion | Core |
| `models/bronze.py`, `ingestion/engine/metadata.py` | `bronze/models.py`, `bronze/hashing.py` | Core |
| `jobs/runner.py` | `ingestion/service.py` + `bootstrap.py` | Core |
| `jobs/ingest.py` | `cli/ingest.py` + `ingestion/planning.py`; range CLI đầy đủ chuyển sau luồng một ngày | Core rồi mở rộng |
| `jobs/failures.py` | Phân loại lỗi ở ingestion; truy vấn trạng thái qua metadata | Core |
| `jobs/lock.py`, `storage/execution.py` | Lease/fencing ở metadata; OS lock chỉ giữ cho công cụ cũ | Core |
| `ingestion/batch_runner.py`, `ingestion/engine/daily_runner.py` | Lifecycle về service; crawl thuần ở engine; DLT về Bronze | Core |
| `ingestion/engine/{models,page_runner,records,pagination,stats}.py` | Contracts và engine tương ứng; giữ semantics | Core |
| `ingestion/sources/muasamcong/*` | Giữ package; chuyển dependency envelope/error/writer khi cần | Project trước, resource khác sau |
| `ingestion/coverage.py`, `quality/coverage.py` | Query partition/attempt từ metadata; bỏ quét manifest ở luồng mới | Core / quality sau |
| `storage/control.py`, `storage/errors.py` | Metadata PostgreSQL; adapter đọc lịch sử chỉ ở công cụ migration | Core |
| `storage/object_store.py` | `infrastructure/object_store.py` | Core |
| `storage/bronze.py` | `bronze/writer.py`, `dlt_writer.py`, `paths.py` | Core |
| `storage/committed.py` | `bronze/reader.py`, `verification.py` + metadata snapshot query | Core |
| `storage/io.py`, `storage/events.py` | Giữ cho legacy; bỏ khỏi đường core khi DB là nguồn chính | Sau chuyển callers |
| `orchestration/bronze.py`, `resources.py`, `definitions.py` | Adapter mỏng gọi service; sandbox definitions chỉ bật core khi chuyển | Core |
| `orchestration/workflows.py`, `watcher.py` | Gọi service của chức năng tương ứng, không gọi CLI | Sau core |
| `storage/compaction.py`, `compact_parquet.py`, `tools/compact_bronze.py` | `compaction/{planner,executor,verification,recovery}.py` + CLI | Sau core |
| `storage/transfer.py`, `transfer_archive.py`, `tools/bronze_transfer.py` | `transfer/{export,importer,archive}.py` + CLI | Sau core |
| `quality/repair.py` | `quality/repair/{planner,executor,reconstruction,verification}.py` | Sau core |
| Các module quality còn lại | Giữ rules/contracts; persistence sang DB khi chuyển resource tương ứng | Rules cần thiết trước |
| `ops/index.py`, `sync.py`, `repositories/*`, `service.py` | DB read repositories + query services; bỏ SQLite sync khi cutover | Sau core |
| `ingestion/bid_opening_watch.py`, `tools/watch_bid_opening.py` | `watcher/` + CLI | Sau core |
| `benchmark/*`, `api/benchmark.py`, `tools/benchmark.py` | Giữ feature; dùng service công khai, HTTP route mỏng | Sau core |
| Explorer/count/profile/audit tools | CLI mỏng + chức năng query/audit có tên cụ thể; reader dùng snapshot | Sau core |
| `tools/recover_result_2024.py` | Công cụ recovery lịch sử; không biến ngoại lệ thành policy core | Đóng băng |
| `processing/*`, `storage/iceberg.py` | Giữ nguyên, không đưa vào core mới | Ngoài đợt |

Danh sách từng file, số dòng và imports hiện tại: [module-inventory.csv](module-inventory.csv).
Đây là snapshot phục vụ refactor, không phải mã dùng để chạy ứng dụng.

## Contract core cần hiện thực

API ở đây mô tả hành vi, chữ ký cuối cùng sẽ chốt cùng schema migration:

| API | Cam kết |
| --- | --- |
| `materialize_day(request) -> result` | Một resource/ngày; trả commit, attempt nếu có, reused và metrics |
| `begin_or_reuse(partition, refresh, request_id)` | Transaction chọn reuse hoặc tạo attempt + claim lease; không SELECT rồi INSERT rời nhau |
| `renew_lease(attempt_id, owner, generation)` | Gia hạn có điều kiện bằng thời gian DB; mất quyền thì báo lỗi |
| `record_page / record_errors` | Gắn đúng attempt, có khóa chống ghi trùng khi retry ghi metadata |
| `publish_commit(attempt_id, generation, expected_base, files, checks)` | Transaction kiểm quyền, base commit, ghi commit/files, SUCCESS và cập nhật partition |
| `fail_attempt(attempt_id, generation, reason)` | Chỉ thay attempt chưa commit và đúng quyền; không hạ SUCCESS thành FAILED |
| `resolve_request(request_id)` | Xác định kết quả sau khi mất phản hồi DB; không tự tạo attempt mới |
| `get_snapshot(partitions)` | Cố định commit IDs và file list bằng snapshot đọc nhất quán |

Reuse là đọc dữ liệu đã công bố: có thể trả commit cũ khi một refresh đang chạy,
nhưng phải trả rõ đang có refresh; không khẳng định đó là kết quả refresh mới.
Refresh/new write cần lease. Commit rỗng vẫn hợp lệ nếu kết quả nguồn và kiểm tra
xác nhận không có dữ liệu; không bắt buộc mọi commit phải có file.

Attempt đề xuất: `RUNNING → SUCCESS | FAILED | CANCELED | ABANDONED`.
`ABANDONED` nghĩa quyền chạy cũ bị thu hồi, không khẳng định process vật lý đã chết.
Retry nghiệp vụ tạo attempt mới. Mất phản hồi commit là tình trạng của caller cần
đối soát; không vội ghi FAILED hoặc tạo commit thứ hai.

Lease generation tăng khi cấp lại quyền và không reset khi giải phóng lease.
Worker hết hạn không được tự gia hạn hay commit bằng generation cũ. Việc kiểm
lease và cập nhật commit phải dùng cùng transaction, khóa/ràng buộc thích hợp;
không giữ transaction mở trong suốt lúc crawl/upload.

Upload file vào namespace attempt riêng, xác minh xong mới publish DB. DB không
thể rollback upload S3. File chưa commit là orphan có thể dọn sau; không được reader
tự nhận làm dữ liệu hợp lệ. File đã commit không ghi đè; retention phải bảo vệ các
snapshot đang đọc. Nếu DB không dùng được, luồng mới dừng rõ ràng, không fallback
sang lựa chọn manifest cũ trong cùng bucket.

## DB và công cụ

Core bắt đầu với bảy bảng logic: `partitions`, `attempts`, `attempt_pages`,
`partition_leases`, `commits`, `commit_files`, `errors`. DDL, foreign keys, unique
constraints, indexes và lease locking sẽ là bước kế tiếp. `request_id` cần bảo vệ
retry của cùng yêu cầu. Bảng quality checks được bổ sung trước khi chuyển resource
cần quality; không ép toàn bộ 12 bảng vào luồng project đầu tiên.

Dagster quản lý job/run riêng. Core lưu correlation `dagster_run_id` khi có;
CLI không phải dựng giả một Dagster run. Range plans, operation plans và lịch sử
run ngoài Dagster thuộc các đợt mở rộng sau.

Tận dụng SQLAlchemy 2 đã có trong dependency graph, Pydantic, HTTPX, DLT,
PyArrow, pytest đang dùng. Alembic là lựa chọn đề xuất cho migration; xác minh
compatibility và khai báo dependency trực tiếp khi hiện thực DB. Không tự viết
ORM, migration engine, HTTP retry framework hay worker queue mới. Chưa thêm
dependency ở bước tài liệu này. Không retry mù transaction commit có kết quả
chưa xác định. Không cần thêm Redis/Celery vì Dagster đã điều phối công việc.

## Thứ tự triển khai và điều kiện hoàn thành

| Phần / commit dự kiến | Công việc | Điều kiện hoàn thành |
| --- | --- | --- |
| A — bản đồ kiến trúc (bước này) | Tài liệu, inventory, ranh giới core/deferred | Bao phủ module hiện tại, phân biệt rõ hiện trạng và đề xuất |
| B — schema và migration | DDL bảy bảng, constraints, DB config, SQLAlchemy/Alembic | Upgrade trên DB sandbox rỗng; kiểm constraints trên PostgreSQL thật |
| C — metadata lifecycle | begin/reuse, lease, renew, publish, fail, read snapshot | Hai worker cạnh tranh; lease hết hạn; stale worker bị từ chối; lost response idempotent |
| D — Bronze và engine | Di chuyển envelope/hash; tách DLT writer; engine nhận dependencies | Giữ hash, schema/lineage; kiểm record/count/corruption bằng test hiện có |
| E — project một ngày | Service + source adapter + CLI tối thiểu + Dagster adapter | Ingest → Parquet → DB commit; lần hai reuse, không gọi nguồn |
| F — resource còn lại | Chuyển từng resource cùng rule chất lượng | Test đa bảng, empty day, lỗi một phần; không công bố dữ liệu thiếu |
| G — công cụ và Ops | Chuyển chức năng deferred từng phần | Không còn reader/writer nghiệp vụ dùng metadata cũ trong deployment mới |
| H — dữ liệu lịch sử/cutover | Import metadata, đối chiếu, backup/restore, dừng writer cũ | Không đổi dữ liệu cũ cho tới khi kiểm chứng; một nguồn trạng thái chính |

Không move toàn bộ repo trong một commit. Bước D/E có thể chia nhỏ theo dependency,
nhưng mỗi commit phải có callers và tests tương ứng; tránh module trống hoặc hai
bản nghiệp vụ độc lập. Không yêu cầu scaffold cả cấu trúc đích trước khi có chức năng.

Trong quá trình chuyển, code cũ được giữ để so sánh nhưng không được chạy trên
bucket DB-managed. Khi bật service DB mới, sandbox dùng bucket mới sạch và
definitions riêng chỉ chứa asset đã chuyển. Smoke năm asset hiện tại chỉ chứng
minh checkpoint hoạt động; thay bằng smoke core mới khi chuyển definitions.

## Kiểm chứng và debug

- Unit: pagination, hash, phân loại lỗi, quyết định refresh/reuse; không mock SQL
  để khẳng định transaction/locking đúng.
- Integration PostgreSQL thật: simultaneous claim/commit, generation cũ, lease
  expiry, unique request, read snapshot nhất quán, commit rollback.
- Integration S3/Parquet thật + mock nguồn: upload thiếu, checksum sai, empty day,
  fail trước commit, mất phản hồi sau commit, retry không công bố trùng.
- Deployment: Dagster queue/daemon/worker chạy đúng code DB mới, kiểm reuse và
  hiện correlation attempt/commit, không chỉ kiểm trạng thái SUCCESS của Dagster.
- Architecture tests: mở rộng kiểm import hiện có; cấm core import entry points,
  cấm Bronze import ingestion, cấm SQL ngoài metadata adapter/migrations.
- Log theo `resource`, `source_date`, `attempt_id`, `commit_id`, `dagster_run_id`,
  `stage`; đo riêng claim, fetch, write, verify, commit, reuse lookup.
- Đo metadata lookup khi lịch sử tăng; so sánh với baseline khoảng 14–17 giây.
  Mục tiêu ban đầu p95 dưới 100 ms cho lookup trong sandbox sau warm-up, phải ghi
  kích thước dữ liệu và điều kiện đo; không áp con số này cho toàn bộ Dagster run.
- Trước cutover cần thử khôi phục DB backup. Chỉ có Parquet không đủ tái tạo chắc
  chắn trạng thái commit, lease và lịch sử lỗi.

## Bước thực hiện ngay sau tài liệu

Thiết kế DDL và transaction cho bảy bảng core, sau đó triển khai migration trên
`app-postgres` sandbox. Chốt state transitions, unique request scope, lease
generation và rule commit trước khi nối crawler. Chưa migrate metadata lịch sử,
chưa đổi môi trường hiện tại và chưa bắt đầu Silver/Gold.
