# Chuyển Bronze giữa các máy bằng ZIP theo năm

[Mục lục](../README.md) · [Cấu hình storage](setup.md) · [Ingestion](ingestion.md)

Máy gửi xuất dữ liệu đã crawl thành một ZIP; chuyển file bằng USB, ổ mạng hoặc công cụ
chia sẻ file; máy nhận nhập vào bucket của mình. Hai máy cần cùng phiên bản project và
cấu hình `OBJECT_STORAGE_*` riêng trong `.env`. Bucket phải tồn tại; export cần quyền đọc,
import cần đọc/ghi. Các lệnh này không gọi MuaSamCong và không cần token crawl.

## Máy gửi

Chạy từ thư mục gốc project. Năm được chọn theo `source_date`, không phải thời điểm crawl.
Chỉ nhận năm đã kết thúc theo lịch Việt Nam.

```powershell
# Xem phạm vi, số file và tổng byte nguồn; chưa tạo ZIP hay kiểm nội dung Parquet
uv run --locked python -m procurement.tools.bronze_transfer export --year 2025 --output ./exports/bronze-2025.zip --dry-run

# Xuất cả bốn resource
uv run --locked python -m procurement.tools.bronze_transfer export --year 2025 --output ./exports/bronze-2025.zip

# Hoặc chỉ xuất KHLCNT, gồm cả bảng kế hoạch và bảng gói thầu
uv run --locked python -m procurement.tools.bronze_transfer export --year 2025 --resource khlcnt --output ./exports/khlcnt-2025.zip
```

`--resource` nhận `all` (mặc định), `project`, `khlcnt`, `notify_contractor`,
`contractor_result`. Tên file đầu ra phải chưa tồn tại; script không ghi đè ZIP cũ.

Nếu năm còn thiếu ngày, export vẫn thành công với `coverage_complete=false`,
`missing_days` và `missing_dates` theo từng resource. Nếu không có ngày SUCCESS nào,
script báo lỗi và không tạo ZIP. Ngày SUCCESS với 0 record vẫn được chuyển.

Trong lúc xuất, dữ liệu được ghi vào file `.part` bên cạnh file đích. Chỉ sau khi kiểm
tra gói và đối chiếu lại manifest nguồn, script mới công bố tên ZIP chính thức. Không
chuyển file `.part`. Lỗi thông thường được dọn file tạm; sau mất điện/kill cứng có thể
xóa đúng file `.part` còn sót và chạy lại export từ đầu.

## Máy nhận

Chuyển ZIP vào `./imports/`, kiểm tra `.env` đang trỏ đến bucket muốn bổ sung rồi chạy:

```powershell
# Kiểm checksum, manifest, record count, lineage và payload hash hoàn toàn offline
uv run --locked python -m procurement.tools.bronze_transfer inspect --archive ./imports/bronze-2025.zip

# Kiểm gói và xem các ngày sẽ nhập/bỏ qua/bị chặn; không ghi storage
uv run --locked python -m procurement.tools.bronze_transfer import --archive ./imports/bronze-2025.zip --dry-run

# Nhập thật; tự lấy năm và resource từ gói
uv run --locked python -m procurement.tools.bronze_transfer import --archive ./imports/bronze-2025.zip
```

`inspect` không kết nối storage. Import luôn kiểm lại toàn bộ gói, kể cả khi đã chạy
inspect hoặc dry-run trước đó. Dry-run là bước xem trước tùy chọn.

| Tình trạng máy nhận                                              | Hành vi                                                             |
| ---------------------------------------------------------------- | ------------------------------------------------------------------- |
| Đã có SUCCESS cùng resource/ngày                                 | Bỏ qua cả ngày, giữ dữ liệu máy nhận dù ZIP có attempt mới hơn      |
| Ngày thiếu hoặc chỉ có FAILED ở run khác                         | Nhập dữ liệu từ ZIP                                                 |
| Chưa có SUCCESS và bị run RUNNING chặn                           | Dừng trước khi ghi, báo `blocked_days`; không tự vượt stale/unknown |
| Object cùng key có cùng SHA-256                                  | Tái sử dụng, phục vụ chạy lại sau nhập dở                           |
| Object cùng key khác nội dung hoặc có Parquet thừa trong attempt | Dừng trước khi ghi, báo `conflicts`; không ghi đè                   |

Script giữ nguyên Parquet, run ID, ngày nguồn và thời gian crawl của từng ngày. Dữ liệu và metadata
được đọc lại tại đích trước khi công bố `day.json` SUCCESS. Commit theo từng ngày:
nếu mất kết nối hoặc bị ngắt, các ngày đã hoàn tất vẫn sử dụng được. Chạy lại **cùng lệnh
với cùng ZIP** để bỏ qua các ngày đó và tiếp tục phần còn lại. Không sửa manifest thủ công
và không xóa dữ liệu cũ để thử lại. Nếu báo commit chưa xác định, kiểm tra lại storage rồi
chạy lại; script không ghi FAILED đè lên một SUCCESS có thể đã được lưu.

Với run RUNNING cũ, xác minh worker đã dừng rồi dùng quy trình repair trong
[hướng dẫn ingestion](ingestion.md); transfer không có tùy chọn vượt khóa hoặc ghi đè.

## Kiểm tra kết quả

JSON ở stdout; tiến độ/lỗi ở stderr. Báo cáo có mã gói, năm, resource, số object,
tổng byte chưa nén, ngày thiếu và `verified` cho nội dung gói.

Import có thêm danh sách `planned_days`, `imported_days`, `skipped_days`, `blocked_days`,
`conflicts`; `uploaded_objects`, `uploaded_bytes`, `reused_objects` và
`destination_verified` cho các ngày vừa nhập. Lỗi trong quá trình nhập giữ báo cáo tiến
độ trong JSON; ngắt bằng Ctrl+C trả code 130 và hướng dẫn chạy lại.

```powershell
uv run --locked python -m procurement.jobs.ingest status --year 2025
uv run --locked python -m procurement.jobs.ingest verify --year 2025

# Tùy chọn: crawl bổ sung các ngày còn thiếu; cần cấu hình token nguồn
uv run --locked python -m procurement.jobs.ingest repair --year 2025 --continue-on-error
```

Nếu ZIP chưa đủ năm, `status`/`verify --year` có thể trả code 1 vì thiếu coverage.
`verify` hiện có không kiểm toàn bộ các ngày đã có của resource còn thiếu coverage;
dùng `verified`/`destination_verified` của transfer để đối soát phần được chuyển.
Import không tự chạy repair. Ops tự đồng bộ metadata mới vào index hiện có; có thể
cần chờ lượt đồng bộ tiếp theo trước khi giao diện cập nhật.

| Exit code | Ý nghĩa                                                             |
| --------- | ------------------------------------------------------------------- |
| `0`       | Hoàn tất; gói thiếu ngày hoặc mọi ngày bị bỏ qua vẫn có thể trả 0   |
| `1`       | Gói/manifest hỏng, xung đột, bị chặn, lỗi storage hoặc verification |
| `2`       | Tham số không hợp lệ                                                |
| `130`     | Bị ngắt                                                             |

## Định dạng và giới hạn

ZIP64, DEFLATE mức 1; hỗ trợ member và archive lớn hơn 4 GiB. Script đọc theo khối,
không giữ cả năm dữ liệu trong RAM và không giải nén cả gói cùng lúc. Cần chỗ trống cho
ZIP đầu ra và, trong thư mục tạm hệ điều hành, tối thiểu một file Parquet lớn nhất
ở dạng chưa ZIP. Kiểm tra đầy đủ phải đọc dữ liệu nhiều lượt nên có thể mất thời gian.

Gói mới chứa `transfer.json` định dạng phiên bản 2 và `objects/` với đường dẫn tương đối.
Script vẫn đọc được gói phiên bản 1; hai máy nên dùng cùng phiên bản project đã cập nhật.

```text
transfer.json
objects/bronze/muasamcong/<table>/source_date=<date>/run_id=<id>/*.parquet
objects/_control/muasamcong/<resource>/run_id=<id>/run.json
objects/_control/muasamcong/<resource>/run_id=<id>/source_date=<date>/day.json
objects/_control/muasamcong/<resource>/run_id=<id>/source_date=<date>/pages/page-*.json
```

Inventory chứa resource/ngày/run, ngày thiếu, số byte và SHA-256 từng object. Endpoint,
bucket, `.env`, DLT state, heartbeat, Ops SQLite và lịch sử attempt cũ không được đóng gói.
SHA-256 phát hiện lỗi nội dung khi truyền file; gói không có chữ ký xác thực người gửi.
Giới hạn metadata: `transfer.json` tối đa 256 MiB, mỗi manifest JSON tối đa 16 MiB;
inventory được giữ trong RAM. Các giới hạn này không áp dụng cho file Parquet.

Run cũ theo tháng/năm được hỗ trợ: chỉ lấy những ngày SUCCESS đang có hiệu lực, kể cả
khi tổng kết run là `partial_failed`, `failed` hoặc còn ghi `running`. Ngày chưa SUCCESS
không được xuất. Bản gốc `run.json` được lưu một lần trong ZIP để giữ thông tin nguồn;
export không sửa bất kỳ manifest nào trong bucket nguồn.

Khi nhập, tổng kết run tại đích được tính từ những ngày của run đó đã nhập, giữ run ID
và `started_at`; phạm vi ngày, số ngày SUCCESS và `completed_at` phản ánh phần đã nhập.
Không chép trạng thái `running` của run nguồn sang đích. Ngày đã có SUCCESS ở một run
khác vẫn được bỏ qua. Có thể nhập thêm gói khác của cùng run và chạy lại sau gián đoạn;
nếu run đích có lịch sử không khớp với tổng kết do transfer tạo, script báo xung đột.

Thông báo cũ `Unsupported schema or unfinished manifest` ở run nhiều ngày là giới hạn
của script phiên bản đầu; cập nhật script trên cả hai máy rồi chạy lại. Schema chưa được
hỗ trợ hoặc day/page chưa hoàn tất vẫn bị từ chối. Đây là chuyển các bản SUCCESS đang
dùng, không phải backup toàn bộ bucket. Chưa chia ZIP nhiều phần hoặc tự truyền file qua mạng.

Nếu báo `Committed day has no Parquet`, manifest SUCCESS đang ghi số record > 0 nhưng
không tìm thấy file của đúng run/ngày đó. Lỗi này được phát hiện ngay cả trong dry-run.
Khôi phục file bị thiếu hoặc crawl lại đúng resource/ngày bằng `ingest repair --refresh`
trước khi export; script không chuyển ngày hỏng thành ngày rỗng. Ví dụ:

```powershell
python -m procurement.jobs.ingest repair --resource notify_contractor --start-date 2025-01-01 --end-date 2025-01-01 --refresh
```

Export/import thật dùng cùng khóa host với ingestion. `--lock-dir` phải giống các tiến
trình khác cùng endpoint/bucket. Mỗi bucket cần một máy ghi; khóa này không điều phối
nhiều host hoặc thư mục khóa khác nhau. Giữ nguồn ổn định trong lúc xuất, không sửa/xóa
object bằng công cụ khác đang bỏ qua khóa.

## Kiểm thử dành cho phát triển

```powershell
uv run --locked pytest tests/storage/test_transfer.py tests/tools/test_bronze_transfer.py
uv run --locked pytest tests/integration/test_transfer.py -m integration -k local

# Dùng SeaweedFS test riêng theo docs/development.md, tuyệt đối không dùng bucket vận hành
$env:TEST_S3_ENDPOINT = 'http://127.0.0.1:18333'
uv run --locked pytest tests/integration/test_transfer.py -m integration

# Test dung lượng thật: stream một member >4 GiB; dữ liệu lặp nên ZIP trên đĩa nhỏ
$env:RUN_ZIP64_LARGE_TEST = '1'
uv run --locked pytest tests/integration/test_transfer.py -m integration -k actual_member
```

Integration thông thường kiểm export/import với Parquet tạo bởi DLT thật giữa hai
bucket độc lập và ép ngưỡng ZIP64 thấp để kiểm toàn bộ luồng. Test dung lượng chuyên
biệt kiểm đọc/ghi SHA-256 của một member thực sự vượt 4 GiB, tách khỏi tải DLT lớn.
