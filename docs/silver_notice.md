# Silver notice — contract notice-2

Nhánh triển khai: `silver-notice`. Phạm vi là ba bảng Bronze
`notify_contractor_standard_detail`, `notify_contractor_vk_adb_detail`,
`notify_contractor_reoffer_detail`. Dùng `BaseResourceTransformer`, registry và
`TransformedRecord` theo guide Silver. Các resource khác chưa được thiết kế lại.

## CLI chính: resource trước, family là tùy chọn

CLI dùng [Typer](https://typer.tiangolo.com/tutorial/commands/), entrypoint `silver`.
Không cần truyền tên bảng Bronze:

```powershell
# Một ngày, tự chạy cả ba family
uv run --no-sync silver notice --date 2025-01-01

# Khoảng ngày bao gồm cả ngày đầu và cuối
uv run --no-sync silver notice --start-date 2025-01-01 --end-date 2025-01-31

# Alias tiếng Việt
uv run --no-sync silver tbmt --date 2025-01-01

# Chỉ chọn family khi cần kiểm riêng
uv run --no-sync silver notice --date 2025-01-02 --family reoffer

# Tiếp tục các ngày sau nếu một ngày lỗi; exit code cuối vẫn khác 0
uv run --no-sync silver notice --start-date 2025-01-01 --end-date 2025-01-31 --continue-on-error
```

`--no-sync` dùng môi trường đã cài dependency hiện tại. Máy mới cần cài project
và Typer từ lockfile trước (ví dụ `uv sync --frozen --extra dev`; giữ thêm các
extras metadata/ops/orchestration đang sử dụng). Có thể thay `uv run --no-sync silver`
bằng `.\.venv\Scripts\silver.exe` hoặc
`.\.venv\Scripts\python.exe -m procurement.cli.silver`.

Luồng: CLI kiểm tham số → service tra resource catalog → chọn commit Bronze cho
từng ngày → kiểm checksum/lineage/content hash/count → transform các family được
chọn → assemble/validate → cast typed schema → ghi candidate và report.
Family không có record trong ngày có count=0; cả ngày thiếu commit là lỗi,
không tự glob dữ liệu chưa commit. Ngay cả khi lọc một family, tổng input của
commit cả ngày vẫn được đối soát trước khi chấp nhận output.

Đầu ra mới không ghi đè Silver cũ:

```text
silver_candidates/muasamcong/notify_contractor/<run_id>/report.json
silver_candidates/muasamcong/notify_contractor/<run_id>/source_date=<ngày>/<bảng>.parquet
```

Report ghi từng ngày, input run/checksum, số dòng theo family, accepted/quarantine,
quality và đường dẫn output. Chỉ sử dụng ngày có status completed/needs_review
trong report; nếu ghi bị gián đoạn có thể tồn tại file chưa hoàn chỉnh của ngày
failed. Mỗi lần chạy có run_id mới, không overwrite run trước. `needs_review`
(quarantine/blocking issue) hoặc failed đều trả exit code 1; sai tham số trả 2.
Warning không blocking vẫn được ghi và không tự làm run thất bại.

Đây là **candidate theo ngày**, không phải global current_entity hay certified
Iceberg release. Source selection hiện dùng `_control` S3 manifests của luồng
Silver có sẵn; chưa có fallback đọc commit PostgreSQL-only. Nếu chỉ có metadata
PostgreSQL, cần hoàn thiện adapter selection trước; không tự dùng raw glob.

CLI chỉ nhận tham số/hiển thị; điều phối nằm trong `silver/service.py`, resource contract trong `silver/resources.py`. CLI cũ và generic mapping đã bị xóa. Dispatcher dùng registry cho mọi transformer; assembler/writer dùng danh mục `schemas.py`, không chứa nhánh đặc biệt cho notice. Project dùng transformer đã đăng ký; resource chưa implement báo lỗi rõ.

Kiểm thử sau cleanup: **96 unit tests passed** (57 Silver tests). Lint và format check đều đạt.
Test service sử dụng object storage trong RAM và Parquet thật: kiểm thứ tự ngày,
đủ ba family, lọc riêng family vẫn đối soát toàn input, Decimal output, lưu report,
run mới không ghi đè run cũ, thiếu commit và continue-on-error. Typer CliRunner
kiểm alias, option conflict, date validation và exit code. Ruff phần CLI mới,
processing Silver, warehouse và Silver tests đạt; entrypoint `silver list` đã chạy.
Chưa chạy lệnh notice này lên storage thật trong phiên triển khai CLI.

## Schema và grain

`notice_revision`: một nội dung phiên bản được quan sát của một notice. Giữ nguyên
các cột cũ và thêm các nhóm sau; DDL chính xác nằm tại
`src/procurement/processing/silver/schemas.py` và `silver/notice_schema.py`.

| Nhóm | Cột mới chính | Nguồn / quy tắc |
|---|---|---|
| Nguồn | source_table, source_family, source_root_path, source_document_id | standard: bidoNotifyContractorM hoặc bidNoContractorResponse.bidNotification; VK: bidoNotifyContractorP; reoffer: root |
| Gói/kế hoạch | package_id, plan_id/no/version/name/type_raw, project_name | bidId, planNo/name/type, projectName; plan_id/version từ embedded package chỉ khi package ID khớp |
| Tổ chức | investor_code, procuring_entity_code/name, investor_name | Giữ riêng hai vai trò; buyer_id/name là alias bên mời thầu |
| Thời gian | published_at, original_published_at, raw dates, plan_decision_date | publicDate/originalPublicDate giữ giờ; ISO không timezone dùng Asia/Ho_Chi_Minh theo parser của guide; timestamp_policy ghi rõ |
| Chào giá lại | reoffer_no/version/status_raw, reoffer_open_at/close_at, initial_price, price_step | reofferNo/version/statusReoffer, reofferOpenDate/CloseDate, priceInit/Step; không gán giá ban đầu thành giá gói |
| Giá/bảo đảm | estimate_price/currency, guarantee_amount/currency/form, reoffer_currency | standard guaranteeValue/Form; VK bidGuaranteeValue/Form. Không suy đoán tiền tệ từ giá gói |
| Hợp đồng | contract_type, contract_duration_raw/unit | standard contractType/Period/PeriodUnit; VK cType/cPeriod/cPeriodUnit |
| Điều kiện | bid_validity_raw/unit, is_internet/domestic/multi_lot/prequalification, is_medicine_raw, medical_type_raw | Giữ mã thô khi ngữ nghĩa chưa chuẩn hóa; boolean dùng parser, không dùng truthiness |
| Phạm vi/thủ tục | capital_detail, process_apply_raw, fee_type_raw/amount, issue/receive/open/execution_location, shortlist_no | Trường tương ứng tại root; không suy luận từ tên hoặc nội dung text |
| Hủy/tài liệu | cancelled_at, cancel_reason/type_raw, cancel_decision_no/date/agency, cancel_file_id/name, report_file_id, appraisal_file_id | Giữ cả thông tin hiện hành và raw history trong child |
| Bằng chứng | field_sources_json, root_json; observation.payload_json | Field sources ghi path đầy đủ; trường chưa typed vẫn có trong payload |

Các cột currency bổ sung chỉ điền khi có đơn vị trực tiếp tại nguồn tương ứng;
chưa có bằng chứng chung để kế thừa tiền tệ từ parent. Khi có giá mà không có
currency, giữ giá và phát cảnh báo ở root. Không công bố tổng tiền từ các cột này
cho đến khi có mapping currency được chứng nhận. `amount` là alias tương thích của
`bid_price`, không phải giá trúng hoặc giải ngân. `bid_open_date/close_date` giữ
ngày mở/đóng thông báo thông thường, không âm thầm thay bằng ngày chào giá lại.

Ba bảng con có kiểu mới:

| Bảng | Grain | Trường nghiệp vụ |
|---|---|---|
| notice_lot | Một phần tử lô thuộc revision và path nguồn được chọn | lot_no/name, lot_price, estimate_price, currency, guarantee_amount, contract_duration_raw/unit, notify_id/no |
| notice_location | Một phần tử địa bàn nguồn trong revision | province/district/ward code và name |
| notice_reoffer_submission | Một lần xuất hiện lượt chào trong một list nguồn của revision | source_list, submission_id, reoffer_id, lot_no, contractor/venture code/name, offered_at/raw, times_raw, price/final_price, currency, is_newest, form_code/value |

Mọi bảng con có `child_id`, `revision_id`, `source_id`, `source_path`, `ordinal`,
`raw_json`. Child ID định danh lần xuất hiện, không được coi là identity ổn định
xuyên revision. Cùng submission xuất hiện ở `contractorsResult` và
`contractorsResultAll` tạo hai occurrence có source_list khác nhau: chưa có view
dedup nên không SUM/COUNT cả hai list như các sự kiện độc lập.

Standard có mirror lô: ưu tiên list ở `bidNoContractorResponse.bidNotification`
nếu list đó tồn tại, kể cả list rỗng; nếu không mới dùng root chính. Chỉ list được
chọn tạo notice_lot. Cả hai list vẫn được bảo toàn trong child/raw để kiểm tra.
Các mảng khác (gia hạn, lịch sử hủy, version, shortlist, dữ liệu mới chưa biết)
được giữ đệ quy trong `child` với kind=notice_component và path chính xác;
chưa khẳng định đã chuẩn hóa hết các sự kiện hoặc parse JSON string trong form.

## Identity, quality và tích hợp

### Làm rõ reoffer bằng kiểm tra toàn tập Bronze

Kiểm lại toàn bộ 6.966 records reoffer trong bốn archive 2022–2025:
`bidForm=CGTTRG` và `notifyType=CGTTRG` ở cả 6.966 records;
`notifyNo=reofferNo` và `notifyId=id` ở cả 6.966 records. Detail payload không
có stepCode có giá trị. Extractor hiện dùng resolve_route với notify.toml:
rule family-reoffer match bidForm=CGTTRG; không chỉ dựa trên router.py cũ.

Trong phạm vi dữ liệu này, reoffer là family của thông báo chào giá trực tuyến
rút gọn, không phải một hồ sơ độc lập bắt buộc xuất hiện sau một standard notice.
Mọi family đều vào notice_revision; notice_reoffer_submission là bảng con 0..N
của revision reoffer, nối bằng revision_id. Không tồn tại bảng reoffer_revision
riêng trong thiết kế hiện tại. id_scheme=reoffer_no là lựa chọn namespace kỹ
thuật, không phải bằng chứng có hai thực thể nghiệp vụ riêng biệt khi cùng mã.

Hai danh sách contractorsResult/contractorsResultAll cùng không rỗng ở 6.498
records, cùng rỗng ở 351 records, cùng null ở 117 records. Vì vậy có thể có dòng
notice_revision family=reoffer mà không có dòng submission. Không được diễn giải
null/empty thành chắc chắn không có ai chào giá hoặc quy trình thất bại.
Trong contractorsResultAll có giá trị times=2,3,... (riêng times=2 có 9.323
occurrences), là bằng chứng nguồn ghi nhận nhiều lượt; không phải mọi reoffer
đều chỉ có một lượt, cũng không mặc định tất cả đều có lượt thứ hai.
Chưa dùng các occurrence này làm số lượt duy nhất trước dedup theo nhà thầu/lô/
submission và danh sách nguồn. Kết luận phân loại chỉ áp dụng tập archive và
rule hiện tại, không mở rộng thành khẳng định mọi quy trình chào giá của hệ thống.

- Standard/VK giữ id_scheme=notify_no. Reoffer dùng reoffer_no để tránh ghi đè
  entity thông báo thông thường khi cùng số; cả notifyNo và reofferNo vẫn giữ.
  Quan hệ liên kết reoffer với kết quả theo scheme khác cần rule riêng về sau.
- Hai mirror có ID/no/version/bidId mâu thuẫn, hoặc envelope identity/version
  khác root, được quarantine cùng raw evidence. Root thiếu ID cũng quarantine.
- Sai giá/ngày/boolean tùy chọn: typed null + quality_issue; vẫn giữ record và
  raw. Root null không bị nhầm với một field tùy chọn null.
- Các relationship đến package qua bidId và plan qua planNo giữ nguyên raw
  target; không tự fuzzy-join theo tên. Chưa chọn target revision từ planVersion.
- Build pipeline dispatch ba notice table qua cùng transformer với registry/CLI.
  Cache fingerprint và revision contract có `notice-2`; không tái sử dụng cache
  mapping generic cũ. Các resource khác chỉ chạy khi có transformer đã đăng ký; không còn generic fallback.
- Assembler lưu generic child để truy vết và typed child theo bảng đích riêng;
  không đẩy notice lot/submission nguyên dict vào schema lot/participation cũ.

## Giới hạn vận hành

Dùng lệnh resource ở đầu tài liệu. Các lệnh run/inspect/query theo bảng của bản cũ đã bị bỏ; không giữ module legacy.
Schema mới có 20 bảng. Với Iceberg đã tồn tại, cần schema evolution hoặc build vào namespace mới rồi đối soát trước khi chuyển release. CLI candidate không tự publish hoặc chỉnh schema production.

## Kiểm chứng

Unit tests kiểm cả ba family, raw preservation, số tiền độ chính xác cao, timezone,
zero/false, ID conflict, root thiếu, optional parse errors, chọn mirror, hai list
reoffer, metadata embedded package, build/registry parity và INSERT toàn bộ bảng
đầu ra vào DuckDB theo SCHEMAS. Service test dùng filesystem trong RAM, đọc lại Parquet
để kiểm Decimal/timestamp thật, SHA256, ingested_at và không ghi đè family.

Kiểm tra archive thật là kiểm tra bổ sung cục bộ; không phải chứng nhận toàn bộ
Bronze hoặc production Iceberg.

Kết quả ngày 17/10/2026:

- `pytest tests/unit`: **80 passed**, trong đó `tests/unit/silver`: **41 passed**.
- Ruff cho processing/silver, warehouse và tests/unit/silver: đạt.
  CLI cũ chứa cmd_query đã được xóa trong lần cleanup; `git diff --check`: đạt.
- Đọc archive exports/bronze-2022.zip đến bronze-2025.zip: mỗi family/năm có file,
  sort các path Parquet, chọn các vị trí 0, n//3, 2*n//3, n-1 và đọc tối đa 100
  rows đầu mỗi file. Tổng **40 file, 1.467 observations**; không có reoffer file
  năm 2022/2023. Đây là mẫu phân bố theo vị trí file, không phải mẫu ngẫu nhiên.
- **1.467 accepted, 0 quarantined, 0 blocking issues**; đối soát occurrence đạt.
  Có **1.648 cảnh báo unknown_currency** ở các giá trị thiếu đơn vị trực tiếp;
  không tự gán currency hoặc loại bỏ giá trị. Số cảnh báo có thể lớn hơn số record
  vì một record có nhiều trường giá.
- Sinh **4.901 notice_lot**, **1.532 notice_location**,
  **2.535 notice_reoffer_submission**, **12.691 child occurrences**.
  Số lượt chào là occurrences của hai list, chưa phải số lượt duy nhất.
- INSERT mọi bảng đầu ra không rỗng vào DuckDB trong RAM theo SCHEMAS đạt;
  không chạy ghi lên S3/Iceberg, không đổi release hiện tại.

| Năm | Standard | VK/ADB | Reoffer |
|---|---:|---:|---:|
| 2022 | 303 | 25 | Không có file |
| 2023 | 235 | 32 | Không có file |
| 2024 | 269 | 52 | 13 |
| 2025 | 356 | 82 | 100 |

Những phần còn giới hạn: mapping currency cho giá bảo đảm/lô/chào lại; chuẩn hóa
history events và nội dung JSON form; dedup xuyên list của lượt chào; lựa chọn
revision và liên kết reoffer→result để làm Gold. Dữ liệu tương ứng vẫn được giữ,
không loại bỏ vì chưa có quy tắc phân tích. Schema/transformer notice đã có kiểm
chứng cục bộ; chưa đồng nghĩa toàn bộ Silver hoặc production đã được chứng nhận.
