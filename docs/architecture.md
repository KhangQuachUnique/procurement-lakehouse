# Kiến trúc và hợp đồng dữ liệu

[README](../README.md) · [Vận hành](operations.md) · [Phát triển](development.md)

**Đã triển khai:** ingestion Bronze, manifests, Ops, quality audit/repair, profile,
query và transfer. **Silver/Gold bên dưới là thiết kế mục tiêu, chưa có jobs hoặc
bảng được triển khai.** Code và config là nguồn chuẩn cho hành vi hiện tại;
báo cáo từng lần khảo sát nằm ngoài docs.

## Bronze hiện tại

Python gọi API MuaSamCong, DLT ghi Parquet vào object storage tương thích S3.
Catalog dùng chung cho jobs/Ops tại [catalog.py](../src/procurement/common/catalog.py):

| Resource | Bảng Bronze | Root nghiệp vụ / identity |
| --- | --- | --- |
| `project` | `project_detail` | `projectDTO.id`; version `projectDTO.version` |
| `khlcnt` | `khlcnt_plan_detail` | `bidPoBidpPlanProjectDetailView.id`; plan version được đối chiếu với search |
| `khlcnt` | `khlcnt_bid_package_detail` | Root `id`; package version có thể null |
| `notify_contractor` | `notify_contractor_standard_detail` | `bidoNotifyContractorM` hoặc `bidNoContractorResponse.bidNotification` |
| `notify_contractor` | `notify_contractor_reoffer_detail` | Root payload |
| `notify_contractor` | `notify_contractor_vk_adb_detail` | `bidoNotifyContractorP` |
| `contractor_result` | `contractor_result_detail` | `bideContractorInputResultDTO.id`; envelope source_id thường là notifyNo, không phải result UUID |

Envelope giữ `source_id`, `source_version`, `source_date`, `run_id`, `ingested_at`,
`content_hash`, `payload`; không sửa raw để làm đủ trường. `source_id` có nghĩa
theo resource, không phải khóa dùng chung cho mọi bảng.

| Đường dẫn trong bucket | Nội dung |
| --- | --- |
| `bronze/muasamcong/<table>/source_date=<date>/run_id=<id>/*.parquet` | Records của attempt |
| `_control/<source>/<resource>/run_id=<id>/run.json` | Tổng kết run |
| Cùng run, `source_date=<date>/day.json` và `pages/page-*.json` | Commit ngày và tiến độ trang |
| `_errors/<source>/<resource>/run_id=<id>/source_date=<date>/page-*.jsonl` | Errors |
| `_ops/<source>/<resource>/run_id=<id>/execution.json` | Heartbeat/liveness |
| `_quality/<source>/<resource>/run_id=<id>/source_date=<date>/` | Evidence validation/routing |

DayManifest SUCCESS là commit marker; effective là SUCCESS có `started_at` mới
nhất. Run FAILED/PARTIAL_FAILED vẫn có thể chứa ngày SUCCESS. Refresh lỗi không
thay bản SUCCESS trước; FAILED có thể để lại Parquet. SUCCESS 0 không cần file.
Recovery tạo attempt mới và giữ lịch sử; không lấy Ops SQLite làm nguồn chân lý.

[Committed reader](../src/procurement/storage/committed.py) pin ngày/run/file,
kiểm count và lineage; bật `verify_hash=True` để kiểm payload. Phải đọc hết
iterator trước khi công bố output. Host lock không điều phối nhiều máy ghi.

## Thông báo và validation

| Family | API suffix | Identity tại root đã chọn | Khác biệt cần giữ |
| --- | --- | --- | --- |
| standard | `lcnt_tbmt_ttc_ldt` | `id`, `notifyNo`, `notifyVersion` | Hai root có thể phản chiếu nhau; kiểm mâu thuẫn |
| reoffer | `online-reoffer/detail` | `id`, `notifyNo/reofferNo`, `notifyVersion/reofferVersion` | Chào giá trực tuyến; `priceInit`, `priceStep`, `reofferCloseDate/OpenDate` |
| vk_adb | `lcnt_tbmt_ttc_vk_adb` | `id`, `notifyNo`, `notifyVersion` | Root `bidoNotifyContractorP`, biểu mẫu riêng |

[notify.toml](../src/procurement/quality/notify.toml) chọn route bằng tổ hợp chính
xác `stepCode`, `processApply`, `bidForm`, `isInternet`, `bidMode`; không suy chỉ
từ online/offline hoặc tên family. Không có đúng một rule khớp thì báo routing
lỗi. Ingestion gọi API với search `id`, rồi [validator](../src/procurement/quality/contracts.py)
kiểm từng response:

- Payload/root rỗng, thiếu `id` hoặc số thông báo, identity không khớp search,
  root/alias mâu thuẫn hoặc error envelope → fail.
- Version thiếu có thể lấy từ search nếu contract cho phép; không lấy search
  để cứu ID/số thông báo thiếu trong detail.
- Thiếu context cần đối chiếu → unresolved. Fail/unresolved không nhận vào Bronze.
- Null tùy chọn, list rỗng, 0/false không tự là lỗi. Tỷ lệ rỗng ≥80% trên ≥10
  fields chỉ cảnh báo; audit còn so size/cụm rỗng theo contract/workflow.

HTTP 200 chưa chứng minh detail hợp lệ. Validator hiện chưa bắt buộc `bidId`,
`bidName` hoặc `publicDate`; profile theo năm giúp review thêm rule, không tự đổi
contract. Xem [quality và profiling](operations.md#kiểm-tra-và-sửa-chất-lượng-thông-báo).

## Silver đề xuất

Luồng: **Bronze đã verify → observations/lineage → typed revisions → relationships
và children → latest observed → Gold**. Dùng Python + DuckDB batch và Parquet bất
biến, một publisher; cân nhắc table format/catalog khác khi cần nhiều writer.

### Identity, lịch sử và quan hệ

| Nhóm | Hợp đồng mục tiêu |
| --- | --- |
| `source_observation` | Một occurrence vật lý: source/table/run/ngày/file checksum/row ordinal toàn file, raw pointer và hash |
| `entity` | Namespace + entity type + ID scheme + source ID; hash canonical tuple, giữ các thành phần gốc |
| `entity_revision` | Entity + source version token + semantic hash + mapping version; hash gồm children nghiệp vụ |
| `revision_observation` | Giữ mọi occurrences đóng góp cho revision đã dedup |
| Typed entities | `project_revision`, `procurement_plan_revision`, `bid_package_revision`, `tender_notice_revision`, `selection_result_revision` |
| Children | Lots, participants, award suppliers, items, locations, documents, amount components; gắn parent revision |
| Quan hệ | Reference gốc, scheme/version/path, candidate và trạng thái resolved/unresolved/ambiguous/invalid |
| Quality | Issue, quarantine, coverage và capability theo release; không âm thầm bỏ record |

Project/plan/package dựa trên detail UUID có namespace; notice dùng số thông báo
và giữ document UUID ở revision; result dùng DTO.id. Package không lấy
`planVersion` làm version của chính nó. Không gộp tổ chức theo tên, không đồng
nhất mã cổng với mã thuế khi chưa xác minh.

Cùng identity/version nhưng khác nội dung phải giữ revisions và báo conflict.
`*_current` có nghĩa **latest observed**, chọn từ input membership của release;
timestamp bằng nhau nhưng nội dung khác thì không tự chọn. Không dùng max version
lexical hoặc max source_date. Repair copy giữ observed_at cũ; mapping đổi không
được giả thành sự kiện sửa hồ sơ. Mất record khỏi refresh không chứng minh bị hủy.

`source_date` là cửa sổ search, `ingested_at` là thời điểm quan sát, ngày trong
payload là thời gian nghiệp vụ. Crawl lại dữ liệu cũ không tái tạo được trạng
thái thật trong quá khứ. Initial Silver chỉ dùng effective selection; lịch sử
attempt cũ cần verify riêng trước khi bổ sung.

Join ưu tiên UUID đúng namespace, rồi business number/version có uniqueness đã
chứng minh. Không join bắt buộc bằng cùng source_date hoặc fuzzy tên; giữ gaps
bằng LEFT JOIN/unresolved. Notice và result có search filters khác nhau, nên
thiếu notice chưa chứng minh result lỗi. Embedded context có provenance riêng,
không âm thầm thay detail authoritative.

### Mapping trọng tâm

Các path dưới đây đã được ghi nhận khi khảo sát; phải kiểm trên phạm vi triển
khai trước khi freeze required/type/codelist. `N` là root notice theo family,
`P = bidPoBidpPlanProjectDetailView`, `R = bideContractorInputResultDTO`.

| Thực thể | Path cần giữ |
| --- | --- |
| Project | `projectDTO.id/no/version/name`, `investorCode/Name`, `investTotal/Unit`, `publicDate`, `decisionNo/Date`; địa điểm root `provCode/districtCode/plocation` |
| Plan | `P.id/planNo/planVersion/name`, `pid/pno`, `investorName`, `investTotal/Unit`, `bidPack`, `publicDate`; `bidpPlanDetailToProjectList[].id` liên kết package |
| Package | Root `id/bidNo/bidName/planId/planNo/planVersion`, `bidPrice/Unit`, `bidEstimatePrice`, `bidField/bidForm/bidMode`, `ctype/cperiod/cperiodUnit` |
| Notice chung | `N.id/notifyNo/notifyVersion`, `bidId/bidNo/bidName`, `planNo/Name`, `investorCode/Name`, `procuringEntityCode/Name`, `publicDate`, `status`, `investField` |
| Notice biến thể | Standard/VK: `bidCloseDate/bidOpenDate/bidPrice/bidPriceUnit`; reoffer: `reofferCloseDate/reofferOpenDate/priceInit/priceStep`; không gộp giá khởi điểm vào budget |
| Result | `R.id/resultVersion`, `notifyId/notifyNo/notifyVersion`, `bidId/bidNo/bidName`, `publicDate`, `decisionNo/Date`, `status/type`, `bidPrice/Unit`, `bidField` |
| Result lots | `R.lotResultDTO[]`: `id/lotNo/lotName/lotPrice/lotEstimatePrice/lotPriceUnit/winningCode/status` |
| Participants | Trong lot, `contractorList[]`: `id/orgCode/orgFullname/taxCode/taxNation/ventureCode/ventureName/role/bidResult/bidWiningPrice` (đúng spelling nguồn) |
| Items | `R.lotResultDTO[].goodsList`, `R.lotResultItems[].formValue`: JSON trong string, parse theo formCode/schema |

Package có `bidLocation[]`, `bidpBidLotList[]`; standard có vị trí ở
`bidpBidLocationList[]`, lots ở nested `bidNotification.lotDTOList[]`; VK có
`lsBidpBidLocationDTO[]`, `N.lotDTOList[]`; reoffer có `bidDetail.bidLocation[]`.
Identity-root fallback không mặc nhiên thay thế được mọi child path.

Giữ riêng các list reoffer (`bidoListContractorReofferPassedDTOList`,
`contractorsResult`, `contractorsResultAll`), mở thầu và đánh giá; không union rồi
coi tất cả là người thắng. Không đếm embedded `resultDTO` của package thêm một
lần cùng result endpoint. Child ID phải duy nhất trong parent/list role; thiếu
ID dùng content hash + occurrence và ghi rõ không ổn định xuyên revision.

Award chỉ tạo khi outcome, consortium role và phạm vi giá đã xác minh; có
`bidWiningPrice` chưa đủ chứng minh thắng. Không nhân tổng award theo thành viên
liên danh hoặc tự chia đều. Items cần phân biệt leaf/group, goodsList/formValue,
bản dịch và lịch sử quyết định; chưa map được phải công bố thiếu capability.

### Chuẩn hóa và chất lượng

IDs/version là text, giữ số 0 đầu. Text chuẩn hóa có kiểm soát, không bỏ dấu hoặc
tự sửa tên. Boolean parse theo contract, không coi string `"0"` là true. Codelist
giữ raw code/domain/mapping version/evidence; unknown không được bịa nhãn.

Money đề xuất DECIMAL(38,6), quantity DECIMAL(38,9), kiểm scale/overflow trước
khi chốt. Parse decimal từ JSON gốc; không qua float. Tách budget, estimate,
initial reoffer price, award value và unit price. Currency thiếu giữ unknown,
không tự mặc định VND hoặc dùng tỷ giá hiện tại cho lịch sử. Ngày chỉ có year/
quarter/month giữ precision; naive timestamp chưa tự gắn timezone. Lưu raw,
local time, UTC khi có căn cứ và `timezone_basis`.

Missing file/count/hash/lineage hoặc output PK/FK sai chặn release. Thiếu root/
identity đưa record vào quarantine; trường optional parse lỗi giữ null + issue.
Unknown code/reference và nhiều null được báo rõ theo rule. Field mới giữ raw
và profile, không tự fail; schema/mapping đổi phải version và replay.

Đối soát: `input occurrences = accepted + quarantined`,
`child candidates = accepted children + quarantined children`,
`references = resolved + unresolved + ambiguous + invalid`.
Dedup không được làm mất lineage. Child lỗi phải đánh dấu parent chưa đủ children.
Ngày missing khác SUCCESS rỗng; đạt core không đồng nghĩa đạt participants/awards/items.

### Release và incremental

Pin exact input files, manifest/hash, config và code/mapping version. Rebuild toàn
resource/ngày dirty, gồm cả ba bảng notice khi route đổi; lấy hợp entity keys cũ
và mới để tính lại current và quan hệ. Không chỉ dùng watermark ngày mới nhất.

Ghi build bất biến → validate input/output/reconciliation → ghi và đọc lại manifest
release → kiểm lại baseline và parent release → promote channel pointer → readback.
Baseline đổi thì không promote; ACK không rõ giữ uncertain để đối soát. Không giả
định S3 có rename thư mục nguyên tử hoặc transaction đa bảng. Consumer pin một
release và exact file list; rollback trỏ về release đã verify, giữ file cũ.
Chưa tự bật GC; chỉ một publisher đến khi có cơ chế nhiều writer được kiểm chứng.

## Gold và khai thác đề xuất

| Người dùng | Câu hỏi chính | Nhóm dữ liệu |
| --- | --- | --- |
| Cơ quan quản lý | Quy mô, cơ cấu, cạnh tranh, chênh lệch giá, coverage | Package, results, competition, award comparison |
| Chủ đầu tư/bên mời thầu | Danh mục, lịch đóng thầu, tiến trình, đối sánh giá | Project/plan/package, notice, lifecycle, items |
| Nhà thầu | Cơ hội còn mở, phân khúc, lịch sử tham gia/thắng | Notice, participation, award, organization |
| Nghiên cứu | Dữ liệu tái lập, lịch sử, độ phủ và lý do loại mẫu | Pinned release, revisions, coverage, lineage |

Mỗi fact có release ID, grain/key rõ, revision lineage, eligibility và lý do loại.
Dims dùng chung: date/period, organization theo vai trò, geography theo phiên bản,
project/plan/package, selection scope, sector, procurement/submission method,
context/status/contract type, currency, bidder, item/unit và data release. Giữ
unknown/unresolved/not-applicable riêng; không đếm chúng thành thực thể thật.
SCD2 quan sát không được gọi là lịch sử hiệu lực pháp lý.

| Fact mục tiêu | Một dòng trong một release |
| --- | --- |
| `fact_project_portfolio`, `fact_plan_publication`, `fact_package_plan` | Một entity tại revision được chọn; measures tiền đúng cấp |
| `fact_tender_notice`, `fact_selection_result` | Một notice/result; số notice không bằng số package |
| `fact_bid_participation` | Một bidder trong một selection scope; liên danh là một bidder khi có căn cứ |
| `fact_competition_summary` | Một scope với distinct bidders và trạng thái đầy đủ danh sách |
| `fact_award` | Một phần trao thầu độc lập, xác minh outcome và price scope |
| `fact_award_comparison` | Một scope không chồng lấn + cơ sở giá; aggregate awards trước khi so sánh |
| `fact_award_item` | Một item leaf đã xác minh; không gồm dòng tổng hoặc bản dịch |
| `fact_selection_lifecycle` | Một lần lựa chọn, mốc đầu tiên và hoàn tất phân biệt |
| `fact_procurement_event` | Một sự kiện có bằng chứng, không suy từ mọi lần hash đổi |
| `fact_data_coverage` | Release + resource + search date + capability; không cộng trùng ngày qua capabilities |

`selection_episode` là một lần tổ chức lựa chọn có bằng chứng, không mặc định
bằng package. `selection_scope` là episode + cấp package/lot; result chưa nối
notice có scope cục bộ, không tạo notice giả. Bridges xử lý bidder–member,
award–recipient/supplier, scope–location, comparison–award và entity relationships.
Lọc qua bridge không được nhân số tiền; chỉ phân bổ khi có evidence/share hợp lệ.

| Chỉ số | Quy tắc tối thiểu |
| --- | --- |
| Quy mô/cơ cấu | Distinct đúng entity trong một release; số gói khác số notice/result; giữ nhóm unknown và coverage |
| Giá trị trúng | SUM award độc lập đủ điều kiện, cùng currency; không phải tiền thanh toán |
| Tiết kiệm | `SUM(B - A) / SUM(B)` trên cùng tập scope đủ điều kiện, không AVG tỷ lệ từng gói; mẫu số 0 → null |
| So sánh giá | Cùng scope, currency, thuế, reference revision/basis, tập awards đủ và không chồng package/lot; phần chưa rõ bị loại có lý do |
| Một bên dự thầu | Chỉ tính khi danh sách đầy đủ và bidder identity/consortium đã resolve; missing list không phải 0 |
| Tỷ lệ thắng | Bidder-scope thắng / bidder-scope có outcome cuối đã biết; kèm coverage outcome; dữ liệu chỉ có người thắng chưa đủ |
| CR4/HHI | Thị trường/cohort rõ, phần giá trị độc quyền và identity đủ; không cấp toàn award cho từng thành viên liên danh |
| Đơn giá | So cùng sản phẩm/quy cách, đơn vị, currency và basis; không SUM đơn giá hoặc quantity khác đơn vị |
| Thời lượng | Mốc cùng episode, cùng precision/timezone, phân biệt kết quả đầu và hoàn tất; không dùng ngày ingest |
| Tăng trưởng | Hai kỳ cùng coverage/basis/currency; chênh lệch phạm vi thu thập không phải tăng trưởng thị trường |

Tiết kiệm âm giữ nguyên nếu đầu vào hợp lệ. Tỷ lệ, median và distinct phải tính
lại trên tập chung, không cộng các nhóm. Không cộng snapshots qua thời gian;
không cộng vốn project + plan + package. Chỉ báo tập trung/đồng dự không tự là
kết luận vi phạm. Dữ liệu hiện tại chưa đủ để cam kết hợp đồng đã ký, giải ngân,
tiến độ thực hiện hoặc chất lượng nhà thầu; cần nguồn bổ sung.

## Trình tự triển khai tiếp

1. Profile đầy đủ phạm vi pilot; chốt identity, aliases, codelists, timezone,
   money/currency và child schemas. Bằng chứng mẫu không thay full validation.
2. Xây input selection, lineage, release/readback/rollback và core revisions/current.
3. Bổ sung relationships, organizations, lots và participants; chỉ mở awards/items
   sau khi xác minh outcome/role/price scope/form schemas.
4. Xây Gold theo metric đủ điều kiện; benchmark full và incremental, công bố
   coverage/exclusions/release ID và khả năng truy về raw.

Nghiệm thu phải bao gồm: HTTP 200 nhưng root null, identity/alias conflict, cùng
version khác nội dung, repair chuyển table, ngày cũ đổi/mất record, target đến
muộn, child reorder/duplicates, liên danh/nhiều lô, tiền/timezone mơ hồ, crash và
ACK uncertain. Full build và incremental cùng selection phải cho cùng semantic
rows. Không công bố certified khi còn integrity, PK/FK hoặc reconciliation sai.
