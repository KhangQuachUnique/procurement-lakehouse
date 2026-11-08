# Resource kết quả thực hiện hợp đồng

Thiết kế triển khai ngày 03/11/2026, mapping `contract-performance-1`.
Quyết định và lý do: [ADR-001](decisions/0001-contract-performance.md).
Thiết kế này thay thế phạm vi bảy bảng/191 trường trong khảo sát `silver-schema-v1` trước đây.
Chỉ có **hai bảng nghiệp vụ Silver**; observation, quality_issue và quarantine là bảng quản trị dùng theo kiến trúc chung.

## Dữ liệu này trả lời được gì?

Phục vụ chính nhóm 4: trong **các báo cáo kết quả đã được công bố**, hợp đồng nào hoàn thành muộn/sớm so với kế hoạch, chênh bao nhiêu ngày, chất lượng được bên đăng đánh giá ra sao, khác nhau thế nào theo chủ đầu tư, nhà thầu và quy mô hợp đồng.

Giá hợp đồng sau thuế chỉ bổ trợ phân nhóm quy mô. Đây không phải giá thực chi, tiền đã thanh toán hoặc bằng chứng tiết kiệm ngân sách. Nguồn này cũng không đại diện đầy đủ cho mọi hợp đồng đang thực hiện. Không có báo cáo không đồng nghĩa hợp đồng chưa hoàn thành hay vi phạm.

Đợt backfill chọn **ngày công bố kết quả trong năm 2025**, không chọn năm ký hay năm hoàn thành hợp đồng. Hợp đồng ký trước 2025 vẫn được lấy nếu kết quả năm 2025 tham chiếu đến nó. Báo cáo công bố 2026 nằm ngoài đợt này, dù hợp đồng hoàn thành 2025.

## Luồng API đã xác minh

Tất cả là POST có ngữ nghĩa đọc, cùng origin cấu hình `MUASAMCONG_BASE_URL` (`https://muasamcong.mof.gov.vn` trong môi trường khảo sát).

| Bước | Đường dẫn | Request chính | Kết quả và kiểm tra |
|---|---|---|---|
| Search kết quả | `/o/egp-portal-database/services/result-perform-contract/search-view-po` | `publicDate.greaterThan/lessThan`, `pageNumber`, `pageSize`; `contractSignDate` không lọc | `page.content[]`; request trang 0, response `currentPage` từ 1; adapter chuẩn hóa về 0 |
| Danh sách liên kết | `/o/egp-portal-contractor-selection-v2/services/get-linked-list-contract-po` | `{"contractCode":"..."}` | Chọn `resultList[]` khớp **id + version**; kiểm tra `contractCode + contractVersion` |
| Tra hợp đồng | `/o/egp-portal-database/services/smart/search` | `index=es-contractor-selection`, `type=es-ct-contract`, exact `contractCode`, pageSize 1 | Phải có đúng một hit cùng mã; zero hit là liên kết chưa giải được |
| Chi tiết hợp đồng | `/o/egp-portal-contractor-selection-v2/services/econsign/contract-info/detail-by-id-for-po` | `{"id":"..."}` | `contract` và `versionsList`; đối chiếu ID, mã và phiên bản |

Nếu chi tiết ban đầu là phiên bản khác, tìm phiên bản được báo cáo tham chiếu trong `versionsList` rồi lấy đúng ID đó. Giữ cả phản hồi ban đầu đã phải tải; không quét thêm các phiên bản không liên quan. Không mặc định lấy `resultList[0]`, không thay phiên bản thiếu bằng bản mới nhất.

Mẫu search:

```json
{
  "contractorName": {"contains": null},
  "contractNo": {"contains": null},
  "contractCode": {"contains": null},
  "bidName": {"contains": null},
  "taxCode": {"contains": null},
  "investorName": {"contains": null},
  "publicDate": {
    "greaterThan": "2025-10-17T00:00:00.000Z",
    "lessThan": "2025-10-17T23:59:59.999Z"
  },
  "contractSignDate": {"greaterThan": null, "lessThan": null},
  "pageSize": 50,
  "pageNumber": 0
}
```

Các hậu tố Z trên request không đủ chứng minh timezone của mọi trường nguồn. Pipeline giữ ngày lịch của `publicDate`, không tự dịch giờ. Gate pilot đối chiếu toàn bộ ID–version và ngày công bố của truy vấn tuần với bảy ngày trước khi cho backfill tiếp.

## Bronze và tính đầy đủ

Một resource catalog: `contract_performance`, gồm:

- `contract_performance_detail`: một bản ghi cho kết quả ID–version được chọn. Payload gồm `performance` (kết quả), `search` (dòng tìm kiếm), `linked` (phản hồi liên kết đầy đủ), `resolution.status`.
- `contract_detail`: phản hồi `{contract, versionsList}` đầy đủ. Trong cùng ngày, khử trùng theo ID, phiên bản và hash nội dung `contract`.

Envelope chung: `source_id`, `source_version`, `run_id`, `source_date`, `ingested_at`, `content_hash`, `payload`. Với cả hai bảng, `source_date` là ngày kết quả đang được thu thập; ngày công bố/ký của hợp đồng nằm trong payload.

Cache chỉ tồn tại trong một attempt ngày, theo mã hợp đồng; các phiên bản đã tải được dùng lại trong attempt. Không dùng cache cũ để bỏ qua cập nhật nguồn khi refresh. Tất cả trường chưa đưa vào Silver vẫn có trong payload; dữ liệu nhạy cảm và quyền truy cập Bronze áp dụng theo cấu hình hệ thống. Token/cookie/header xác thực không nằm trong payload.

Kiểm tra pagination trong IngestionService: tổng không đổi, đúng số dòng mỗi trang, chỉ số trang/page size đúng, không lặp ID–version. Tổng từ 10.000 trở lên làm ngày thất bại; chưa có chiến lược chia nhỏ được xác minh nên không tự đoán filter thay thế.

Hai bảng của resource mới được nạp cùng một DLT load khi flush; pipeline riêng từng attempt không khôi phục state từ destination. PostgreSQL commit là điểm quyết định resume, không phải state của DLT.

Ngày chỉ commit sau khi trích xuất xong và kiểm tra file Parquet. Kiểm tra file chỉ liệt kê đúng các prefix bảng/ngày/attempt đã ghi, không quét wildcard toàn bộ dataset. Ngày rỗng là commit hợp lệ với 0 file. Ngày lỗi không commit dở; nếu đang refresh, commit tốt trước đó vẫn giữ nguyên. Khôi phục chạy từ đầu ngày lỗi, không hứa resume chính xác từ trang giữa chừng. Có heartbeat trong quá trình xử lý ngày dài.

`verification` của commit ghi riêng số search, số bản ghi mỗi họ, số mã hợp đồng, số cảnh báo nối và số trang. Không giả định mỗi kết quả luôn tạo đúng hai bản ghi: có hợp đồng dùng chung hoặc nhiều phiên bản đã phải tải.

## Hai bảng Silver

### `contract_revision`

Một dòng là một revision nội dung của một phiên bản hợp đồng. Entity được nhận diện bằng `contractCode`, ID kỹ thuật nguồn giữ riêng.

| Cột | Đường dẫn trong payload | Ý nghĩa |
|---|---|---|
| `source_contract_id` | `contract.id` | ID kỹ thuật để gọi chi tiết đúng bản hợp đồng |
| `contract_code` | `contract.contractCode` | Mã hợp đồng nghiệp vụ; dùng nối với kết quả |
| `contract_no` | `contract.contractNo` | Số hiệu hợp đồng do đơn vị khai báo; khác mã hệ thống |
| `contract_version` | `contract.contractVersion` | Phiên bản hợp đồng, giữ chuỗi như `00` |
| `contract_name` | `contract.contractName` | Tên hợp đồng; không tự lấy tên gói để lấp chỗ trống |
| `package_name` | `contract.bidName` | Tên gói thầu được nguồn khai báo |
| `investor_code` | `contract.investorCode` | Mã chủ đầu tư trong nguồn |
| `investor_name` | `contract.investorName` | Tên chủ đầu tư tại bản hợp đồng này |
| `contractor_code` | `contract.contractorCode` | Mã nhà thầu trong nguồn; không mặc định là mã số thuế |
| `contractor_name` | `contract.contractorName` | Tên nhà thầu tại bản hợp đồng này |
| `signed_date` | `contract.contractSignDate` | Ngày ký hợp đồng |
| `effective_date` | `contract.contractEffectiveDate` | Ngày hợp đồng có hiệu lực |
| `public_date` | `contract.publicDate` | Ngày công bố hợp đồng, có thể khác năm báo cáo |
| `published_at_raw` | `contract.publicDate` | Giá trị ngày giờ nguồn nguyên bản để đối chiếu |
| `price_after_tax` | `contract.priceAfter` | Giá hợp đồng sau thuế; Decimal(38,6), không phải thực chi |
| `currency` | `contract.priceAfterUnit` | Đồng tiền của giá trên; thiếu thì để null, không tự điền VND |
| `notice_source_id` | `contract.notifyId` | ID TBMT do nguồn cung cấp, ứng viên nối với Silver thông báo |
| `notice_no` | `contract.notifyNo` | Mã TBMT, ứng viên nối `notice_revision.notify_no` |
| `notice_version` | `contract.notifyVersion` | Phiên bản TBMT cần đi cùng mã |
| `result_source_id` | `contract.resultId` | ID KQLCNT, ứng viên nối `result_revision.result_id` |
| `result_version` | `contract.resultVersion` | Phiên bản KQLCNT cần đi cùng ID |

Các khóa tới TBMT/KQLCNT được giữ theo nguồn; chưa coi là khóa ngoại toàn vẹn cho đến khi đối chiếu được bản ghi Silver tương ứng. Không thêm bảng tổ chức/nhà thầu trong resource này.

### `contract_performance_revision`

Một dòng là một revision nội dung của một phiên bản báo cáo kết quả. Entity theo `performance.id`, không theo mã hợp đồng: một hợp đồng có thể có nhiều báo cáo.

| Cột | Đường dẫn trong payload | Ý nghĩa |
|---|---|---|
| `source_evaluation_id` | `performance.id` | ID kỹ thuật của báo cáo |
| `evaluation_no` | `performance.idNo` | Số hiệu báo cáo, ví dụ mã bắt đầu bằng CS |
| `evaluation_version` | `performance.version` | Phiên bản báo cáo |
| `contract_code` | `performance.contractCode` | Mã hợp đồng được báo cáo nhắc đến |
| `contract_version` | `performance.contractVersion` | Đúng phiên bản hợp đồng được tham chiếu |
| `investor_code` | `performance.investorCode` | Mã chủ đầu tư trong báo cáo |
| `investor_name` | `performance.investorName` | Tên chủ đầu tư trong báo cáo |
| `contractor_code` | `performance.contractorCode` | Mã nhà thầu trong báo cáo |
| `contractor_name` | `performance.contractorName` | Tên nhà thầu trong báo cáo |
| `public_date` | `performance.publicDate` | Ngày công bố kết quả; cơ sở chọn backfill 2025 |
| `published_at_raw` | `performance.publicDate` | Giá trị thời gian công bố nguyên bản |
| `planned_completion_date` | `performance.completePlanDate` | Ngày hoàn thành theo kế hoạch được khai báo |
| `actual_completion_date` | `performance.completeRealityDate` | Ngày hoàn thành thực tế được khai báo |
| `adjusted_completion_date` | `performance.completeAdjustedDate` | Mốc điều chỉnh nếu được cung cấp; các mẫu khảo sát đều null |
| `progress_status_code` | `performance.contractStatus` | Mã đánh giá tiến độ của bên công bố; giữ riêng với số ngày tính từ lịch |
| `is_completed` | `performance.isContractComplete` | 1 → true, 0 → false; giá trị lạ → null kèm cảnh báo |
| `quality_status_code` | `performance.contractQualityPer` | Mã đánh giá chất lượng nguồn; chưa tự gán nhãn suy đoán |

Không đưa `diffPlanReality` vào cột số ngày chậm: mẫu hoàn thành sớm cũng có giá trị dương. Không đưa `completedRate`, `completedValue`, dòng hàng, biểu mẫu, tài liệu đính kèm hoặc nội dung vi phạm vào các cột phân tích đợt này.

### Cột kỹ thuật có ở cả hai bảng

| Cột | Ý nghĩa |
|---|---|
| `revision_id` | Hash nội dung root + entity + phiên bản nguồn + hợp đồng mapping; khóa chính |
| `entity_id` | Hash namespace, loại entity, cách định danh và khóa nghiệp vụ |
| `entity_type` | `contract` hoặc `contract_performance` |
| `observation_id` | Trỏ về lần quan sát Bronze đã tạo revision |
| `is_current` | Cờ lựa chọn trong lần assemble ngày; **không dùng riêng cờ này để kết luận mới nhất toàn bộ dữ liệu** |
| `source_date` | Ngày phân vùng kết quả; dùng partition và thay thế dữ liệu khi refresh |
| `observed_at` | Thời điểm hệ thống thực sự lấy dữ liệu, có timezone |
| `root_json` | Root hợp đồng/kết quả đầy đủ, giúp đối chiếu và bổ sung mapping sau này |

Đổi dòng search, danh sách phiên bản hoặc báo cáo khác trong `linked` không tạo revision giả. Đổi nội dung root vẫn tạo revision mới, kể cả khi nguồn chưa đổi số phiên bản.

Parse lỗi ngày/tiền/boolean → null và quality issue, giữ nguyên giá trị trong root_json. Giữ nguyên mã trạng thái chưa biết, thêm cảnh báo tiến độ nếu ngoài tập 01/02/03. Thiếu root/identity → quarantine và chặn xuất bản resource mới. Thiếu hợp đồng/phiên bản liên kết → cảnh báo nhưng vẫn giữ báo cáo hợp lệ.

Nối phân tích qua `contract_code + contract_version`, dùng `v_contract_version` để chọn nội dung quan sát mới nhất của đúng phiên bản. `v_contract_performance_version` làm tương tự cho báo cáo. `v_latest_contract` và `v_latest_contract_performance` chọn phiên bản số lớn nhất **trong dữ liệu đã quan sát**, rồi thời điểm quan sát mới nhất. Các view không chứng minh đã có phiên bản mới nhất của toàn bộ nguồn.

Gold sau này có thể tính `actual_completion_date - planned_completion_date`: dương là hoàn thành sau mốc kế hoạch. Nếu muốn so với mốc điều chỉnh phải tính chỉ tiêu riêng khi trường đó có dữ liệu. Chưa tạo các chỉ tiêu này trong Silver.

## Đường dẫn và bảo toàn resource cũ

- Bronze theo layout hiện tại: `bronze/muasamcong/<table>/source_date=<day>/run_id=<attempt>/...parquet`.
- Hai bảng nghiệp vụ: `silver/<table>/source_date=<day>/data.parquet`.
- **Lineage riêng của resource mới:** `silver/_governance/contract_performance/<table>/source_date=<day>/data.parquet`. Không ghi đè file observation/quality_issue/quarantine theo ngày của resource cũ.
- Iceberg vẫn dùng bảng quản trị chung; ID observation/issue được upsert theo khóa. Hai bảng nghiệp vụ partition theo `source_date`. Kết quả thay thế phân vùng ngày và upsert revision ID xuyên ngày. Hợp đồng giữ các revision đã quan sát, upsert theo revision ID và không xóa cha chỉ vì một ngày không còn báo cáo; hợp đồng có thể vẫn được ngày khác tham chiếu.
- Báo cáo Silver: `silver/_reports/contract_performance/<run>/report.json`.

Iceberg commit theo từng bảng, không có transaction chung cho toàn bộ resource. Nếu xuất bản giữa chừng lỗi, ngày được báo thất bại và chạy lại theo khóa để hoàn tất; chỉ coi ngày hoàn thành khi cả hai bảng đã được xác nhận.

Resource này luôn xử lý cả hai họ cùng nhau; không hỗ trợ xuất bản riêng bằng `family` hoặc sync riêng một bảng. Khi xuất bản Iceberg, không dùng `--skip-existing`: file Parquet tồn tại chưa chứng minh lần xuất bản trước đã thành công. Công cụ backfill luôn xử lý lại Silver từ Bronze commit hợp lệ.

`sync-iceberg --resource contract_performance` tái tạo từ Bronze đã commit, dùng cùng transformer và cơ chế publication. Không INSERT thô các file hợp đồng lặp giữa nhiều ngày. Sync toàn bộ cũng đi qua nhánh này cho resource mới. Reset toàn bộ Silver hiện hành bao gồm đường dẫn lineage mới.

## Vận hành

Resource mới có budget riêng, không đổi mặc định các resource cũ: tối đa **20 lần bắt đầu HTTP request/giây**, tối đa **4 request đang chạy**. Search, lookup, linked, detail và retry đều dùng cùng budget trong một flow. Không chạy nhiều backfill process đồng thời nếu muốn giữ trần tổng này. Budget thấp hơn được truyền qua bootstrap/Dagster được tôn trọng; cấu hình cao hơn vẫn bị chặn ở trần 20/4.

Timeout/5xx retry tối đa ba lần tổng cộng với backoff. 401/403/429 chặn cả request đang chờ; HTML challenge/login/error page không retry như JSON. Không tự đổi danh tính, token hoặc vượt hạn chế. Thử nghiệm ngắn trước đây không chứng minh 20 request/giây bền vững trên một backfill dài.

```bash
# Gate pilot: công bố 17–23/10/2025, ghi cả Bronze và Silver/Iceberg
.venv/bin/python -m procurement.tools.backfill_contract_performance --pilot-only \
  --output docs/reports/contract-performance-2025-pilot.json

# Pilot đạt mới chạy tiếp toàn bộ năm; ngày đã có Bronze commit được tái sử dụng
.venv/bin/python -m procurement.tools.backfill_contract_performance --year 2025 \
  --output docs/reports/contract-performance-2025-run.json

# Ingestion/Silver thông thường và refresh chọn ngày
.venv/bin/python -m procurement.cli.ingest --resource contract_performance --date 2025-10-17 --refresh
.venv/bin/python -m procurement.cli.silver contract_performance --date 2025-10-17
.venv/bin/python -m procurement.cli.silver sync-iceberg --resource contract_performance --year 2025
```

CLI Bronze và Dagster tạo ResourceSpec theo từng attempt ngày. Asset Bronze: `bronze_contract_performance`; nhóm Silver xuất hai asset bảng nghiệp vụ. Khi đọc resource mới vào Silver, adapter kiểm tra SHA256 của file so với commit và content hash từng payload trước khi xuất bản. Chính sách kiểm tra của resource cũ được giữ nguyên.

Các lệnh cần PostgreSQL metadata, S3 và Iceberg cấu hình trong môi trường; đặt `MUASAMCONG_BASE_URL=https://muasamcong.mof.gov.vn` theo `.env.example`. Không dùng các runner S3-manifest cũ.

Backfill không refresh lại toàn bộ báo cáo cũ mỗi ngày. Để bắt cập nhật muộn, orchestration gọi refresh ngày công bố cần rà soát rồi chạy lại Silver tương ứng. Chưa có bằng chứng filter updatedDate nên chưa tuyên bố incremental theo ngày sửa đổi; số ngày lookback là lựa chọn vận hành, không có bảo đảm từ nguồn.

## Bằng chứng và kiểm thử

Fixture đã làm sạch: `tests/fixtures/contract_performance/2024.json` và `2025.json`, mỗi năm 12 hợp đồng và 12 kết quả từ khảo sát gốc. Không có token/cookie/header trong fixture; các mã cần che dùng pseudonym nhất quán. Fixture không phải một mẫu đại diện thống kê toàn thị trường.

Kiểm thử bao gồm phân trang/giới hạn nguồn, chọn phiên bản, cảnh báo thiếu liên kết, dừng truy cập bị chặn, mapping/Decimal/ngày/null, hash revision, view xuyên ngày, refresh và PostgreSQL commit → Parquet → Silver. Test tích hợp dùng database tạm tự xóa và local storage, không gọi API nguồn.

```bash
.venv/bin/pytest -q tests/ingestion/muasamcong/test_contract_performance_resource.py \
  tests/unit/silver/test_contract_performance.py
# Set TEST_METADATA_DATABASE_URL theo PostgreSQL sandbox local trước lệnh sau
.venv/bin/pytest -q -m integration tests/integration/ingestion/test_contract_performance.py
```

Kết quả pilot/backfill thực tế được ghi riêng trong `docs/reports/contract-performance-2025-*.json`. Chỉ trạng thái `year_verified` với 365 commit và Silver hoàn thành đủ ngày mới được gọi là backfill năm đã xong.

Đối soát pilot trong Iceberg: 427 báo cáo, 0 thiếu hợp đồng đúng phiên bản, 0 revision trùng, 10 trường hợp ngày thực tế sau kế hoạch. Khóa nguồn đã nối khớp được với một phần Silver hiện có: 219/431 phiên bản hợp đồng khớp TBMT và 422/431 khớp KQLCNT. Đây là kết quả đối chiếu trên kho hiện tại, không phải tuyên bố mọi khóa còn lại không tồn tại ở nguồn. Xem `docs/reports/contract-performance-2025-validation.json` (đường dẫn tính từ gốc repo).


## Trạng thái chạy thực tế và tiếp tục sau quota

Lượt đầu hoàn tất Bronze và Silver cho **166 ngày**, gồm 01/01–08/06/2025 và 17–23/10/2025. Có **9.120 báo cáo** và **9.205 phiên bản hợp đồng** trong các view đã khử trùng; chưa đủ năm, còn **199 ngày**. Có 128 báo cáo với ngày thực tế sau kế hoạch. Một báo cáo thiếu hợp đồng trong kết quả tra cứu nguồn đã được giữ lại kèm `missing_contract`, không ghép sang hợp đồng khác. Không coi khoảng min/max ngày là phạm vi liên tục.

Ngày 09/06/2025, gateway của endpoint smart-search trả HTTP 400, nhưng body ghi `429 Too Many Requests` và `API rate limit exceeded`. Một mã hợp đồng đã từng tra thành công cũng bị cùng lỗi, nên đây là giới hạn nguồn, không phải cú pháp mã cụ thể. Không có Retry-After; chưa xác minh được thời điểm reset hay quota theo cửa sổ nào. Số request trong report chỉ tính process crawl cuối, không phải ngưỡng quota đã được chứng minh.

Client hiện nhận diện cả 429 trực tiếp và trường hợp bọc trong 400, dừng các request đang chờ và không đổi token/IP để tiếp tục. Vì vậy chưa thể kết luận 20 request/giây là tốc độ bền vững cho một backfill dài.

Sau khi nguồn cho phép đọc lại, dùng lại lệnh backfill năm; PostgreSQL giúp bỏ qua các ngày đã commit. Không có tiến trình crawl tự khởi động lại trong lúc nguồn đang giới hạn.

Có thể hoàn tất Silver từ các commit đang có mà **không tạo client nguồn hoặc gọi website**:

```bash
.venv/bin/python -m procurement.tools.backfill_contract_performance --silver-only \
  --output docs/reports/contract-performance-2025-run.json
```

Lệnh này exit 0 khi xử lý Silver hiện có thành công; trạng thái năm vẫn `blocked` hoặc `partial` nếu chưa đủ 365 ngày. Danh sách ngày còn thiếu nằm trong `pending_dates` của report. Schema và mã đã đăng ký CLI/Dagster; nếu chạy bằng container đang dùng image cũ, cần build/deploy image mới theo quy trình hiện hành. Các lượt kiểm chứng trên chạy bằng CLI host.
