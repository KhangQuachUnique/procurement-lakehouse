# Gộp Parquet của các ngày đã ingest

`procurement.tools.compact_bronze` đọc lại đúng run SUCCESS đang có hiệu lực của
mỗi resource/ngày, gộp riêng từng bảng và tạo một run SUCCESS mới. Không gọi API
nguồn. Chỉ `run_id` trong record thay đổi; payload, Unicode, record trùng,
`ingested_at`, hash và các cột bổ sung được giữ nguyên. Writer ingest không đổi.

## Chạy thử một ngày KHLCNT

Chọn một ngày đã SUCCESS có nhiều file, ví dụ:

```powershell
.venv\Scripts\python.exe -m procurement.tools.compact_bronze plan --resource khlcnt --start-date 2022-09-23 --end-date 2022-09-23
.venv\Scripts\python.exe -m procurement.tools.compact_bronze run --plan exports/bronze-compaction/<plan_id>/plan.json
```

Lệnh `plan` chỉ khảo sát storage và in đường dẫn kế hoạch. Thay `<plan_id>` bằng
ID vừa được in. Kiểm tra `report.json`: số file phải giảm, record trước/sau bằng
nhau, `verification` thành công và trạng thái ngày là `success`. Ngày nhỏ thường
giảm còn một file mỗi bảng; KHLCNT có hai bảng nên thường còn hai file.

Sau khi nghiệm thu ngày nhỏ trên storage thật, có thể lập kế hoạch lịch sử:

```powershell
python -m procurement.tools.compact_bronze plan --resource all --year 2022
python -m procurement.tools.compact_bronze run --plan exports/bronze-compaction/<plan_id>/plan.json
```

`--year` nhận năm đã kết thúc. Với năm hiện tại, dùng cặp `--start-date/--end-date`
đến trước hôm nay. Có thể chọn một resource thay cho `all`. Mặc định ZSTD và mục
tiêu 128 MiB/file nén; `plan --target-mib 64` đổi mục tiêu. Kích thước là xấp xỉ:
writer đóng file sau batch vượt mục tiêu, footer được ghi khi đóng file.

## Kiểm chứng và lưu trữ

Kế hoạch cố định namespace storage, baseline, các file và dấu nhận diện object,
PageManifest, quality evidence và cấu hình ghi. Ngày chưa SUCCESS, đang có attempt
chạy, rỗng hoặc mỗi bảng chỉ có tối đa một file được báo trong `skipped`.

Mỗi ngày được tải vào thư mục tạm dưới thư mục kế hoạch. PyArrow đọc batch và
kiểm tra lineage/hash; DuckDB so sánh `EXCEPT ALL` hai chiều trên toàn bộ cột trừ
`run_id`, giữ đúng số lượng record trùng. File upload được kiểm tra SHA-256 và
đọc lại qua committed reader. Schema hợp nhất cột nullable thiếu và kiểu null;
các xung đột kiểu khác hiện được từ chối để tránh chuyển đổi mất dữ liệu.

File mới nằm trong cấu trúc Bronze thông thường. PageManifest được sao chép số
đếm, thay identity/thời gian attempt; quality JSON hiện có được sao chép nguyên
byte. Không có evidence thì không tạo evidence mới. Provenance nằm tại:

```text
_ops/muasamcong/<resource>/run_id=<new_run_id>/compaction.json
```

Provenance ghi nguồn/đích, cấu hình, số lượng và kiểm chứng trước commit. Trạng
thái xuất bản có hiệu lực vẫn do DayManifest quyết định. Các artifact local:

```text
exports/bronze-compaction/<plan_id>/
  plan.json
  report.json
  days/<resource>-<date>.json
  attempts/<failed_run_id>.json   # được giữ khi retry
```

Chương trình cần dung lượng tạm cho file nguồn, file đã gộp, bản tải lại kiểm
chứng và phần spill của DuckDB. DuckDB giới hạn bộ nhớ 1 GiB, có thể spill xuống
đĩa. Đợt gộp phát sinh lượt đọc/ghi storage; lợi ích giảm số file dành cho các
lượt đọc sau. Không tự xóa run cũ hoặc file upload dở.

## Resume và xử lý lỗi

Chạy lại cùng lệnh `run --plan ...` để resume. Ngày đã SUCCESS được xác minh và
không tạo run khác. Attempt FAILED đã xác nhận sẽ retry bằng run ID mới. Nếu
baseline, file nguồn hoặc evidence đổi, cần lập kế hoạch lại cho ngày đó.

Mặc định chạy tuần tự và dừng khi lỗi. `--continue-on-error` tiếp tục với ngày kế
tiếp khi lỗi đã xác nhận; trạng thái `commit_uncertain` luôn dừng. Ctrl+C trước
commit đánh dấu attempt FAILED; Ctrl+C lúc commit giữ `commit_uncertain`. Resume
chỉ hoàn tất attempt này khi đọc lại thấy DayManifest SUCCESS và kiểm chứng đạt.
Nếu vẫn RUNNING hoặc không xác minh được, phải kiểm tra attempt trước khi xử lý
tiếp; không tự biến mất xác nhận thành FAILED. Không sửa checkpoint để ép retry.

Nếu số file mới không giảm, báo `no_benefit`, đóng candidate thành FAILED nhưng
giữ baseline SUCCESS. Không upload Parquet mới trong trường hợp này.

Compaction dùng execution lock chung với ingest/repair trên cùng host, theo
namespace storage và `INGESTION_LOCK_DIR`. Mọi tiến trình phải dùng cùng thư mục
khóa. Đây không phải khóa phân tán giữa nhiều host; baseline và active attempts
được kiểm tra lại ngay trước commit.

Sau SUCCESS, coverage, Ops, explorer/count, audit, watcher và transfer chọn run
mới theo cơ chế effective SUCCESS hiện tại. Ops cần chờ lượt đồng bộ index kế
tiếp; phiên đọc đã cố định run cũ vẫn đọc được. Export/import dùng định dạng bundle
hiện tại, không bổ sung quality/provenance vào định dạng archive.

## Kiểm tra local

```powershell
.venv\Scripts\python.exe -m pytest tests/storage/test_compaction.py -q
```

Các test dùng Parquet thật trên filesystem tạm: KHLCNT hai bảng, giữ record trùng
và Unicode, selection của các reader, export/import round-trip, lỗi từng bước,
commit mất xác nhận, Ctrl+C, resume, baseline đổi, schema và `no_benefit`.
Không dùng API nguồn hoặc bucket đang cấu hình.
