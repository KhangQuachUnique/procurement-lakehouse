# Hướng dẫn phát triển Silver

## Luồng duy nhất

`CLI → service → committed Bronze selection → registry transformer → assemble/validate → writer`

- CLI Typer chỉ nhận tham số và hiển thị kết quả.
- `service.py` chạy theo ngày và xuất candidate/report; `pipeline.py` là batch build có cache, dùng cùng dispatcher, transformer và assembler.
- `observations.py` tạo `RawRecordEnvelope`, gọi transformer đã đăng ký và giữ JSON nguồn nguyên văn.
- Transformer kế thừa `BaseResourceTransformer`, trả `TransformedRecord`.
- `schemas.py` là danh mục SQL schema chung. Writer không chứa mapping nghiệp vụ.
- Assembler giữ raw child và typed projection được khai báo; không đoán loại child bằng tên field.

Hiện có transformer project và ba family notice. Resource chưa có transformer báo lỗi rõ; không còn fallback generic. CLI resource hiện hỗ trợ notice/tbmt.

## Viết transformer

1. Thêm module dưới `processing/silver/transformers/`, đăng ký bằng `@register_transformer(table_name)` và import trong `transformers/__init__.py`.
2. Implement `extract_root`, `extract_source_identity`, `extract_attributes`, `extract_references`, `extract_children`.
3. Dùng parser chung: tiền Decimal chính xác, boolean không dùng truthiness, ngày giữ raw và quy tắc timezone.
4. Giữ mã nguồn, ID opaque và leading zeros; không suy đoán currency hoặc outcome.
5. Trả `QualityIssue` khi trường tùy chọn lỗi; quarantine khi thiếu/mâu thuẫn identity; không mất observation.
6. Mảng con giữ path/ordinal/raw. Nếu có typed projection, khai báo `typed_table` và `typed_attributes`, với child_id/revision_id khớp raw child.
7. Đăng ký schema bảng con trong danh mục `CHILD_SCHEMAS`; không thêm điều kiện riêng cho resource vào assembler.
8. Khi mapping đổi, cập nhật `mapping_version` và `revision_contracts`. Cache build lấy contract từ registry theo source tables được chọn.
9. Resource-level command tra `resources.py` để biết family/output tables; không yêu cầu người dùng nhớ tên bảng Bronze.

`TransformedRecord.to_dict()` là contract duy nhất cho assembler và cache; không giữ adapter legacy.

## Schema notice

Xem [Silver notice](silver_notice.md) để biết grain, mapping, tiền tệ, quan hệ và giới hạn.
Danh mục schema nằm trong `processing/silver/schemas.py`, schema riêng notice nằm trong `notice_schema.py`.
Tổng hiện tại 20 bảng: 17 bảng nền và 3 bảng con notice. Có schema không đồng nghĩa resource đã có transformer.

## Chạy

```powershell
uv run --no-sync silver notice --date 2025-01-01
uv run --no-sync silver notice --start-date 2025-01-01 --end-date 2025-01-31
uv run --no-sync silver tbmt --date 2025-01-01
uv run --no-sync silver notice --date 2025-01-02 --family reoffer
uv run --no-sync silver list
```

Các lệnh bảng riêng `run`, `inspect`, `query` của CLI cũ đã được bỏ. Kiểm tra kết quả qua report.json và các file Parquet được ghi trong report.

Source selection dùng commit manifests `_control` trên S3; ngày thiếu commit báo lỗi. Chưa hỗ trợ commit chỉ nằm trong PostgreSQL.
CLI hiện xuất Silver candidate riêng theo run/ngày, chưa publish Iceberg release. Không đọc candidate theo ngày như global current_entity.
Khi triển khai Iceberg, cần schema evolution hoặc namespace mới, backfill và đối soát trước khi chuyển consumer; CREATE TABLE IF NOT EXISTS không tự thêm cột.

## Kiểm thử

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/silver -q -p no:cacheprovider
.\.venv\Scripts\python.exe -m pytest tests/unit -q -p no:cacheprovider
.\.venv\Scripts\ruff.exe check --no-cache src/procurement/cli/silver.py src/procurement/processing/silver src/procurement/processing/warehouse.py tests/unit/silver
```

Test cần kiểm mapping nguồn, raw preservation, Decimal/timezone, null/quarantine, PK/FK child, dispatch parity và ghi–đọc schema thật. Service tests dùng filesystem trong RAM, đối soát source hash/count và giữ nguyên observed_at. Không chỉ test số dòng accepted.
