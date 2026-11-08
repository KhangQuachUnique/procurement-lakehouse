# CLI inventory sau chuyển đổi PostgreSQL

Ngày rà soát: 2026-11-03. Phạm vi: mọi entry point `__main__`, argparse/Typer và
`project.scripts` trong `src/procurement`, cùng Docker CMD và hướng dẫn vận hành.

## Còn sử dụng

| Entry point | Nguồn trạng thái / chức năng |
| --- | --- |
| `procurement.cli.ingest` | PostgreSQL MetadataService; cùng IngestionService với Dagster Bronze; dry-run đọc commit |
| `procurement.cli.silver`, script `silver` | Arrow Bronze adapter đọc commit PostgreSQL; cấu hình DB thiếu phải báo lỗi |
| `procurement.tools.profile_bronze` | Profiling qua PostgresActiveBronzeAdapter |
| `procurement.profiling.adapters.driver.cli` | Cùng profiling driver, PostgreSQL |
| `procurement.tools.bronze_explorer` | CLI bắt buộc MetadataService; không fallback khi DB lỗi |
| `procurement.tools.count_records` | Chọn file qua PostgreSQL, đếm footer; `--cached` đọc báo cáo local |
| `procurement.tools.profile_notify_fields` | Chọn commit/file từ PostgreSQL; kiểm count, lineage và payload hash |
| `procurement.tools.benchmark` | Benchmark API nguồn và state local; không publish Bronze |
| `procurement.tools.audit_notify_routing` | Đọc API nguồn, lưu evidence local; không dùng coverage S3 |
| `procurement.tools.notify_quality_evidence` | Tạo/kiểm evidence từ API nguồn và file local |
| `procurement.cli.import_metadata`, `procurement.tools.import_metadata` | Ngoại lệ chuyển đổi: nhập manifest S3 lịch sử vào PostgreSQL, không crawl |
| `procurement.orchestration.reconcile` | Đồng bộ materialization từ thư mục S3 vào Dagster; không phải commit/ingestion và không xác minh dữ liệu. Không dùng để kết luận ngày đã ingest thành công |

## Đã gỡ

- `procurement.jobs.ingest` và `procurement.jobs.runner`: lập kế hoạch/chạy ingest theo S3 manifest.
- `procurement.tools.recover_result_2024`: recovery riêng dựa trên job ingest cũ.
- `procurement.tools.compact_bronze` và alias `procurement.cli.compact_bronze`.
- `procurement.tools.bronze_transfer` và alias `procurement.cli.bronze_transfer`.
- `procurement.tools.repair_bronze_quality` và alias `procurement.cli.repair_bronze_quality`.
- `procurement.tools.watch_bid_opening` và alias `procurement.cli.watch_bid_opening`.
- `procurement.tools.audit_bronze_quality`.
- Entry point CLI trong `procurement.ops.sync`: đồng bộ index SQLite theo S3 manifest.

Không giữ alias về implementation ingest cũ. Lệnh đã gỡ sẽ không còn module để chạy;
image Docker cũ phải build lại. Các test chỉ dành cho CLI/runner cũ được thay bằng
regression test cho CLI PostgreSQL. Test và thư viện đọc/ghi dữ liệu lịch sử vẫn giữ.

## Giới hạn của thay đổi

Đây là việc dọn CLI, không phải xóa toàn bộ thư viện S3 manifest. Import dữ liệu
lịch sử còn cần các model/parser cũ; compaction/transfer/quality/watcher và một số
workflow Dagster phụ trợ còn phụ thuộc thư viện đó. Việc gỡ CLI không làm các
workflow này tự chuyển sang PostgreSQL. Đặc biệt `quality_audit_job` và
`quality_repair_job` vẫn gọi workflow legacy; không dùng chúng để xác nhận hay
sửa commit của luồng mới trước khi chuyển đổi riêng.

Không thay đổi commit PostgreSQL, file Bronze hay chạy crawl trong quá trình dọn.
Xem [lệnh vận hành](operations.md) để chạy CLI mới.
