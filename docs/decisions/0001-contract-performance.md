# ADR-001: Hai bảng Silver cho kết quả thực hiện hợp đồng

## Status

Accepted — 2026-11-03.

Thay thế thiết kế khảo sát `csdl-discovery-20261103/silver-schema-v1` gồm bảy bảng/191 trường.

## Context

Nguồn CSDL công bố báo cáo kết quả thực hiện và hợp đồng liên quan. Khảo sát 2024–2025 xác minh luồng đọc chung, hai root hợp đồng/kết quả, các mốc hoàn thành và sự khác nhau giữa phiên bản hợp đồng. Nhiều biểu mẫu, dòng hàng và thông tin phụ chưa phục vụ nhu cầu Gold đã chốt.

## Decision

- Một resource `contract_performance`, phân vùng theo ngày công bố kết quả.
- Bronze bảo toàn payload; Silver chỉ có hai bảng nghiệp vụ: `contract_revision`, `contract_performance_revision`.
- Hợp đồng lấy theo đúng phiên bản được báo cáo tham chiếu, không giới hạn năm ký; giữ cả phản hồi ban đầu nếu cần tải để xác định phiên bản.
- Lưu ngày kế hoạch/thực tế/điều chỉnh và mã đánh giá nguồn. Gold tính chênh lệch ngày riêng; không coi số chênh lệch nguồn là số ngày chậm có dấu.
- Giá sau thuế chỉ hỗ trợ phân nhóm quy mô. Chưa dùng `completedValue` làm chi thực tế hay thước đo tiết kiệm.
- Metadata PostgreSQL quyết định commit/resume. Lineage Parquet của resource mới có đường dẫn riêng để không ghi đè resource cũ.

## Alternatives considered

Giữ bảy bảng và chuẩn hóa mọi trường khảo sát sẽ tăng chi phí mapping/versioning trước khi có nhu cầu phân tích tương ứng. Chỉ giữ bản hợp đồng mới nhất sẽ làm sai liên kết khi đọc báo cáo cũ. Lọc năm ký hợp đồng sẽ bỏ sót báo cáo thuộc năm công bố cần thu thập.

## Consequences

Gold có thể phân tích tiến độ và đánh giá chất lượng trong tập báo cáo công bố. Chưa có cơ sở suy ra toàn bộ hợp đồng đang thực hiện, giải ngân hoặc tiết kiệm thực tế. Trường bổ sung vẫn có thể lấy lại từ Bronze khi có nhu cầu và bằng chứng rõ ràng.

Chi tiết trường, luồng API, kiểm thử và vận hành: [Contract performance](../contract-performance.md).
