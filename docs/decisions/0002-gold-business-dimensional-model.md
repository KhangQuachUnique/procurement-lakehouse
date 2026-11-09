# ADR-002: Mô hình chiều phục vụ nhu cầu phân tích khách hàng

## Status

Proposed — 2026-11-04. Thiết kế mới; chưa áp dụng schema hoặc pipeline vào kho.

## Context

Đặc tả hiện hành yêu cầu tiết kiệm theo ngành, phân khúc quy mô gói, khu vực, thời gian và bên mua; đồng thời phục vụ thị trường, cơ hội nhà thầu và tổ chức đấu thầu. Người dùng yêu cầu thiết kế từ các đầu ra đó. Phạm vi nguồn gồm dự án, kế hoạch, mời thầu và kết quả lịch sử 2022–2025.

## Decision

Đề xuất năm fact: nhu cầu gói, thông báo mời, khoản trao theo phạm vi, so sánh tiết kiệm và đợt lựa chọn. Chín dimension dùng chung cung cấp ngày, gói, phạm vi, tổ chức/liên danh, ngành, đặc điểm lựa chọn, địa bàn, tiền tệ và khoảng quy mô.

Fact tiết kiệm giữ cặp giá cùng phạm vi; trang tổng, phân khúc và đối sánh dùng cùng tập so sánh. Phân hoạch toàn gói hoặc các lô không chồng nhau; không đưa toàn gói và lô vào cùng tổng. Khoản trao không nhân theo số bên nhận hoặc địa bàn. Đơn vị tổ chức và liên danh được định danh theo vai trò, không gộp theo tên.

Các fact dùng một bản số liệu chốt khi truy vấn. Bản bổ sung không thay đổi số liệu đã công bố. Chính sách baseline, phân loại và độ đủ nguồn phải được kiểm chứng trước khi phát hành metric; chưa coi mô hình đề xuất là chứng nhận dữ liệu.

## Alternatives considered

Một bảng phẳng chung làm nhân tiền khi gói có nhiều notice, lô, bên nhận và địa bàn. Một fact mỗi resource tạo thêm bảng không gắn với quyết định khách hàng. Chỉ lưu bảng tổng hợp tiết kiệm mất khả năng đi xuống từng gói và đối sánh theo bộ lọc mới. Thiết kế chọn các quá trình nghiệp vụ riêng cấp chi tiết, tái sử dụng chiều và định nghĩa chỉ số.

## Consequences

Cần xác minh định danh gói/đợt/phạm vi và các khoản trao trước khi tổng hợp. Có thể phát hành lát cắt tiết kiệm đủ chứng cứ trước, đồng thời hiển thị phần chưa so được; không cần chờ mọi hồ sơ giải được qua TBMT. Phân tích quy trình chờ bằng chứng các đợt/mốc tương ứng. Chưa mở nguồn mở thầu hoặc thực hiện hợp đồng trong giai đoạn này.

Chi tiết cấp dòng, khóa, cột, phép tổng hợp và đường ánh xạ nhu cầu: [thiết kế fact/dim](../gold-dimensional-model.md).
