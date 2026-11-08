# Quyết định: quarantine hồ sơ có root null — 2026-11-07

## Đã được người dùng duyệt

Khang Quach (`gateway-owner`): “THnog nhất các hồ sơ bị null cho vào quaratine ngoại lệ hết ko dùng nó trong tính toán”.

Quyết định này thay giới hạn một ID của quyết định trước: áp dụng cho TẤT CẢ hồ sơ có nội dung gốc/root được chứng minh là JSON null. Không phải chỉ cho phép đúng ba ID hiện có và không phải miễn mọi lỗi dữ liệu.

- Giữ Bronze/raw, observation, quality_issue và quarantine cùng hash/file/ordinal/ID truy vết.
- Không dựng domain revision hoặc đóng góp fact từ hồ sơ null; không sửa giá trị, điền số liệu hay giả lập lịch sử.
- Chỉ nhận `missing_root` khi payload hoặc root chính tường minh bằng null, payload hash và lineage khớp, observation/quarantine/error đối soát đủ, không lọt vào domain.
- JSON thiếu trường/không đúng kiểu, lỗi tiền tệ/ngày/identity/hash, lỗi khác và thiếu coverage vẫn chặn. Một cột tùy chọn null trong hồ sơ hợp lệ KHÔNG phải căn cứ loại cả hồ sơ.
- Receipt v3 lưu số lượng và danh sách chứng cứ thật của từng phạm vi, policy digest cố định cho cùng chính sách. Prefix/full có thể khác số hồ sơ null mà không đổi grain hay chính sách.
- Receipt v1 strict và v2 một-ID giữ nguyên để đọc lịch sử, không ghi lại thành “clean”. Target/state đã đóng băng policy cũ không tự đổi qua v3.

Policy: `NULL_ROOT_QUARANTINE_V1`; digest `287dc8ff5111622ada7e9625fd1ff4ec11cb049d14a3afdc6bde560a1e3668c3`.

## Đối soát lịch sử 2022–2025

4.383/4.383 lượt ngày–nguồn, 5.359.973 raw observations. Ba hồ sơ KHLCNT có cùng payload root null:

| Ngày | Source ID |
|---|---|
| 2024-04-10 | f705dfb8-e887-4d6e-aba5-bffba046de24 |
| 2025-11-26 | 3f0af505-9ad7-4fc4-adb8-dea6a00dcd58 |
| 2025-12-05 | cf956479-8ddb-482c-84fa-d9886eaedd2a |

Danh sách hash/observation/file/ordinal/root_path: `data/gold-v2-acceptance/recovery-20261107/null-policy-manifest.json` (checkout nghiệm thu). Nguồn canonical vẫn NEEDS_REVIEW; receipt mới chấp nhận với ngoại lệ rõ ràng, không tuyên bố nguồn sạch hoàn toàn.

Không mở rộng quyền production rollout/cutover, xóa dữ liệu, GC snapshot hoặc xóa code checkout production. Các gate đó giữ nguyên.
