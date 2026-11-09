# KIẾN TRÚC PROCUREHOUSE LAKEHOUSE: HÀNH TRÌNH TIẾN HÓA, BÀI HỌC KINH NGHIỆM & CÁC CẢI TIẾN "CÁCH MẠNG"

> **Tài liệu đúc kết kỹ thuật thực chiến**  
> *Lưu giữ bối cảnh, phân tích các sai lầm/khó khăn kiến trúc ban đầu và ghi nhận các bước chuyển mình mang tính bước ngoặt của hệ thống Procurement Lakehouse.*

---

## 1. TỔNG QUAN HỆ THỐNG & TẦM NHÌN

Hệ thống **Procurement Lakehouse** được thiết kế nhằm xử lý, chuẩn hóa và phân tích dữ liệu đấu thầu quốc gia từ Hệ thống Mạng đấu thầu Quốc gia (Mua Sắm Công - Bộ Kế hoạch và Đầu tư). Hệ thống vận hành theo kiến trúc Medallion 4 tầng trên nền tảng công nghệ nguồn mở hiện đại:
- **Lưu trữ & Định dạng Bảng**: Apache Iceberg Format v2 trên nền tảng SeaweedFS (tương thích AWS S3).
- **Công cụ Tính toán & Chuyển đổi**: DuckDB SIMD in-memory engine kết hợp Apache Arrow.
- **Điều phối Dữ liệu (Orchestration)**: Dagster Modern Asset-based Lineage với phân vùng Ngày đồng bộ.

Mục tiêu nghiệp vụ cốt lõi là cung cấp góc nhìn toàn cảnh về vòng đời mua sắm công: từ **Nhu cầu (Demand)** $\rightarrow$ **Mời thầu (Notice)** $\rightarrow$ **Trao thầu (Award)** $\rightarrow$ **Tiết kiệm ngân sách (Savings)** $\rightarrow$ **Thời gian chu kỳ (Duration)** $\rightarrow$ **Trạng thái chuyển dịch (Status)** $\rightarrow$ **Năng lực & Thị phần Nhà thầu (Supplier Performance)**.

---

## 2. PHẦN I: NHỮNG "ÁC MỘNG", KHÓ KHĂN & BÀI HỌC TỪ KIẾN TRÚC CŨ

Để đạt được trạng thái tinh gọn, ổn định và thanh thoát như hiện tại, hệ thống đã trải qua những giai đoạn bế tắc nghiêm trọng về mặt thiết kế. Dưới đây là những sai lầm điển hình và bài học kinh nghiệm được rút ra:

### 2.1. Cái bẫy "Hộp đen nguyên khối" (The Monolithic Gold Black Box)
- **Sai lầm**: Ở giai đoạn đầu, toàn bộ tầng Gold được gói kín trong một Asset/Op khổng lồ mang tên `gold_v2_active_release`.
- **Hệ quả**:
  - Khi người vận hành bấm chạy trên Dagster, giao diện chỉ hiển thị duy nhất một node to đùng quay tròn. Hoàn toàn không biết pipeline đang tính Fact nào, Dimension nào, tiến độ đến đâu.
  - Chỉ cần 1 lỗi nhỏ ở 1 trường dữ liệu của 1 Fact là toàn bộ tiến trình văng lỗi (`FAIL`), cuốn trôi toàn bộ công sức tính toán của các bảng khác.
  - Không thể chạy kiểm thử, debug hay backfill riêng lẻ từng bảng.
  - Làm triệt tiêu hoàn toàn ưu điểm lớn nhất của Dagster: *Asset Lineage trực quan và khả năng phục hồi theo từng node*.

### 2.2. Xung đột phân vùng Ngày vs Tháng (The Daily vs Monthly Mismatch)
- **Khó khăn & Sai lầm**:
  - Tầng Bronze và Silver được thiết kế chuẩn phân vùng Ngày (`DailyPartitionsDefinition`).
  - Tuy nhiên, tầng Gold Fact ban đầu lại bị ép theo phân vùng Tháng (`MonthlyPartitionsDefinition`) với lý do "báo cáo tài chính thường tổng kết theo tháng".
- **Bế tắc**:
  - Dagster cấm triệt để việc gom các asset có phân vùng khác nhau vào cùng một Job (`DagsterInvalidDefinitionError`).
  - Hàng ngày, khi crawler lấy dữ liệu mới về Bronze và Silver chuẩn hóa xong, tầng Gold bị "chặt đứt", không thể tự động kích hoạt tính toán tiếp mà phải chờ tới cuối tháng hoặc phải viết script chạy ngoài luồng bằng tay.
  - Người dùng không có cách nào quan sát được hiệu ứng *"chạy increment 1 ngày thấy sáng đèn xanh tuần tự từ trái sang phải"*.

### 2.3. Hiểm họa Trùng lặp & Bế tắc Tính lũy thừa (Idempotency Failure)
- **Bối cảnh**: Dữ liệu đấu thầu liên tục có hiện tượng chỉnh sửa gói thầu, làm rõ hồ sơ mời thầu, cập nhật quyết định hủy thầu (Late-arriving data và Updates).
- **Sai lầm**: Sử dụng cơ chế ghi nối đuôi (Append) hoặc thiếu giao dịch xóa phân vùng nguyên tử khi tính toán lại.
- **Hệ quả**: Khi người vận hành chạy lại (rerun) một ngày/tháng dữ liệu, các bản ghi cũ không được dọn dẹp sạch sẽ, dẫn đến tình trạng nhân đôi dữ liệu (**Duplicate Rows**). Các chỉ số tài chính trọng yếu (Tổng giá trị gói thầu, Giá trị trúng thầu, Tỷ lệ tiết kiệm) bị thổi phồng nghiêm trọng.

### 2.4. Nhiễm độc Luồng Chính (Pipeline Group & Dependency Pollution)
- **Sai lầm**: Gom toàn bộ tài nguyên vào chung nhóm `group_name="bronze"` và `group_name="silver"`.
- **Bất cập**:
  - Các phân hệ mới đang thử nghiệm (như `contract_performance` – kết quả thực hiện hợp đồng sau trúng thầu, hoặc `bid_opening` – biên bản mở thầu) chưa được kiểm chứng đầy đủ về chất lượng dữ liệu nhưng lại bị kéo vào chung Job vận hành hàng ngày (`lakehouse_end_to_end`).
  - Asset `gold_dim_geography` khai báo thừa dependency sang bảng `silver_project_location`, vô tình kéo cả cụm dự án vào đồ thị.
  - Khai báo nhầm một asset không tồn tại trong hợp đồng dữ liệu Iceberg là `gold_dim_bidding_type`, khiến Dagster ném lỗi `Table not found` và làm đồ thị bị **đỏ lòm ngay từ bước đầu**.

### 2.5. Sự đảo lộn Thứ tự Đồ thị (Topological Graph Inversion)
- **Hiện tượng kỳ quặc trên UI**:
  - Các bảng Dimension tĩnh do không khai báo dependency nên có `Depth = 0`, bị Dagster xếp vào Cột 1 đứng ngang hàng với Bronze. Nhìn vào đồ thị như thể "Tầng Gold được sinh ra trước cả khi tầng Silver bắt đầu".
  - Bảng Fact đo thời gian chu kỳ (`fact_duration_daily`) và trạng thái (`fact_status_daily`) nhảy cóc lên Cột 3 chạy trước cả `fact_demand_daily` (Kế hoạch thầu) và `fact_notice_daily` (Mời thầu) ở Cột 4. Luồng dữ liệu bị đảo ngược hoàn toàn logic nghiệp vụ.

---

## 3. PHẦN II: NHỮNG BƯỚC NGOẶT CẢI TIẾN "CÁCH MẠNG"

Trải qua quá trình tái cấu trúc toàn diện, hệ thống đã đạt được 6 bước tiến mang tính cách mạng:

### 3.1. Cách mạng 1: Kiến trúc Hexagonal & Tách nhỏ 7 Facts Độc lập
- Áp dụng kiến trúc Lục giác (Hexagonal Architecture / Ports & Adapters) cho tầng Gold:
  - `gold.domain`: Chứa logic nghiệp vụ thuần túy, định nghĩa cấu trúc 9 Dimensions, 7 Facts, Grain và công thức toán học.
  - `gold.application`: Quản lý quy trình tính toán phân vùng ngày/tháng (`processor.py`), bộ kiểm toán đối soát (`reconciliation.py`).
  - `gold.storage`: Tương tác với hạ tầng Iceberg REST Catalog và DuckDB.
- Khối nguyên khối vỡ vụn thành **7 `@asset` độc lập**: Mỗi Fact là một thực thể riêng biệt, có grain rõ ràng, có bộ kiểm thử chất lượng dữ liệu tự động (`AssetCheckSpec`) kiểm tra Foreign Key và giá trị tài chính.

### 3.2. Cách mạng 2: Thống nhất Toàn diện Phân vùng Ngày (End-to-End Daily Incrementals)
- Đồng bộ hóa toàn bộ 4 tầng dữ liệu sang cùng một thước đo: Phân vùng Ngày (`BRONZE_PARTITIONS`).
- **Trải nghiệm vận hành đột phá**: Khi bấm Materialize 1 ngày bất kỳ:
  $$\text{Bronze (4)} \longrightarrow \text{Silver (13)} \longrightarrow \text{Gold Dimensions (9)} \longrightarrow \text{Gold Facts (7)} \longrightarrow \text{Audit}$$
  Từng node sáng đèn xanh từ trái sang phải, không còn bất kỳ sự gián đoạn hay đứt gãy nào.

### 3.3. Cách mạng 3: Cơ chế Ghi đè Phân vùng Nguyên tử (Idempotent 100%)
- Triển khai cơ chế **Partition Overwrite trong Giao dịch Nguyên tử** tại `processor.py`:
  ```python
  conn.execute("BEGIN")
  try:
      conn.execute(f"DELETE FROM lake.gold_v2.{fact} WHERE report_date >= '{start_date}' AND report_date < '{end_date}'")
      conn.execute(f"INSERT INTO lake.gold_v2.{fact} ({cols}) SELECT {cols} FROM _upload_fact")
      conn.execute("COMMIT")
  except BaseException:
      conn.execute("ROLLBACK")
      raise
  ```
- Tầng Dimension sử dụng cơ chế `INSERT ... WHERE NOT EXISTS`.
- **Cam kết bảo toàn dữ liệu**: Dù bạn bấm chạy lại 1 ngày bao nhiêu lần thì dữ liệu cũ tự động bị dọn dẹp và ghi đè mới, **tuyệt đối không bao giờ bị nhân đôi dòng (Zero Duplicate)**.

### 3.4. Cách mạng 4: Phân định Ranh giới Nghiêm ngặt (Core vs Pilot Isolation)
- **Bảo toàn tài nguyên**: Giữ trọn vẹn tài nguyên quy hoạch dự án (`project`) trong luồng chính để làm giàu dữ liệu địa lý cho `gold_dim_geography`.
- **Cô lập phân hệ thử nghiệm**: Chuyển module `contract_performance` sang các group `bronze_pilot` và `silver_pilot`, đồng thời tạo riêng job `pilot_contract_performance_job`.
- Luồng chính `lakehouse_end_to_end` chỉ còn đúng **33 assets cốt lõi**, sạch bóng và tập trung 100% vào chu trình đấu thầu.

### 3.5. Cách mạng 5: Chuẩn hóa Lineage 4 Cột Thẳng tắp (Pragmatic Topological Sorting)
- Căn chỉnh quan hệ dependency giữa Silver và Dimensions:
  - **Cột 1**: 4 Bronze assets (`khlcnt`, `notify_contractor`, `contractor_result`, `project`).
  - **Cột 2**: 13 Silver assets chuẩn hóa.
  - **Cột 3**: Đủ 9 Gold Dimensions chuẩn (xóa vĩnh viễn node đỏ `gold_dim_bidding_type`).
  - **Cột 4**: Cả 7 Gold Facts đứng thẳng tắp, cùng chạy sau khi toàn bộ Dimensions ở Cột 3 đã hoàn tất.
  - **Cột 5**: Check kiểm toán toàn vẹn `full_gold_reconciliation_audit`.

### 3.6. Cách mạng 6: Cột mốc Vận hành Dữ liệu Lịch sử 5 Năm (2022 - 2026)
- Hoàn thành xuất sắc chiến dịch Backfill dữ liệu lịch sử xuyên suốt 5 năm:
  - Tổng số bản ghi Fact được tính toán: **2.274.659 dòng**.
  - Tỷ lệ Foreign Key mồ côi: **0% (Zero Orphan FKs)**.
  - Toàn bộ tầng báo cáo `gold.*` được ánh xạ bằng cơ chế **Zero-Copy Views** trỏ trực tiếp vào dữ liệu vật lý `lake.gold_v2.*`.

---

## 4. PHẦN III: NHỮNG BÀI HỌC KINH NGHIỆM ĐẮT GIÁ

1. **Grain là linh hồn của Fact Table**:  
   Không bao giờ được lập lờ về định nghĩa "một dòng trong bảng đại diện cho cái gì". Khi grain được định nghĩa chuẩn xác (ví dụ: `report_date, geography_key, sector_key, value_band_key`), việc tính toán aggregate trở nên tường minh và không bị trùng lặp.
2. **Lineage trực quan là thước đo sức khỏe của Kiến trúc**:  
   Nếu đồ thị DAG của bạn bị chéo dây, nhảy cóc, hoặc có tầng chạy trước cả tầng cha, đó không phải là lỗi hiển thị của công cụ — đó là tín hiệu cảnh báo rằng thiết kế dependency của bạn đang có vấn đề.
3. **Idempotency phải được thiết kế ngay từ dòng code đầu tiên**:  
   Trong Data Engineering, lỗi mạng, lỗi API và nhu cầu backfill dữ liệu là chuyện diễn ra hàng ngày. Mọi thao tác ghi dữ liệu (Write operations) bắt buộc phải có tính lũy thừa (chạy nhiều lần kết quả vẫn không đổi).
4. **Cô lập phân hệ thử nghiệm (Sandbox the Pilot)**:  
   Không bao giờ được trộn lẫn dữ liệu đang thử nghiệm với dữ liệu sản xuất. Việc tách các job thử nghiệm độc lập vừa bảo vệ pipeline chính, vừa cho phép các kỹ sư tự do thử nghiệm mà không sợ làm sập hệ thống.
