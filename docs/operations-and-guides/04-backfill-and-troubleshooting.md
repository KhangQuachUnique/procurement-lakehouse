# Bước 4: Backfill Lịch Sử & Sổ Tay Xử Lý Sự Cố (Backfill & Troubleshooting)

## 1. Quy Trình Backfill Dữ Liệu Lịch Sử (5 Năm: 2022–2026)

### 1.1. Backfill Tầng Silver (Spark + Iceberg WAP)
Chạy theo năm hoặc toàn bộ khoảng ngày; hệ thống tự chia batch theo tháng và ghi receipt idempotent vào Postgres (`silver_spark_meta.receipts`):
```bash
.venv/bin/silver backfill --k3s --start-date 2022-01-01 --end-date 2026-10-31
```
Sau khi backfill xong, nên chạy 1 lượt compaction để gộp nhỏ file Parquet & manifest:
```bash
.venv/bin/silver maintain --k3s
```

### 1.2. Rebuild Toàn Bộ Tầng Gold (Two-Tier WAP)
Khuyến nghị chạy `local[4] --driver-memory 4g` cho lần rebuild full 5 năm đầu tiên (~18 phút, 0 OOM):
```bash
.venv/bin/gold rebuild \
  --master local[4] \
  --driver-memory 4g \
  --start-date 2022-01-01 \
  --end-date 2026-10-31 \
  --namespace gold_spark \
  --core-namespace gold_spark_core
```
Sau khi hoàn tất, kiểm tra watermark bằng:
```bash
.venv/bin/gold run --master local[4]   # phải trả về NO_OP
```

---

## 2. Sổ Tay Xử Lý Sự Cố Thường Gặp (Troubleshooting Runbooks)

### Sự cố 1: Sửa code Python nhưng Dagster trong Docker vẫn chạy bản cũ
- **Nguyên nhân:** Container Dagster chạy package đã cài trong `/app/.venv` lúc build image.
- **Cách xử lý chuẩn:** Rebuild lại 3 container Dagster (dùng cache layer nên chỉ mất ~5–10 giây):
  ```bash
  docker compose up -d --build dagster-code-location dagster-webserver dagster-daemon
  ```

### Sự cố 2: `java.net.BindException: Cannot assign requested address` khi chạy `--k3s`
- **Nguyên nhân:** `SPARK_LOCAL_IP` trong `.env` không khớp IP thật của máy (ví dụ đổi mạng Wi-Fi hoặc điền nhầm IP mẫu).
- **Cách xử lý:** Kiểm tra IP thật bằng `tailscale ip -4` (Flow Tailscale) hoặc `ip -4 addr` (Flow LAN), cập nhật `SPARK_LOCAL_IP` và `SPARK_K3S_MASTER` trong `.env`.

### Sự cố 3: `Connect to http://127.0.0.1:8181 failed` khi chạy K3s qua Tailscale
- **Nguyên nhân:** SeaweedFS đã bind vào IP Tailscale (`ICEBERG_CATALOG_BIND_ADDRESS=100.x.y.z`), nhưng `ICEBERG_CATALOG_URI` hoặc `OBJECT_STORAGE_ENDPOINT` vẫn để `127.0.0.1`.
- **Cách xử lý:** Đổi cả 2 biến trên trong `.env` sang `http://<MASTER_TS_IP>:8181` và `http://<MASTER_TS_IP>:8333`.

### Sự cố 4: `Initial job has not accepted any resources` trên K3s
- **Nguyên nhân:** Pod `spark-worker` chưa ở trạng thái `Running` (có thể vừa OOMKill khi chạy full rebuild 5 năm trên worker giới hạn 4 GB RAM).
- **Cách xử lý:**
  ```bash
  kubectl get pods -n procurement -o wide
  kubectl rollout restart daemonset/spark-worker -n procurement
  ```
  Với job full rebuild 5 năm, ưu tiên chạy `--master local[4] --driver-memory 4g` thay vì K3s worker 4 GB.

### Sự cố 5: Đã xóa bảng Iceberg nhưng ổ cứng chưa giảm dung lượng
- **Nguyên nhân:** SeaweedFS đánh dấu block đã xóa là garbage trong file `.dat` và chỉ co file lại sau khi chạy `volume.vacuum`; ngoài ra Docker có thể giữ nhiều image build cũ.
- **Cách xử lý:**
  ```bash
  # 1. Co file volume SeaweedFS (thu hồi DeletedBytes)
  docker compose exec -T object-storage sh -c "echo 'volume.vacuum -garbageThreshold=0.05' | weed shell -master=localhost:9333"

  # 2. Dọn Docker images cũ không dùng tới
  docker image prune -a
  ```
