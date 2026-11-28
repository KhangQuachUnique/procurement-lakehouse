# Bước 2: Sổ Tay Tra Cứu Toàn Bộ Lệnh CLI (`ingest`, `silver`, `gold`)

Hệ thống cung cấp 3 CLI chính thức được cài sẵn trong `.venv/bin/` (`pyproject.toml` `[project.scripts]`):

| CLI | Mã nguồn | Tầng phụ trách |
| :--- | :--- | :--- |
| `ingest` | `src/procurement/cli/ingest.py` | Tầng **Bronze** (cào API Mua Sắm Công → Parquet trên S3 + commit Postgres) |
| `silver` | `src/procurement/silver/cli.py` | Tầng **Silver** (Spark 4.0 + Iceberg WAP → 21 bảng `lake.silver_spark`) |
| `gold` | `src/procurement/gold/cli.py` | Tầng **Gold** (Spark 4.0 Two-Tier WAP → 12 bảng Core + 17 bảng Mart `lake.gold_spark`) |

---

## 1. CLI Tầng Bronze (`ingest`)

```bash
# Cào toàn bộ 6 tài nguyên cho 1 ngày cụ thể
.venv/bin/ingest --resource all --date 2026-10-30

# Cào 1 tài nguyên theo khoảng ngày
.venv/bin/ingest --resource khlcnt --start-date 2026-10-01 --end-date 2026-10-31

# Dry-run: kiểm tra ngày còn thiếu trên Postgres mà không gọi API
.venv/bin/ingest --resource project --date 2026-10-30 --dry-run

# Ép cào lại dù ngày đó đã có commit
.venv/bin/ingest --resource contractor_result --date 2026-10-30 --refresh
```

*Tài nguyên hỗ trợ:* `all`, `khlcnt`, `project`, `notify_contractor`, `contractor_result`, `bid_opening`, `contract_performance`.

---

## 2. CLI Tầng Silver (`silver`)

Cả 3 subcommand (`run`, `backfill`, `maintain`) đều hỗ trợ:
- `--k3s`: tự động dùng `SPARK_K3S_MASTER` và tự nhận diện IP Driver (LAN hoặc `SPARK_LOCAL_IP` từ `.env`).
- `--master local[4]` / `--driver-memory 4g`: chạy cục bộ trên máy host không cần K3s.
- `--namespace <ns>`: mặc định `silver_spark`.

### 2.1. Chạy Incremental Hàng Ngày (`silver run`)
Tự động quét các Bronze commit mới chưa có receipt trong Postgres và chỉ xử lý phần thay đổi:
```bash
# Chạy incremental tất cả 6 tài nguyên trên K3s
.venv/bin/silver run --k3s

# Chạy lẻ 1–2 tài nguyên cục bộ
.venv/bin/silver run --master local[4] --resource notice,khlcnt
```

### 2.2. Chạy Backfill Khoảng Ngày (`silver backfill`)
```bash
# Backfill toàn bộ 6 tài nguyên trong khoảng ngày
.venv/bin/silver backfill --k3s --start-date 2026-01-01 --end-date 2026-10-31

# Ép chạy lại dù đã có receipt (--force)
.venv/bin/silver backfill --k3s --resource project --start-date 2026-01-01 --end-date 2026-01-31 --force
```

### 2.3. Bảo Trì Iceberg (`silver maintain`)
Chạy trọn bộ 4 thủ tục Iceberg (`rewrite_data_files`, `rewrite_manifests`, `expire_snapshots`, `remove_orphan_files`):
```bash
# Kiểm tra trước (không xóa file orphan thật)
.venv/bin/silver maintain --k3s --dry-run-orphans

# Chạy bảo trì chính thức
.venv/bin/silver maintain --k3s
```

---

## 3. CLI Tầng Gold (`gold`)

Hỗ trợ đầy đủ `--k3s`, `--master`, `--driver-memory`, `--silver-namespace` (mặc định `silver_spark`), `--core-namespace` (mặc định `gold_spark_core`), `--namespace` (mặc định `gold_spark`).

### 3.1. Chạy Incremental Hàng Ngày (`gold run`)
Đọc `gold_spark_meta.runs` trên Postgres và so với Iceberg snapshot hiện tại của `lake.silver_spark`:
- Nếu không có snapshot mới → trả về **`NO_OP`** trong ~10 giây.
- Nếu có snapshot mới → đọc delta bằng Iceberg `start-snapshot-id`, `MERGE INTO` trên `gold_spark_core` qua WAP branch, dựng lại mart bị ảnh hưởng và chạy Quality Gate (đối soát `award == supplier` lệch `0`).
- Nếu snapshot cũ đã bị expire → tự động fallback sang `rebuild` an toàn.

```bash
# Chạy incremental trên K3s
.venv/bin/gold run --k3s

# Chạy incremental cục bộ
.venv/bin/gold run --master local[4]
```

### 3.2. Rebuild Toàn Bộ 5 Năm (`gold rebuild`)
> **Khuyến nghị tài nguyên:** Với full rebuild toàn bộ 5 năm (`2022-01-01..2026-10-31`), dùng `--master local[4] --driver-memory 4g` chạy ổn định trong ~18 phút (tránh OOMKill ở worker pod khi shuffle nặng bảng `fact_award`). Sau lần rebuild đầu tiên, các lượt `gold run` hàng ngày chạy mượt trên `--k3s`.

```bash
.venv/bin/gold rebuild \
  --master local[4] \
  --driver-memory 4g \
  --start-date 2022-01-01 \
  --end-date 2026-10-31 \
  --namespace gold_spark \
  --core-namespace gold_spark_core
```

### 3.3. Bảo Trì Iceberg Tầng Gold (`gold maintain`)
```bash
.venv/bin/gold maintain --k3s --namespace gold_spark
.venv/bin/gold maintain --k3s --namespace gold_spark_core
```

---

## 4. Luồng Chạy Trọn Vẹn Một Ngày (Daily E2E)

```bash
# 1. Cào Bronze cho ngày hôm qua
.venv/bin/ingest --resource all --date 2026-10-30

# 2. Đồng bộ Silver incremental
.venv/bin/silver run --k3s

# 3. Cập nhật Two-Tier Gold incremental + Quality Gate
.venv/bin/gold run --k3s
```
*(Hoặc bật schedule `bronze_daily_schedule` và `spark_lakehouse_daily_schedule` trên Dagster UI `http://127.0.0.1:3002` để chạy hoàn toàn tự động.)*
