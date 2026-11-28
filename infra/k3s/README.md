# Hướng Dẫn Triển Khai Cụm K3s + Apache Spark 4.0 Multi-Node

File manifest [`spark-cluster.yaml`](./spark-cluster.yaml) triển khai cụm **Apache Spark 4.0 Standalone** trên K3s với thiết kế tối giản, không hardcode IP:

- **`DaemonSet` cho `spark-worker`**: tự động chạy **đúng 1 Spark Worker (`6 cores / 4g RAM`) trên mỗi node K3s** — thêm máy mới là Spark tự nhận thêm worker, không cần `kubectl scale`.
- **`hostNetwork: true` + `dnsPolicy: ClusterFirstWithHostNet`**: bỏ qua độ trễ NAT/VXLAN khi shuffle, đồng thời vẫn phân giải được DNS nội bộ `spark-master.procurement.svc.cluster.local:7077` (không bao giờ phải hardcode IP Master trong YAML).
- **`SPARK_LOCAL_IP` tự động từ `status.hostIP`**: mỗi node tự quảng bá đúng IP của giao diện mạng mà K3s đang chạy (LAN hoặc Tailscale).

> **Lưu ý quan trọng:** Khi dùng Spark trên K3s, **không bật** `--profile spark` trong Docker Compose để tránh trùng cổng `7077` và `8080`.

---

## Chọn Mô Hình Mạng Phù Hợp

| Tiêu chí | Flow 1 — Mạng LAN / Wi-Fi nội bộ | Flow 2 — Mạng Tailscale VPN (Khuyên dùng) |
| :--- | :--- | :--- |
| **Phạm vi hoạt động** | Các máy phải ở **cùng 1 mạng Wi-Fi / LAN** | Chạy **xuyên mạng** (khác Wi-Fi, khác nhà/văn phòng) |
| **Tốc độ shuffle** | Nhanh nhất (trực tiếp qua router LAN) | Rất nhanh khi `direct` (WireGuard P2P) |
| **Độ ổn định IP** | IP LAN có thể đổi khi reboot router | IP `100.x.y.z` **cố định vĩnh viễn** |
| **Bảo mật** | Phải mở port ra LAN (không nên dùng ở Wi-Fi công cộng) | Mã hóa E2E, chỉ máy trong Tailnet truy cập được |
| **Worker trên Windows** | Cần `networkingMode=mirrored` + mở Hyper-V Firewall | Chỉ cài Tailscale + `k3s-agent` **trong WSL2 Ubuntu** |

---

# FLOW 1 — Triển Khai Qua Mạng LAN / Wi-Fi Thông Thường

Giả sử IP LAN máy Master là `192.168.1.11` và máy Worker là `192.168.1.22` *(lấy bằng `ip -4 addr`)*.

## 1.1. Máy Master (Linux)

1. **Cài K3s Server & cấp quyền `kubectl` an toàn (`600`):**
   ```bash
   curl -sfL https://get.k3s.io | sh -

   mkdir -p ~/.kube
   sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config
   sudo chown "$USER:$USER" ~/.kube/config
   chmod 600 ~/.kube/config
   ```
2. **Lấy Node Token:**
   ```bash
   sudo cat /var/lib/rancher/k3s/server/node-token
   ```
3. **Mở firewall `ufw` chỉ cho IP máy Worker trong LAN:**
   ```bash
   sudo ufw allow from 192.168.1.22 proto tcp
   sudo ufw allow from 192.168.1.22 to any port 8472 proto udp
   sudo ufw reload
   ```
4. **Cấu hình `.env` trên Master theo IP LAN:**
   ```ini
   OBJECT_STORAGE_BIND_ADDRESS=0.0.0.0
   ICEBERG_CATALOG_BIND_ADDRESS=0.0.0.0
   OBJECT_STORAGE_ENDPOINT=http://192.168.1.11:8333
   ICEBERG_CATALOG_URI=http://192.168.1.11:8181
   SPARK_K3S_MASTER=spark://192.168.1.11:7077
   SPARK_LOCAL_IP=192.168.1.11
   ```
   Restart lại SeaweedFS:
   ```bash
   docker compose up -d object-storage
   ```

## 1.2. Thêm Máy Worker — Linux (1 dòng lệnh)

```bash
curl -sfL https://get.k3s.io | K3S_URL="https://192.168.1.11:6443" K3S_TOKEN="<TOKEN>" sh -
```

## 1.3. Thêm Máy Worker — Windows 11 (WSL2 Ubuntu)

1. Mở **PowerShell (Admin)** cài WSL2 Ubuntu:
   ```powershell
   wsl --install -d Ubuntu-24.04
   ```
2. Tạo `C:\Users\<TenUser>\.wslconfig` bật mạng `mirrored` & cấp RAM cho Spark:
   ```ini
   [wsl2]
   networkingMode=mirrored
   memory=8GB
   processors=6
   swap=4GB
   ```
3. Mở **Ubuntu (WSL2)** bật `systemd`:
   ```bash
   sudo tee /etc/wsl.conf >/dev/null <<'EOF'
   [boot]
   systemd=true
   EOF
   ```
4. Mở Hyper-V Firewall cho duy nhất IP máy Master & khởi động lại WSL (**PowerShell Admin**):
   ```powershell
   Set-NetFirewallHyperVVMSetting -Name '{40E0AC32-46A5-438A-A0B2-2B479E8F2E90}' -DefaultInboundAction Block
   New-NetFirewallHyperVRule -Name "Allow-K3s-From-Master" -DisplayName "Allow K3s/Spark from Master" -Direction Inbound -VMCreatorId '{40E0AC32-46A5-438A-A0B2-2B479E8F2E90}' -RemoteAddresses 192.168.1.11 -Action Allow
   wsl --shutdown
   ```
5. Mở lại **Ubuntu (WSL2)** và join cụm:
   ```bash
   curl -sfL https://get.k3s.io | K3S_URL="https://192.168.1.11:6443" K3S_TOKEN="<TOKEN>" sh -
   ```

---

# FLOW 2 — Triển Khai Qua Mạng Tailscale VPN (Khuyên Dùng)

Giả sử IP Tailscale máy Master là `100.70.129.72` *(thay bằng kết quả lệnh `tailscale ip -4` thực tế của máy bạn)*.

## 2.1. Máy Master (Linux)

1. **Cài & bật Tailscale:**
   ```bash
   # Arch / CachyOS:
   sudo pacman -S tailscale
   # Ubuntu / Debian:
   # curl -fsSL https://tailscale.com/install.sh | sh

   sudo systemctl enable --now tailscaled
   sudo tailscale up
   tailscale ip -4   # ghi lại MASTER_TS_IP thực tế
   ```
2. **Mở đúng các port K3s + Spark + SeaweedFS trên riêng interface `tailscale0`:**
   ```bash
   sudo ufw allow in on tailscale0 to any port 6443 proto tcp        # K3s API
   sudo ufw allow in on tailscale0 to any port 8472 proto udp        # Flannel VXLAN
   sudo ufw allow in on tailscale0 to any port 10250 proto tcp       # Kubelet
   sudo ufw allow in on tailscale0 to any port 7077,8080 proto tcp   # Spark Master & Web UI
   sudo ufw allow in on tailscale0 to any port 8181,8333 proto tcp   # Iceberg REST & SeaweedFS S3
   sudo ufw reload
   ```
3. **Cài K3s Server gắn trực tiếp vào `tailscale0`:**
   *(Nếu trước đó từng cài K3s bản LAN, chạy `/usr/local/bin/k3s-uninstall.sh` trước.)*
   ```bash
   export MASTER_TS_IP=$(tailscale ip -4)

   curl -sfL https://get.k3s.io | sh -s - server \
     --node-ip="$MASTER_TS_IP" \
     --advertise-address="$MASTER_TS_IP" \
     --bind-address="$MASTER_TS_IP" \
     --flannel-iface=tailscale0 \
     --tls-san="$MASTER_TS_IP"
   ```
4. **Cấp quyền `kubectl` an toàn (`600`) & lấy Token:**
   ```bash
   mkdir -p ~/.kube
   sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config
   sudo chown "$USER:$USER" ~/.kube/config
   chmod 600 ~/.kube/config

   sudo cat /var/lib/rancher/k3s/server/node-token
   ```
   *(Tùy chọn bảo mật cho máy Worker phụ: vào [Tailscale Admin → Settings → Keys](https://login.tailscale.com/admin/settings/keys) tạo 1 **Auth key** `tskey-auth-...` để join mà không cần đăng nhập tài khoản chính.)*
5. **Cấu hình `.env` trên Master theo IP Tailscale (chỉ bind trên `tailscale0`, không lộ ra Wi-Fi công cộng):**
   ```ini
   OBJECT_STORAGE_BIND_ADDRESS=100.70.129.72
   ICEBERG_CATALOG_BIND_ADDRESS=100.70.129.72
   OBJECT_STORAGE_ENDPOINT=http://100.70.129.72:8333
   ICEBERG_CATALOG_URI=http://100.70.129.72:8181
   SPARK_K3S_MASTER=spark://100.70.129.72:7077
   SPARK_LOCAL_IP=100.70.129.72
   ```
   Restart lại SeaweedFS:
   ```bash
   docker compose up -d object-storage
   ```

## 2.2. Thêm Máy Worker — Linux

```bash
# 1. Cài Tailscale & join Tailnet
curl -fsSL https://tailscale.com/install.sh | sh
sudo systemctl enable --now tailscaled
sudo tailscale up --authkey=tskey-auth-xxxxxx   # hoặc bỏ --authkey để đăng nhập trình duyệt

# 2. Join K3s Agent
export MASTER_TS_IP="100.70.129.72"             # IP Tailscale máy Master
export WORKER_TS_IP=$(tailscale ip -4)
export K3S_TOKEN="<TOKEN>"

curl -sfL https://get.k3s.io | K3S_URL="https://${MASTER_TS_IP}:6443" K3S_TOKEN="$K3S_TOKEN" sh -s - agent \
  --node-ip="$WORKER_TS_IP" \
  --flannel-iface=tailscale0
```

## 2.3. Thêm Máy Worker — Windows 11 (chỉ cài trong WSL2 Ubuntu)

> **Lưu ý:** Không cần cài app Tailscale trên Windows cho K3s Worker — cài thẳng trong WSL2 Ubuntu sẽ gọn và tránh xung đột mạng NAT/DNS.

1. **PowerShell (Admin)** cài WSL2 Ubuntu & tạo `C:\Users\<TenUser>\.wslconfig`:
   ```powershell
   wsl --install -d Ubuntu-24.04
   ```
   File `.wslconfig`:
   ```ini
   [wsl2]
   memory=8GB
   processors=6
   swap=4GB
   ```
2. Mở **Ubuntu (WSL2)** bật `systemd`:
   ```bash
   sudo tee /etc/wsl.conf >/dev/null <<'EOF'
   [boot]
   systemd=true
   EOF
   ```
   Đóng Ubuntu → chạy `wsl --shutdown` trong PowerShell → mở lại Ubuntu.
3. Trong **Ubuntu (WSL2)**, cài Tailscale & join K3s Agent (y hệt mục 2.2):
   ```bash
   curl -fsSL https://tailscale.com/install.sh | sh
   sudo systemctl enable --now tailscaled
   sudo tailscale up --authkey=tskey-auth-xxxxxx

   export MASTER_TS_IP="100.70.129.72"
   export WORKER_TS_IP=$(tailscale ip -4)
   export K3S_TOKEN="<TOKEN>"

   curl -sfL https://get.k3s.io | K3S_URL="https://${MASTER_TS_IP}:6443" K3S_TOKEN="$K3S_TOKEN" sh -s - agent \
     --node-ip="$WORKER_TS_IP" \
     --flannel-iface=tailscale0
   ```

---

# Bước Chung — Deploy Cụm Spark & Kiểm Chứng

Chạy tại **thư mục gốc repository** trên máy Master:

```bash
# 1. Xác nhận các node đều Ready với đúng IP (LAN hoặc Tailscale 100.x.y.z)
kubectl get nodes -o wide

# 2. Deploy Spark Master + DaemonSet Spark Worker
kubectl apply -f infra/k3s/spark-cluster.yaml
kubectl get pods -n procurement -o wide

# 3. Kiểm tra Spark Web UI (thấy đủ số Alive Workers):
#    http://<MASTER_IP>:8080

# 4. Chạy thử Silver & Gold incremental trên cụm K3s
.venv/bin/silver run --k3s
.venv/bin/gold run --k3s
```

---

## Xử Lý Sự Cố Thường Gặp (Troubleshooting)

| Triệu chứng | Nguyên nhân | Cách xử lý |
| :--- | :--- | :--- |
| `BindException: Cannot assign requested address` | `SPARK_LOCAL_IP` trong `.env` không khớp IP thực tế của máy | Chạy `tailscale ip -4` (hoặc `ip -4 addr`) và điền đúng vào `SPARK_LOCAL_IP` |
| `Connect to http://127.0.0.1:8181 failed` | SeaweedFS đã bind sang IP Tailscale nhưng `ICEBERG_CATALOG_URI` vẫn là `127.0.0.1` | Đổi cả `ICEBERG_CATALOG_URI` và `OBJECT_STORAGE_ENDPOINT` sang `http://<MASTER_IP>:8181` / `:8333` |
| `Initial job has not accepted any resources` | Pod `spark-worker` chưa ở trạng thái `Running` hoặc vừa OOMKill | Kiểm tra `kubectl get pods -n procurement` và `http://<MASTER_IP>:8080` |
| Shuffle trên Tailscale chậm | Kết nối đang đi qua DERP relay thay vì P2P | Chạy `tailscale status` kiểm tra trạng thái `direct`; nếu bị `relay`, mở port `41641/UDP` trên firewall |
| `Invalid master URL: spark://... :7077` | Dư dấu cách trong `.env` | Xóa khoảng trắng thừa trong `SPARK_K3S_MASTER` |
