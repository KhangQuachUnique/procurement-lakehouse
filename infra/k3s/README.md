# Triển khai Multi-node Spark trên K3s (Chuẩn thực chiến, ít bước nhất)

File [`spark-cluster.yaml`](./spark-cluster.yaml) đã được kiểm chứng thực tế trên cụm 2 máy (Linux + Windows 11 WSL2) và tích hợp sẵn:
- **`DaemonSet`** cho `spark-worker`: tự động chạy **đúng 1 worker (`6 cores / 4g RAM`) trên mỗi máy vật lý** (không cần `kubectl scale`).
- **`hostNetwork: true` + `SPARK_LOCAL_IP` (`status.hostIP`)**: chạy thẳng qua mạng LAN/WiFi, bỏ qua lỗi VXLAN trên Windows WSL2, không cần `kubectl port-forward`.
- **`enableServiceLinks: false` + `strategy: Recreate`**: tránh xung đột biến `SPARK_MASTER_PORT` của K8s và tránh kẹt port khi restart Master.

> **Lưu ý:** Khi dùng K3s Spark, **không bật** `--profile spark` trong Docker Compose trên máy chủ (chỉ chạy `docker compose up -d` cho SeaweedFS + Postgres + Dagster) để tránh tranh port `7077`/`8080`.

---

## 1. Máy chủ (PC #1 — Linux)
1. Mở SeaweedFS & Iceberg REST cho mạng LAN trong `.env`:
   ```env
   OBJECT_STORAGE_BIND_ADDRESS=0.0.0.0
   ICEBERG_CATALOG_BIND_ADDRESS=0.0.0.0
   ```
   rồi chạy `docker compose up -d object-storage`.
2. Đảm bảo `SPARK_MASTER_HOST` trong [`spark-cluster.yaml`](./spark-cluster.yaml) đúng IP LAN của máy chủ (mặc định `192.168.1.11`).
3. Cài K3s Server, lấy token và triển khai Spark:
   ```bash
   curl -sfL https://get.k3s.io | sh -
   sudo cat /var/lib/rancher/k3s/server/node-token
   sudo kubectl apply -f infra/k3s/spark-cluster.yaml
   ```
4. Khi thêm bất kỳ máy worker nào có IP `<IP_WORKER>`, mở firewall `ufw` chỉ riêng cho IP đó (tránh bị chặn port động của Spark Driver/Shuffle):
   ```bash
   sudo ufw allow from <IP_WORKER> proto tcp
   ```

---

## 2. Thêm máy Worker mới — **Linux** (1 dòng lệnh, 30 giây)
Không cần cài Docker, Python, Java hay clone repo:
```bash
curl -sfL https://get.k3s.io | K3S_URL=https://<IP_MAY_CHU>:6443 K3S_TOKEN=<TOKEN> sh -
```

---

## 3. Thêm máy Worker mới — **Windows 11** (WSL2 Ubuntu, không cần Docker Desktop)
1. Mở **PowerShell (Admin)** bật Hypervisor & cài Ubuntu WSL2:
   ```powershell
   bcdedit /set hypervisorlaunchtype auto
   wsl --install -d Ubuntu
   ```
   *(Khởi động lại máy nếu Windows yêu cầu, mở app Ubuntu tạo user/password.)*
2. Tạo `C:\Users\<TênUser>\.wslconfig` bật mạng `mirrored` (PowerShell):
   ```powershell
   Set-Content -Path "$env:USERPROFILE\.wslconfig" -Value "[wsl2]`nnetworkingMode=mirrored`nmemory=6GB`nprocessors=6"
   ```
3. Bật `systemd` trong **Ubuntu**:
   ```bash
   sudo tee /etc/wsl.conf << 'EOF'
   [boot]
   systemd=true
   EOF
   ```
4. Mở Hyper-V Firewall cho duy nhất máy chủ & khởi động lại WSL (**PowerShell Admin**):
   ```powershell
   Set-NetFirewallHyperVVMSetting -Name '{40E0AC32-46A5-438A-A0B2-2B479E8F2E90}' -DefaultInboundAction Block; New-NetFirewallHyperVRule -Name "Allow-K3s-From-PC1" -DisplayName "Allow K3s/Spark from PC1" -Direction Inbound -VMCreatorId '{40E0AC32-46A5-438A-A0B2-2B479E8F2E90}' -RemoteAddresses <IP_MAY_CHU> -Action Allow; wsl --shutdown
   ```
5. Mở lại **Ubuntu** và join vào cụm (1 dòng):
   ```bash
   curl -sfL https://get.k3s.io | K3S_URL=https://<IP_MAY_CHU>:6443 K3S_TOKEN=<TOKEN> sh -
   ```

---

## 4. Kiểm tra & Chạy job
- Xem danh sách máy & Pod trên máy chủ:
  ```bash
  sudo kubectl get nodes -o wide
  sudo kubectl -n procurement get pods -o wide
  ```
- Mở Spark Master Web UI trực tiếp tại **`http://127.0.0.1:8080`** (mỗi máy mới join sẽ tự hiện thêm 1 worker `ALIVE` nhờ `DaemonSet`).
- Chạy Silver Spark job bằng `.env`:
  ```env
  SPARK_MASTER=spark://<IP_MAY_CHU>:7077
  OBJECT_STORAGE_ENDPOINT=http://<IP_MAY_CHU>:8333
  ICEBERG_CATALOG_URI=http://<IP_MAY_CHU>:8181
  ```
