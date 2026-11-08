"""Read-only terminal notice: no retries, no catalog writes, no model polling."""
import json
import subprocess
import sys
from pathlib import Path

root=Path(sys.argv[1])
unit='procurement-gold-main-2022-2025-stream-20261107.service'
owner=root/'active-owner.json'
if owner.exists():
 import re
 unit=json.loads(owner.read_text())['unit']
 if not re.fullmatch(r'procurement-gold-main-[a-z0-9-]+\.service',unit):
  raise ValueError('Invalid managed owner identity')
props=subprocess.run(['systemctl','--user','show',unit,'-p','ActiveState','-p','MainPID','-p','Result'],capture_output=True,text=True,check=True,timeout=10)
p=dict(line.split('=',1) for line in props.stdout.splitlines() if '=' in line)
running=p.get('ActiveState')=='active' and p.get('MainPID') not in (None,'0')
path=root/'job.json'
x=json.loads(path.read_text()) if path.exists() else {}
status=x.get('status','STARTING' if running else 'STOPPED')
if status=='RUNNING' and not running:status='STOPPED_UNFINISHED'
# A newly resumed owner can briefly see its predecessor's terminal marker.
if running and status not in ('RUNNING','STARTING'):
 status='STARTING'
if status=='SOURCE_CHUNK_GATE' and owner.exists() and json.loads(owner.read_text()).get('kind')!='bounded_source_operator_gate':
 status='STOPPED_UNFINISHED'
done=not running and status not in ('RUNNING','STARTING')
if '--notify' in sys.argv:
 if status=='COMMITTED_VERIFIED':
  print('Gold chính 2022–hết2025 đã COMMITTED và đọc lại khớp đủ16bảng. Đích gold_v2_main_2022_2025; code Gold cũ đã gỡ. Chưa bật lịch định kỳ. Đây là backfill chính, không phải đối chiếu incremental/full độc lập4năm. Bằng chứng: '+str(root/'receipt.json'))
 else:
  print(f"Luồng Gold chính2022–2025 đã dừng: {status}, bước{x.get('current','startup')}, lỗi{x.get('error_type') or p.get('Result','xem log')}. Không coi là hoàn tất hoặc tự chạy lại toàn bộ. Checkpoint được giữ; bằng chứng: {path}")
else:
 print(json.dumps({'done':done,'status':status,'current':x.get('current'),'fingerprint':status+':'+str(x.get('current')),'job_file':str(path)}))
