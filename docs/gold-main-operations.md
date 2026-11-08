# Gold chính — compact runtime, 2026-11-07

## Owner-authorized monthly import — 2026-11-08

Owner requested publishing the already-computed7facts/9dimensions after disclosure
of the failed full-history business oracle. This supersedes the earlier no-main-
publication boundary for this import ONLY, not a restart of the cancelled pipeline.
See `docs/decisions/gold-monthly-main-publication-approved-2026-11-08.md`.

Import evidence: `data/gold-main-import-monthly-20261108/`. Only an active complete
manifest plus exact16table snapshot readback and a successful import receipt prove
storage publication. Its status explicitly retains the business-oracle gap; it is
NOT the old pipeline's `COMMITTED_VERIFIED` receipt and does not bootstrap incremental
ledgers. No recurring schedule enabled. Do not resume cancelled main state.

After successful activation, use the existing CLI (no new query engine):

```sh
.venv/bin/silver ui --gold-target gold_v2_main_2022_2025
```

Query `gold.dim_party`, `gold.fact_award_daily`, etc. These views pin a complete
release, not arbitrary current snapshots. Reopen the UI/session to pick up a new
release. Direct `lake.gold_v2_main_2022_2025.*` reads bypass the release boundary.

Initial main catalog was empty, so rollback of this first import deactivates the
new release to *unpublished*, rather than restoring a nonexistent previous Gold.
Operational importer has guarded `--rollback` (only the expected active release),
never deletes files/snapshots. Snapshot tags and immutable manifests preserve this
version for later use. Already-open sessions retain their snapshot-pinned views;
reconnect to observe a changed/deactivated release. Do not run rollback concurrently
with a writer or assume it reverses changes in external downstream systems.

Completed07:02+07: `main_monthly_20261108` ACTIVE, all16pinned snapshots exact0diff
against original Parquet before activation; fresh main reader16views checked and
16snapshot tags verified. Worker131.744s, serviceexit0; journal132.225swall/1.6Gpeak.
See import `receipt.json` and `post-publication-check.json`. Business-oracle gap
and incremental-state limitations above remain unchanged.

## Trạng thái và phạm vi

Owner đã duyệt tích hợp vào main, xóa code Gold cũ có bản lưu ngoài repo, và chạy
Gold chính cho nguồn/ngày nghiệp vụ2022-01-01..2025-12-31. Phê duyệt này không biến
coverage chưa kiểm chứng thành PASS. Bản nhỏ2022..2023-12-28 khớp16bảng giữa full
độc lập và incremental, recovery/no-opPASS; thiếu hồ sơ nhiều revision thực tế.
Lượt chính là một backfill full, không phải lần đối chiếu full-vs-incremental4năm.

Runtime duy nhất:`procurement.gold.v2`, schema7fact+9dimension đã duyệt.
Union-Find chỉ dùng full backfill; incremental vẫn xử lý vùng ảnh hưởng cũ/mới,
bao gồm rút đóng góp, sửa/xóa cạnh và tách/nhập nhóm. Không thay quy tắc nghiệp vụ.
Các lệnh bootstrap/update của Gold cũ đã gỡ. Không dùng gold_reporting/old pointer.

## Luồng chính một lần

1. Đọc Silver Parquet chuẩn hóa đã hoàn tất4383cohort, kiểm tra hash/mapper/phủ ngày,
   không tải lại Bronze hoặc chuẩn hóa lại Silver.
2. Bulk INSERT theo quy tắc cohort cuối/child-owner đã kiểm chứng vào nguồn quản lý
   `silver_v2_main_2022_2025`.14tableACK và empty-table intent cho phép resume;
   không nhận bảng nonempty không có intent và không append lại bảng đãACK.
3. Giữ raw/quarantine3hồ sơ null-root theo policy đã duyệt, không tính vào fact.
   Sealer xác minh nguồn8bảng, UUID/snapshot, đầy đủ nguồn/ngày, lineage và policy.
4. Tạo kế hoạch bất biến cho `gold_v2_main_2022_2025`, state tại
   `data/gold-main-2022-2025/gold-state`, batch50000rows, localDuckDB2GB,
   remote512MB. Cả tiến trình bị chặn ở4,000,000,000bytes,1CPU. Silver sync3GB
   engine chạy trước, không đồng thời với Gold.
5. CLI chính gọi cùng execute_run/GoldService như Dagster, publish manifest nguyên
   tử sau khi đủ16bảng, checkpointaccept. Đọc lại exactmultiset16bảng theo16bucket.
6. Chỉ receipt.json `COMMITTED_VERIFIED` mới chứng minh bản chính hoàn tất. RUNNING
   phải đối chiếu systemd; process dừng không được gọi là vẫn chạy. Lỗi không tự
   lặp toàn bộ, tăngRAM, thay grain, xóa snapshot hoặc nới gate.

```sh
.venv/bin/gold materialize-history \
  '/home/soul/Dự án/procurement/procurement-gold-v2/data/gold-v2-acceptance/full-history-canonical-v1' \
  data/gold-main-2022-2025 --allow-managed-publication
.venv/bin/gold status main_backfill_2022_2025 data/gold-main-2022-2025/gold-state
```

Luồng one-shot được chạy bằng systemd unit`procurement-gold-main-2022-2025-silver3gb-20261107`;
không tự bật daily schedule. Cap7200s cho lượt chính được duyệt này, không hồi sinh
bộ điều phối test/nguồn cũ. Xemlaunch.json,job.json,pipeline-plan.json trongoutput.
Không chạy lại lệnh nếu owner còn active. Sau lỗi kiểm tra bảngACK/transaction,
phase và codehash trước khi resume cùng kế hoạch. Goldcore/selection/policy/runner
được đóng băng trong lúc job chạy.

## Khai thác sau khi hoàn tất

```sh
.venv/bin/silver ui --gold-target gold_v2_main_2022_2025
```

Reader dùng manifest+snapshot cố định của target, không đọc16bảng current giữa
các lần commit. Fact cógrainkhác nhau; khôngJOINtrực tiếp hai fact gây nhân tiền.
Không sửa catalog metadata để đổi tên test thànhproduction. Bảng Silver/Gold cũ
và snapshot cũ vẫn nguyên vẹn. Target chính mới cócontrolnamespace riêng.

Incremental tương lai phải dùng cùng Silvernamespace, Goldtarget, state, cửa sổ
nghiệp vụ, policy và report chứng minh phủ nguồn không bịhổng. Chưa tự mở sang2026:
đây vẫn là gap riêng, không được che bởi kết quả2022–2025. Không bật lịchđịnhkỳ
trước khi chốt đúng nguồn/ngày tiếp theo và quản trịsinglewriter.

## Code cleanup / rollback

Codelegacy/script/test chỉ phục vụruntimecũ đã loại khỏi cây chạy, bản lưu cóhash:
`/home/soul/Dự án/procurement/gold-retirement-artifacts/main-cutover-20261107/before.zip`.
`plan.json`/`applied.json` ghi từng file trước/sau; residuallegacycache cũng chuyển
ngoài repo. README được ghép riêng, giữ mọi thay đổi không liên quan.
Khôi phục code chỉ sau khi dừng owner an toàn, đối chiếu thay đổi mới của người dùng;
không khôi phục đè cảrepo hoặc resetgit. Runtimecũ không được mở stateV2.


## Memory-only Silver resume

Initial main owner stopped at bulknotice_reoffer_submission under2GB engine;
4/14tables had durableACKs. Catalogpreflight verified all4exactUUID/snapshotmatches,
pendingintent current_snapshotNULL. No Goldcomputation started. Silver source-only
engine now uses previouslyapproved3GB; wholeprocess4GB unchanged. Gold still uses
local2GB+remote512MB. Runnerhash migration explicitly recorded, oldplan/job retained
in output/silver-memory-resume.json. Source/writer/views/policy/Goldlogic unchanged;
no duplicate INSERT of ACKedtables. Remaining runtime6960s,not fresh2hours.
Recoverytool tests initially hit /tmpfree<5GiB guard; same21tests pass on realSSD
via explicit --basetemp, no diskguard waived. No source/core changes for this fix.


## CURRENT: bounded physical-partition INSERT, real gate PASS

Currentowner:`procurement-gold-main-2022-2025-partitioned-20261107.service`;
watcher resolves output/active-owner.json, not hard-coded old failedunits.
Four-year singleINSERT into date-partitionednotice_revision still OOMat3GBengine;
physicalinsertionorder flag alone didnotfix. FiveunpartitionedtableACKs retained.
Partition specification remains identityDATE; no schema/grain/sourcechange.

Writer now groups<=8physicaldates and<=50000rows peratomicINSERT. Hotdate/null
partition useswholePK keyset batches. Chunks have exactmultisetverification,
parent-snapshotintent/ACK andprefixcount; uncertainACKloss rechecks allprefixchunks
exactly in boundedunits,not counts-only. All14tables mustfinishbeforeSourceReady.
`silver-table-acks/<table>-chunks/plan.json` andnumberedACKs give actualprogress;
`newly_committed` counters refer only to currentinvocation, not alloldACKs.

Realoperatorgate PASS from FULLsource:notice_revision total557218rows/148batches,
largest9046rows batch covers2024-11-22..29. Commit/exactcheck/ACK liveUUID+snapshot
verified; cgrouppeak3,502,661,632bytes; peakRSS3,295,760KiB. ControlledpauseafterACK,
not an error. Sixteenboundedwriter tests PASS including duplicate/null/hotkeys,
ACKloss/noappendrerun/full-loadercallbackintegration/oldprefixcorruption detection.

Ownerresumed samefrozenplan after gate,6600sremaining inclusivepipelineguard; nofresh
2hbudget. FivefulltableACKs+firstnoticechunkskipped,147noticechunksremain atstart.
Goldruntime stilllocal2GB/remote512MB,wholeprocess4GB,1CPU. No Gold computation
untilSourceReady. Tests/gates not proof the complete2022–2025 run alreadyfinished.
See output/partitioned-launch.json andpartition-operator-proof.json.


## Current live memory-high correction

Actual PID1983398 was D/mem_cgroup_handle_over_high,3.576GBcurrent versus3.5GBsoft
threshold,despite4GBhardcap.677259highevents and~39percentfullmemorypressure;
thiswas a measuredthrottlingconfigurationerror,not proof CPUcomputation needed45min.
Applied runtime-only MemoryHigh3.9GB,MemoryMaxstill4GB,PIDunchanged andobservedR.
No core/plan/source/table changes or rerun. Exactoverrideproof in mainoutput
memory-high-adjustment.json;retainoriginal launch.json as historical evidence.
Any authorised future resume must merge thisoverride rather than blindly useold
3.5GBhigh setting. No fullpipeline speedup/ETA claimed from one scheduling sample.


## Measured ETA and bounded runtime-budget correction

Observationrecentchunks12–19s,median14.62s;at8/184remaining~40–50min for this table
only. OtherSilverandfull4yrGoldarenotyetmeasured;wholefinishrough2–3hremaining,
not promisedETA. OriginalcurrentownerRuntimeMax6600s hadonly~1hleft,insufficient
forobservationaloneplusotherphases. Sinceowner authorisedfullrun withoutafixedtime
limit,correctedoperator-timebudget to14400sTOTALcurrentowner (4h),notunbounded.
set-propertyunsupported;mergeddistinctruntime-only90-measured-completion-budget.conf
anddaemonreload;verifiedRuntimeMax4h,PID1983398unchanged,MemoryMax4GBunchanged,
MemoryHigh3.9GBunchanged. No jobrestart/source reload. Exactproof/dropinpath recorded
inmainoutput/runtime-estimate-adjustment.json. Previouslaunchcaps arehistorical.

## CURRENT 19:48+07: Gold-only fresh-process resume after kernel kill

Silver finished all14tables and sealed full2022–2025 receipt before Gold started.
Previous PID1983398 was killed19:37:59 at cgroup MemoryPeak3999997952bytes;
kernel CONSTRAINT_MEMCG, systemd oom-kill. No evidence Union-Find failed: read-only
state inspection showed PLANNED, only two durable captures (package_revision2278458,
notice_revision557218), no phase checkpoint. Prior job.json RUNNING was stale.
Root cause beyond enforced4GB is not yet proven (retained Silver heap vs remote
Iceberg/Arrow capture allocations); a new process is NOT proof it is solved.

Owner explicitly approved Gold-only retry. Current owner:
`procurement-gold-main-2022-2025-goldonly-20261107.service`, PID2343669 at launch
19:48:44. New process keeps4GB hardcap/3.9GB high/256MiBswap/1CPU,7000sremaining
of previous4h operatorbudget. Same installed CLI materialize-history sees existing
validated silver-ready.json and skips ENTIRE Silver branch; only existing Gold
state resume, final16table exact verification and official receipt remain. No
source normalization, view preparation or INSERT replay. Two captures are reused.
All8sourceUUID/snapshotpins verified retained; source/runner/writer/corehashes
unchanged, plan/source equality and durable capture counts verified read-only.
See gold-only-resume-preflight.json, gold-only-launch.json and gold-pre-resume-oom.json.
Live log gold-only-execution.log; active-owner.json and existing completionwatcher
93e05917-3232-4143-ba96-b6ca214e8b87 rearmed for this owner. Cleanup gate remains
blocked whileRUNNING and until COMMITTED_VERIFIEDfullscope16tablechecks. Do not
restart previous failed owners or claim capture/publish success from live PID alone.

## CURRENT override: owner cancelled main, fact-by-month spike finished

Owner cancelled Gold-only owner; MainPID0 verified, mainjobCANCELLED_BY_OWNER,
no Gold release completed. Do not resume old fullpipeline implicitly. Three committed
captures retained. Completionwatcher93e05917 disabled intentionally. Owner separately
approved narrow pre-test obsolete Gold local cleanup; actual12.15GBfree reclaimed,
no S3/Silver/main/reference-state deletion. Fullpostrun cleanup gate still applies to
remaining resources, not bypassed by successful one-fact experiment.

Owner's one-fact/month idea implemented ONLY in isolated spike (no frozen runtime edits).
Full2022–2025fact_notice_daily trial VERIFIED:49calendar/null parts,557218source notices,
33933publicrows,0exact differences from independent full narrow query, ParquetreadbackPASS,
real exit/resumePASS;44.777s end-to-end,730566656byte whole-cgroup peak,0OOM.
No speedup multiple/general all-fact proof. See docs/reports/gold-one-fact-monthly-spike-2026-11-07.md.
