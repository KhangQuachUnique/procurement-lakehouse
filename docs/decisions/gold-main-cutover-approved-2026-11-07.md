# Main cutover and managed2022–2025 run — owner approved2026-11-07

Owner explicitly approved the presented scope: replace main code with new Gold,
remove old Gold entirely (externalrecoverablearchive), then run official Gold for
2022 through2025 and handle previously settled issues autonomously. This authorises
one managed historical job/publication, not deletion of data/snapshots or arbitrary
RAM increases. No request for parallel agents, none used.

Repositorymain contained unrelateddirtychanges/deletions. Migration compared each
scopedexistingfile to capturedbaseline/rewrite/candidate and refused unknown changes.
269files added/changed/removed,78file removals; beforeZIP byteverified. Legacycache
and remainingemptytrees recoverably moved outside repository. No data/.env archived.
No sourcecatalog rewrite, production pointer replacement or snapshotGC.

MeasuredUFfullruntime fromrewrite plus candidateCLI/Dagster/readermigration are
integrated; raw/geography/valuepolicy remains raw_v1, unchanged. Source mapper hash
matches completedcanonical4383/4383. Newmanagedsource gets fullbulkINSERT using
same originalnarrowkey selection andowner-set semantics. Existing source only3057
publishedcohorts and secondarybulksource were incomplete; neither is falsely sealed
or renamed. CompletecanonicalParquet reused; no repeatedingestion/normalisation.

Small cutover fixes required for same4GB envelope:officialexecute_run remote512MB
instead of doubling local2GBbudget; readbackuses frozenlocalbudget and exact16bucket
multiset comparisons. ManagedSilvernamespace requires explicit opt-in and original
empty-table intent/UUID/snapshot safeguards; defaultisolatedguard remains.
CLI anddefaultDagster use same sharedruntime; dailydefaults2GB/50000rows,no schedule
started. Built wheel has99runtimeassets identicaltosource,0legacyruntime modules;
actualinstalledgoldconsole returns16tables anddefaultDagster loads.

Oldruntimearchive andpassed134Gold/22targeted/reducedrealproofs remain retained.
417integrationtests passed before finalresourcechanges;143Gold/bootstrap and13managed
entrypoint/recovery checks passed after relevantchanges (overlap,do not add counts).
Finalallselectedtest pass count recorded in main-cutoverartifact final-tests.log.

Officialtarget gold_v2_main_2022_2025 uses separateconsistentmanifest. One-shot4GB
cgroup/1CPU/7200s. Data/status not consideredcomplete until COMMITTED_VERIFIED receipt.
Publication and readback are NOT a new independent incremental-vs-full4year oracle.
Missingrealrevisioncase and2026window remain explicit; owner accepted integration
with existing evidence, not a fabricated testPASS. See gold-main-operations.md.

## Source-write operator correction, same approved run

Sourcebulk2GB stopped after4unpartitioned tables. Previouslyapproved3GB completed
notice_reoffer_submission (fifthunpartitionedtable), then notice_revision INSERT
still OOM at2.7GiBengine. preserve_insertion_order=false exact14tablefixture parity
passed but actualpartitionedwrite stillfailed; no successclaimed for that flag.
These failures were Silverpublication,not GoldUF. No Gold computation had started.

Strong local distinction:all5completedtables unpartitioned;remaininglarge root
andobservation tables useidentityDATEpartitioning. A4yearINSERT opensmany physical
partitionwrites;queryspill alone doesnotboundS3/extensionwritebuffers. This is an
execution-path hypothesis,not exactoperatorallocationattribution withoutprofiling.
No Silvergrain/schema/physicalpartitionspec changed, no furtherRAMincrease.

Newsourcewriter emits<=8physicaldatepartitions and<=50000rows perINSERT. Counts
areplanned withnarrowGROUPBY;hot/nullpartition splits bywholeprimarykeygroups via
1024-row streamingpages,never losesduplicate rows. Component/businessgrain isnot
used to define these sourcewritebatches. NonpartitionedalreadyACKedtables skipped.
Eachchunk hasimmutableplan/intent,atomicINSERT,exactmultisetverification,tableprefix
count and durableACK. Unknowncommits require parent-snapshotlineage andboundedexact
recheck ofallprefixchunks;foreign/wrongoutputfailsclosed. No SourceReady untilall
14tablescomplete and original8inputsealerpasses. No Goldreads partialsource.

16targetedtestsPASS,includingfull-loaderintegration,nulls/duplicates,hotpartition,
commitACKloss,resume/noappendrerun,unknownnonempty refusal,unchanged-count corruption
ofearlierprefix andphysicalrowguards. Original425Gold/Silver/orchestration gates
remainrecorded; don't sum overlappingcounts.

Boundedrealoperatorgate runs one largest-rowcount chunk first from FULL staged
source,under4GB processcap,thencontrolledpauseAFTERexactverifiedACK. Six-hundred
secondgate, noGold yet. Must inspectpartition-operator-proof.json plus livecatalog
andjournal before continuing; no additionalwhole-scope retry ifthisgatefails.
All5existingtableACK/liveUUIDs verified. Inputselection/source/policy/Goldlogic
remainunchanged;writer/runnercontrolledhashupgrade documented inoutput
bounded-insert-migration.json. Previousfailedjob markers retained.
