# Gold compact schema — confirmed target

Date: 2026-11-06. Status: schema target Accepted; implementation NOT accepted/deployed.

User confirmation: “cái chema zip là cái tôi chốt á”. Applies to the attached
`gold-star-schemas-compact.zip`, not earlier proposed/full diagrams.

Canonical retained artifact: `../schemas/gold-star-schemas-compact-approved-2026-11-06.drawio`.
Uncompressed SHA-256: `862aba6eabdbedd764b639fe2a8bfb45300dd856be699283a1ab5df94799a25d`.

Seven facts: demand, notice, award, savings, duration, supplier, status daily.
Nine dimensions: date, geography, sector, procurement method, value band, currency,
party, duration metric, progress status. Repeated dimensions on the seven pages
represent shared tables, not separate physical copies.

This supersedes the seven-dimension public column/grain layout in
`gold-reporting-incremental.md` for future schema implementation only. Existing
active Gold must remain readable until a validated replacement is activated.
Retraction/replay, snapshot-consistent publication, source traceability,
currency separation where applicable, and sum/denominator semantics remain.
Do not add/remove target columns without another explicit schema decision.

Logical FK uniqueness and null-key conventions need implementation contracts;
they do not authorize altering the approved diagram. Migration/deployment and
large data runs are not authorized by this schema confirmation alone.

## Incremental grain clarification — 2026-11-07

User agreed to the explanation that fact grain stays fixed by schema version,
while incremental may replace measures/contributions, move an owner's contribution
between old/new groups and remove empty groups. Six event facts may be restated
for source corrections. Status records actual observation days; routine incremental
does not overwrite past observation snapshots. Final execution plan is recorded in
`../gold-v2-execution-plan.md`; this clarification is not production rollout consent.
