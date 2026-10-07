# M?i tr??ng refactor ??c l?p

Worktree: `C:/Work/DataProject/procurement_lakehouse/.worktrees/refactor-test`

Nh?nh: `refactor/isolated-core`, b?t ??u t? checkpoint `f98e218`.
S?a code trong worktree n?y. Th? m?c source ch?nh v? stack hi?n t?i kh?ng ???c mount v?o sandbox.

## Ch?y tr?n PowerShell

```powershell
cd C:\Work\DataProject\procurement_lakehouse\.worktrees\refactor-test
powershell -ExecutionPolicy Bypass -File .\infra\sandbox\sandbox.ps1 up
```

L?n ??u c?n Docker Desktop ?ang ch?y v?i Linux containers v? m?ng ?? t?i image/dependencies.
L?nh `up` build l?i image t? source trong worktree r?i ch? d?ch v? s?n s?ng.

- Dagster: http://127.0.0.1:23000
- S3: http://127.0.0.1:28333
- PostgreSQL ?ng d?ng: localhost:25432, database/user `procurement`, password `sandbox-app-only`.
- S3 bucket: `procurement-refactor-test`, access key `sandbox-access`, secret `sandbox-secret-only`.
- Credentials n?y ch? d?nh cho sandbox c?c b?, kh?ng d?ng cho production.

```powershell
# Tr?ng th?i t?t c? container
powershell -ExecutionPolicy Bypass -File .\infra\sandbox\sandbox.ps1 status
# Ch?y th? qua h?ng ??i + daemon + worker Dagster
powershell -ExecutionPolicy Bypass -File .\infra\sandbox\sandbox.ps1 smoke
# Log g?n nh?t
powershell -ExecutionPolicy Bypass -File .\infra\sandbox\sandbox.ps1 logs
# Sau khi s?a source: build v? c?p nh?t container test
powershell -ExecutionPolicy Bypass -File .\infra\sandbox\sandbox.ps1 up
# D?ng stack test, gi? d? li?u trong volume
powershell -ExecutionPolicy Bypass -File .\infra\sandbox\sandbox.ps1 down
```

Smoke test materialize ng?y `2025-01-01` cho n?m resource b?ng API gi? l?p: project c?
m?t record, c?c resource c?n l?i r?ng. Ch?y l?n n?a ki?m tra reuse. C? th? ch?n
`bronze_project` v? ng?y ?? trong UI ?? materialize th? c?ng. Schedule/sensor m?c
??nh t?t; kh?ng c?n b?t ?? ki?m th? th? c?ng.

## Ranh gi?i c?ch ly

Compose project c? ??nh: `procurement-refactor-test`; m?i volume/network ???c
Compose ??t t?n theo project n?y. Kh?ng c? bind mount, external volume/network,
Docker socket hay credentials c?a m?i tr??ng ch?nh. Worker, daemon v? fixture ch? d?ng network `internal`, kh?ng c? ???ng ra internet.
Webserver, S3 v? PostgreSQL ?ng d?ng th?m network `access` ri?ng ?? m? c?ng localhost.
Ngu?n d? li?u c?a worker l? fixture n?i b?. Build v?n c?n m?ng.
Source v? c?u h?nh ???c COPY v?o image ri?ng `procurement-refactor-test:local`.
C?c c?ng ch? bind localhost. Container c? gi?i h?n CPU/RAM; v?n d?ng chung Docker
engine, CPU, RAM v? ? ??a c?a m?y.

Kh?ng ch?y `docker system prune`, `docker volume prune` ho?c d?n to?n b? Docker.
L?nh `down` trong script ch? t?c ??ng project test v? kh?ng x?a volume.

## Tr?ng th?i ki?n tr?c

Stack ch?y code checkpoint hi?n t?i, ch?a chuy?n metadata nghi?p v? sang PostgreSQL.
`app-postgres` l? DB ri?ng ?? chu?n b? cho refactor; hi?n ch?a c? b?ng nghi?p v?.
`APP_DATABASE_URL` ch? l? bi?n ?? chu?n b?, code hi?n t?i ch?a s? d?ng.
Dagster s? d?ng `dagster-postgres` ri?ng. Bronze hi?n v?n t?o manifest/error tr?n
object storage sandbox theo logic c?. Vi?c chuy?n attempt/commit/errors sang DB l?
b??c ti?p theo, kh?ng ph?i thay ??i ?? ho?n th?nh trong l?n d?ng m?i tr??ng n?y.

## L?nh Compose t??ng ???ng

```powershell
docker compose -p procurement-refactor-test --env-file infra/sandbox/empty.env -f infra/sandbox/compose.yaml ps -a
```

Lu?n ch? ??nh project, env-file v? compose file n?y; kh?ng gh?p overlay production.
