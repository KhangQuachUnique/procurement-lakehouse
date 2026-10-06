param(
    [ValidateSet('up', 'down', 'status', 'logs', 'smoke', 'build')]
    [string]$Action = 'status'
)
$ErrorActionPreference = 'Stop'
$composeArgs = @('compose', '--project-name', 'procurement-refactor-test',
    '--env-file', (Join-Path $PSScriptRoot 'empty.env'),
    '--file', (Join-Path $PSScriptRoot 'compose.yaml'))
switch ($Action) {
    'up' { & docker @composeArgs up -d --build --wait --wait-timeout 180 }
    'down' { & docker @composeArgs down }
    'status' { & docker @composeArgs ps -a }
    'logs' { & docker @composeArgs logs --tail 100 }
    'build' { & docker @composeArgs build }
    'smoke' {
        & docker @composeArgs exec -T dagster-code-location python /opt/sandbox/smoke.py --url http://dagster-webserver:3000
    }
}
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
