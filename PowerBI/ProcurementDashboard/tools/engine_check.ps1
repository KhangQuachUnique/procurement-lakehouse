param([Parameter(Mandatory=$true)][int]$Port,[switch]$Refresh,[string]$RefreshTable,[string]$QueriesFile)
$ErrorActionPreference='Stop'
$projectRoot=Split-Path -Parent $PSScriptRoot
$pbiBin='C:\Program Files\Microsoft Power BI Desktop\bin'
[void][Reflection.Assembly]::LoadFrom((Join-Path $pbiBin 'Microsoft.PowerBI.Tabular.dll'))
[void][Reflection.Assembly]::LoadFrom((Join-Path $pbiBin 'Microsoft.PowerBI.Tabular.Json.dll'))
$server=New-Object Microsoft.AnalysisServices.Tabular.Server
$server.Connect("Data Source=localhost:$Port")
try {
    $db=@($server.Databases | Where-Object { $_.Model.Tables.Contains('fact_award_daily') })
    if($db.Count -ne 1) {
        @($server.Databases | ForEach-Object { @{name=$_.Name;id=$_.ID;tables=$_.Model.Tables.Count} }) | ConvertTo-Json -Depth 5
        throw 'Expected one ProcurementDashboard database'
    }
    $database=$db[0]
    if($Refresh) {
        $database.Model.RequestRefresh([Microsoft.AnalysisServices.Tabular.RefreshType]::Full)
        $database.Model.SaveChanges()
    }
    if($RefreshTable) {
        if(-not $database.Model.Tables.Contains($RefreshTable)) { throw 'Unknown table' }
        $database.Model.Tables[$RefreshTable].RequestRefresh([Microsoft.AnalysisServices.Tabular.RefreshType]::Full)
        $database.Model.SaveChanges()
    }
    $result=@{database=$database.Name;id=$database.ID;port=$Port;tables=@($database.Model.Tables | ForEach-Object { @{name=$_.Name;partitions=@($_.Partitions | ForEach-Object { @{name=$_.Name;state=$_.State.ToString();error=$_.ErrorMessage} });measures=@($_.Measures | Where-Object {$_.State -ne 'Ready'} | ForEach-Object { @{name=$_.Name;state=$_.State.ToString();error=$_.ErrorMessage} })} })}
    $result | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath (Join-Path $projectRoot 'validation\engine_state.json') -Encoding utf8
    @{database=$database.Name;port=$Port;states=@($result.tables | ForEach-Object { $_.partitions.state } | Group-Object | Select-Object Name,Count)} | ConvertTo-Json -Depth 6
    if($QueriesFile) {
        [void][Reflection.Assembly]::LoadFrom((Join-Path $pbiBin 'Microsoft.PowerBI.AdomdClient.dll'))
        $connection=New-Object Microsoft.AnalysisServices.AdomdClient.AdomdConnection("Data Source=localhost:$Port;Initial Catalog=$($database.ID)")
        $connection.Open()
        try {
            $queries=Get-Content -LiteralPath $QueriesFile -Raw -Encoding utf8 | ConvertFrom-Json
            $results=@()
            foreach($query in $queries) {
                $cmd=$connection.CreateCommand(); $cmd.CommandText=$query.dax; $cmd.CommandTimeout=120
                try {
                    $reader=$cmd.ExecuteReader(); $rows=@()
                    while($reader.Read()) {
                        $row=@{}
                        for($i=0;$i -lt $reader.FieldCount;$i++) { $val=$reader.GetValue($i); if($val -is [DBNull]) {$val=$null}; $row[$reader.GetName($i)]=$val }
                        $rows+=,$row
                    }
                    $reader.Close()
                    $results+=@{name=$query.name;status='passed';rows=$rows}
                } catch { $results+=@{name=$query.name;status='failed';error=$_.Exception.ToString()} }
            }
            $results | ConvertTo-Json -Depth 15 | Set-Content -LiteralPath (Join-Path $projectRoot 'validation\dax_results.json') -Encoding utf8
            @{queries=$results.Count;failed=@($results | Where-Object { $_.status -eq 'failed' })} | ConvertTo-Json -Depth 6
        } finally { $connection.Close() }
    }
} finally { $server.Disconnect() }
