param([Parameter(Mandatory=$true)][int]$Port)
$ErrorActionPreference='Stop'
$projectRoot=Split-Path -Parent $PSScriptRoot
$fixture=Get-Content -LiteralPath (Join-Path $projectRoot 'validation\multipart_fixture.json') -Raw -Encoding utf8 | ConvertFrom-Json
$pbiBin='C:\Program Files\Microsoft Power BI Desktop\bin'
[void][Reflection.Assembly]::LoadFrom((Join-Path $pbiBin 'Microsoft.PowerBI.Tabular.dll'))
[void][Reflection.Assembly]::LoadFrom((Join-Path $pbiBin 'Microsoft.PowerBI.Tabular.Json.dll'))
[void][Reflection.Assembly]::LoadFrom((Join-Path $pbiBin 'Microsoft.PowerBI.AdomdClient.dll'))
$server=New-Object Microsoft.AnalysisServices.Tabular.Server
$server.Connect("Data Source=localhost:$Port")
$db=@($server.Databases | Where-Object {$_.Model.Tables.Contains('fact_award_daily')})[0]
if($null -eq $db) {throw 'No project database'}
$expression=$db.Model.Expressions['DataRoot']
$original=$expression.Expression
$evidence=@{status='failed';source_function='ReadParquetTable';fixture=$fixture;restored=$false}
try {
    $expression.Expression='"'+$fixture.root.Replace('\','/')+'" meta [IsParameterQuery=true,Type="Text",IsParameterQueryRequired=true]'
    $db.Model.Tables['dim_date'].RequestRefresh([Microsoft.AnalysisServices.Tabular.RefreshType]::Full)
    [void]$db.Model.SaveChanges()
    $connection=New-Object Microsoft.AnalysisServices.AdomdClient.AdomdConnection("Data Source=localhost:$Port;Initial Catalog=$($db.ID)")
    $connection.Open()
    try {
        $cmd=$connection.CreateCommand();$cmd.CommandText='EVALUATE ROW("rows",COUNTROWS(dim_date),"distinct",DISTINCTCOUNT(dim_date[date_key]))'
        $reader=$cmd.ExecuteReader();[void]$reader.Read()
        $evidence.rows=$reader.GetValue(0);$evidence.distinct_dates=$reader.GetValue(1);$reader.Close()
        if($evidence.rows -ne $fixture.rows -or $evidence.distinct_dates -ne $fixture.rows) {throw 'Multi-part refresh lost or duplicated rows'}
        $evidence.status='passed'
    } finally {$connection.Close()}
} finally {
    $expression.Expression=$original
    $db.Model.Tables['dim_date'].RequestRefresh([Microsoft.AnalysisServices.Tabular.RefreshType]::Full)
    [void]$db.Model.SaveChanges()
    $evidence.restored=($expression.Expression -eq $original)
    $evidence | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $projectRoot 'validation\multipart_refresh_validation.json') -Encoding utf8
    $server.Disconnect()
}
$evidence | ConvertTo-Json -Depth 8
