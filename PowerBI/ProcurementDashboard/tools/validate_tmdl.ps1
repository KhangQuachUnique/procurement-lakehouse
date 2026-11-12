$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$pbiBin = 'C:\Program Files\Microsoft Power BI Desktop\bin'
[void][Reflection.Assembly]::LoadFrom((Join-Path $pbiBin 'Microsoft.PowerBI.Tabular.dll'))
[void][Reflection.Assembly]::LoadFrom((Join-Path $pbiBin 'Microsoft.PowerBI.Tabular.Json.dll'))
try {
    $database = [Microsoft.AnalysisServices.Tabular.TmdlSerializer]::DeserializeDatabaseFromFolder((Join-Path $projectRoot 'ProcurementDashboard.SemanticModel\definition'))
    $inventory=Get-Content -LiteralPath (Join-Path $projectRoot 'validation\model_inventory.json') -Raw -Encoding utf8 | ConvertFrom-Json
    foreach($tableEntry in $inventory.tables.PSObject.Properties) {
        if(-not $database.Model.Tables.Contains($tableEntry.Name)) {throw "Missing actual TMDL table: $($tableEntry.Name)"}
        foreach($columnEntry in $tableEntry.Value.columns.PSObject.Properties) {
            if(-not $database.Model.Tables[$tableEntry.Name].Columns.Contains($columnEntry.Name)) {throw "Missing actual TMDL column: $($columnEntry.Name)"}
        }
    }
    if($database.Model.Tables['_Measures'].Measures.Count -ne @($inventory.measures.PSObject.Properties).Count) {throw 'TMDL/inventory measure count mismatch'}
    foreach($measureEntry in $inventory.measures.PSObject.Properties) {
        if(-not $database.Model.Tables['_Measures'].Measures.Contains($measureEntry.Name)) {throw "Missing actual TMDL measure: $($measureEntry.Name)"}
    }
    $tables = @($database.Model.Tables | ForEach-Object { @{name=$_.Name; columns=$_.Columns.Count; measures=$_.Measures.Count; partitions=$_.Partitions.Count} })
    $result = @{status='passed'; parser='Microsoft.AnalysisServices.Tabular.TmdlSerializer (installed Desktop)'; tables=$tables; relationships=$database.Model.Relationships.Count; compatibilityLevel=$database.CompatibilityLevel; dateTableMarked=($database.Model.Tables['dim_date'].DataCategory -eq 'Time'); note='Parsing confirms syntax/metadata and inventory references, not DAX or Power Query execution.'}
    $result | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $projectRoot 'validation\tmdl_validation.json') -Encoding utf8
    $result | ConvertTo-Json -Depth 8
} catch {
    @{status='failed'; error=$_.Exception.ToString()} | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $projectRoot 'validation\tmdl_validation.json') -Encoding utf8
    throw
}
