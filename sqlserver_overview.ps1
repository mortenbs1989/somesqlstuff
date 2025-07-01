# Requires: PowerShell 5+ and the SqlServer module
Import-Module SqlServer -ErrorAction Stop
$outputpath = "C:\SQLServer_FullInventory.csv"

# Define registry paths to check for installed SQL Server instances
$regPaths = @(
    "HKLM:\SOFTWARE\Microsoft\Microsoft SQL Server\Instance Names\SQL",
    "HKLM:\SOFTWARE\WOW6432Node\Microsoft\Microsoft SQL Server\Instance Names\SQL"
)

# Get list of instance names
$instances = @()
foreach ($path in $regPaths) {
    if (Test-Path $path) {
        $key = Get-ItemProperty -Path $path
        $values = $key.PSObject.Properties | Where-Object {
            $_.Name -notin 'PSPath', 'PSParentPath', 'PSChildName', 'PSDrive', 'PSProvider'
        } | Select-Object -ExpandProperty Value

        $instances += $values
    }
}

# Deduplicate and simplify instance names
$instances = $instances | Where-Object { $_ } | Sort-Object -Unique
$instancesShort = $instances | ForEach-Object {
    if ($_ -match '\.(.+)$') { $matches[1] } else { $_ }
}

# Prepare result collection
$results = @()

foreach ($instName in $instancesShort) {
    try {
        $fullInstanceName = if ($instName -eq "MSSQLSERVER") {
            $env:COMPUTERNAME
        }
        else {
            "$env:COMPUTERNAME\$instName"
        }

        Write-Host "Collecting data from $fullInstanceName..."

        # Connect to SQL Server
        $server = New-Object Microsoft.SqlServer.Management.Smo.Server $fullInstanceName
        $majorVersion = $server.Version.Major

        # Get list of user databases and their size
        $dbs = $server.Databases | Where-Object { -not $_.IsSystemObject }
        $dbCount = $dbs.Count
        $dbSizeMB = ($dbs | Measure-Object -Property Size -Sum).Sum

        # Query master for user objects
        $usp = Invoke-Sqlcmd -ServerInstance $fullInstanceName -Database "master" -Query @"
SELECT COUNT(*) AS USPinMaster FROM sys.objects WHERE type = 'P' AND is_ms_shipped = 0
"@ -TrustServerCertificate

        $usf = Invoke-Sqlcmd -ServerInstance $fullInstanceName -Database "master" -Query @"
SELECT COUNT(*) AS USFinMaster FROM sys.objects WHERE type IN ('FN', 'FS', 'IF') AND is_ms_shipped = 0
"@ -TrustServerCertificate

        $utf = Invoke-Sqlcmd -ServerInstance $fullInstanceName -Database "master" -Query @"
SELECT COUNT(*) AS UTFinMaster FROM sys.objects WHERE type IN ('TF', 'FT') AND is_ms_shipped = 0
"@ -TrustServerCertificate

        $uv = Invoke-Sqlcmd -ServerInstance $fullInstanceName -Database "master" -Query @"
SELECT COUNT(*) AS UVinMaster FROM sys.objects WHERE type = 'V' AND is_ms_shipped = 0
"@ -TrustServerCertificate

        $ut = Invoke-Sqlcmd -ServerInstance $fullInstanceName -Database "master" -Query @"
SELECT COUNT(*) AS UTinMaster FROM sys.tables WHERE is_ms_shipped = 0
"@ -TrustServerCertificate

        # Linked servers and extended events
        $ls = Invoke-Sqlcmd -ServerInstance $fullInstanceName -Query "SELECT COUNT(*) FROM sys.servers WHERE is_linked = 1;" -TrustServerCertificate
        $xe = Invoke-Sqlcmd -ServerInstance $fullInstanceName -Query "SELECT COUNT(*) FROM sys.dm_xe_sessions WHERE session_source = 'server';" -TrustServerCertificate

        # Trace flag detection
        $traceFlags = @()
        try {
            $conn = $server.ConnectionContext
            $ds = $conn.ExecuteWithResults("DBCC TRACESTATUS(-1);")
            $traceFlags = $ds.Tables[0] | Where-Object { $_.Status -eq 1 } | Select-Object -ExpandProperty TraceFlag
        }
        catch {
            Write-Warning "Unable to query trace flags on $($server.Name): $_"
            $traceFlags = @("Error")
        }

        # Detect SSAS and MDS
        $ssasDetected = (Get-Service | Where-Object { $_.Name -like "MSOLAP$*" -or $_.DisplayName -like "*Analysis Services*" }).Count -gt 0
        $mdsDetected = (Get-Service | Where-Object { $_.Name -like "MDS*" -or $_.DisplayName -like "*Master Data Services*" }).Count -gt 0

        # SQL Version mapping
        $sqlversion = switch ($majorVersion) {
            8 { 'SQL Server 2000' }
            9 { 'SQL Server 2005' }
            10 {
                if ($server.Version.Minor -eq 0) { 'SQL Server 2008' }
                elseif ($server.Version.Minor -eq 50) { 'SQL Server 2008 R2' }
                else { 'SQL Server 2008+' }
            }
            11 { 'SQL Server 2012' }
            12 { 'SQL Server 2014' }
            13 { 'SQL Server 2016' }
            14 { 'SQL Server 2017' }
            15 { 'SQL Server 2019' }
            16 { 'SQL Server 2022' }
            17 { 'SQL Server 2025 (preview)' }
            default { "Unknown ($($server.VersionString))" }
        }

        # OS Info
        $os = Get-CimInstance -ClassName Win32_OperatingSystem
        $osVersion = "$($os.Version) / $($os.BuildNumber)"
        $osFriendlyName = switch ("$($os.Version.Split('.')[0]).$($os.Version.Split('.')[1])") {
            "10.0" {
                if ($os.BuildNumber -ge 20348) { "Windows Server 2022" }
                elseif ($os.BuildNumber -ge 17763) { "Windows Server 2019" }
                elseif ($os.BuildNumber -ge 14393) { "Windows Server 2016" }
                else { "Windows 10 / Server 2016+" }
            }
            "6.3" { "Windows Server 2012 R2" }
            "6.2" { "Windows Server 2012" }
            "6.1" { "Windows Server 2008 R2" }
            "6.0" { "Windows Server 2008" }
            "5.2" { "Windows Server 2003 R2" }
            default { "Unknown Windows ($($os.Version))" }
        }

        # Compose output object
        $info = [PSCustomObject]@{
            ServerName               = $server.NetName
            InstanceName             = $server.InstanceName
            Edition                  = $server.Edition
            Version                  = $server.VersionString
            FriendlyVersion          = $sqlversion
            ProductLevel             = $server.ProductLevel
            Collation                = $server.Collation
            OSVersion                = $osVersion
            OSFriendlyVersion        = $osFriendlyName
            CPUs                     = $server.Processors
            PhysicalMemory_GB        = [Math]::Round($server.Information.PhysicalMemory / 1024.0, 1)
            SQLMaxMemory_MB          = $server.Configuration.MaxServerMemory.ConfigValue
            SQLMinMemory_MB          = $server.Configuration.MinServerMemory.ConfigValue
            MaxDOP                   = $server.Configuration.MaxDegreeOfParallelism.ConfigValue
            CostThresholdParallelism = $server.Configuration.CostThresholdForParallelism.ConfigValue
            CLR_Enabled              = $server.Configuration.IsSqlClrEnabled.ConfigValue
            CrossDBOwnershipChaining = $server.Configuration.CrossDBOwnershipChaining.ConfigValue
            XPCmdShell_Enabled       = $server.Configuration.XPCmdShellEnabled.ConfigValue
            OptimizedForAdHoc        = $server.Configuration.OptimizeAdhocWorkloads.ConfigValue
            FileStreamAccessLevel    = $server.Configuration.FilestreamAccessLevel.ConfigValue
            LinkedServers            = $ls.Column1
            ExtendedEvents           = $xe.Column1
            TraceFlags               = ($traceFlags -join ",")
            uspCountMaster           = $usp.USPinMaster
            usfCountMaster           = $usf.USFinMaster
            utfCountMaster           = $utf.UTFinMaster
            uvCountMaster            = $uv.UVinMaster
            utCountMaster            = $ut.UTinMaster
            DB_Count                 = $dbCount
            DB_TotalSize_MB          = [Math]::Round($dbSizeMB, 2)
            DB_TotalSize_GB          = [Math]::Round($dbSizeMB / 1024, 2)
            SSIS_Installed           = (Get-WmiObject -Class Win32_Service | Where-Object { $_.Name -like "*SSIS*" }).Count -gt 0
            SSRS_Installed           = (Get-Service | Where-Object { $_.Name -like "*ReportServer*" }).Count -gt 0
            SSAS_Installed           = $ssasDetected
            MDS_Installed            = $mdsDetected
        }

        $results += $info
    }
    catch {
        Write-Warning "Failed to collect data from instance: $instName. Error: $_"
    }
}

# Output to screen
# $results | Format-Table -AutoSize

# Export to CSV
$results | Export-Csv -Path $outputpath -NoTypeInformation
Write-Host "`n Export complete: C:\SQLServer_FullInventory.csv"
