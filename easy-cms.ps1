using namespace System.Data.SqlClient

function Write-LogMessage {
	Param (
		[parameter(Mandatory)]
		[string]$Message
	)
	$timeStamp = (Get-Date).ToString('[MM/dd/yy HH:mm:ss.ff]')
	Write-Verbose "$timeStamp $Message"
}

function Read-Config {
	[CmdletBinding()]
	param (
		[Parameter(Mandatory)]
		[string]$ConfigFile
	)

	if (-not (Test-Path $ConfigFile -PathType Leaf)) {
		throw "Configuration file not found: $ConfigFile"
	}

	$json = Get-Content $ConfigFile -Raw | ConvertFrom-Json

	$config = [PSCustomObject]@{
		CmsConnStr = $json.cmsConnStr
		CmsGroupName = $json.cmsGroupName
		BatchMode = $json.batchMode
		ConnStrTemplate = $json.connStrTemplate
		TestConnectivity = $json.testConnectivity
		QueryTimeout = $json.queryTimeout
		ExcludeCmsNames = $json.excludeCmsNames
		DbListQuery = $null
		Cmd = $null
		OutputFile = $null
	}

	$dbListFile = $json.dbListFile
	if ([string]::IsNullOrEmpty($dbListFile)) {
		$dbListFile = Join-Path $PSScriptRoot 'database_list.sql'
	}
	if (-not (Test-Path $dbListFile -PathType Leaf)) {
		throw "Database list query file not found: $dbListFile"
	}
	$config.DbListQuery = Get-Content -Path $dbListFile -Raw

	if ($config.BatchMode) {
		$batchFile = $json.batchFile
		if ([string]::IsNullOrEmpty($batchFile)) {
			$batchFile = Join-Path $PSScriptRoot 'batch.sql'
		}
		if (-not (Test-Path $batchFile -PathType Leaf)) {
			throw "Batch file not found: $batchFile"
		}
		$config.Cmd = Get-Content -Path $batchFile -Raw
	}
	else {
		$cmdFile = $json.cmdFile
		if ([string]::IsNullOrEmpty($cmdFile)) {
			$cmdFile = Join-Path $PSScriptRoot 'command.sql'
		}
		if (-not (Test-Path $cmdFile -PathType Leaf)) {
			throw "Command file not found: $cmdFile"
		}
		$config.Cmd = Get-Content -Path $cmdFile -Raw
	}

	if ([string]::IsNullOrEmpty($json.outputFile)) {
		$config.OutputFile = Join-Path $PSScriptRoot `
			('output_{0:yyyy-MM-dd_HH-mm-ss}.csv' -f (Get-Date))
	}
	else {
		$config.OutputFile = $json.outputFile
	}

	$config
}

function Get-CmsServerList {
	[CmdletBinding()]
	param (
		[Parameter(Mandatory)]
		[string]$ConnStr,

		[string]$GroupName
	)

	$sqlConn = $null
	$sqlCmd = $null
	$sqlReader = $null

	try {
		$sqlConn = [SqlConnection]::new()
		$sqlConn.ConnectionString = $ConnStr
		$sqlConn.Open()

        [string]$command = @'
set nocount on
create table #groups (server_group_id int not null, [name] sysname not null, [path] nvarchar(4000) not null);

with groups_hi (server_group_id, [name], [path]) as
(
select	server_group_id, [name], cast([name] as nvarchar(4000))
from	dbo.sysmanagement_shared_server_groups_internal
where [name] = @name
union all
select	gr.server_group_id, gr.[name], cast(hi.[path] + '\' + gr.[name] as nvarchar(4000))
from	dbo.sysmanagement_shared_server_groups_internal gr
		join groups_hi hi
		on gr.parent_id = hi.server_group_id
)
insert #groups(server_group_id, [name], [path])
select	server_group_id, [name], [path]
from	groups_hi

select	se.[name], se.server_name, t.[name] group_name
from	#groups t
		join dbo.sysmanagement_shared_server_groups_internal gr
		on gr.server_group_id = t.server_group_id
		join dbo.sysmanagement_shared_registered_servers_internal se
		on se.server_group_id = t.server_group_id
order by se.server_name
'@

		$sqlCmd = [SqlCommand]::new($command, $sqlConn)
		$sqlCmd.CommandType = [System.Data.CommandType]::Text
		$pGroupName = $sqlCmd.Parameters.Add('@name', [System.Data.SqlDbType]::VarChar, 128)
		$pGroupName.Value = if ([string]::IsNullOrEmpty($GroupName)) { 'DatabaseEngineServerGroup' } else { $GroupName }
		$sqlReader = $sqlCmd.ExecuteReader()

        if ($sqlReader.HasRows) {
            while ($sqlReader.Read()) {
				[PSCustomObject]@{
					CmsName = $sqlReader['name'].ToLower()
					InstanceName = $sqlReader['server_name'].ToLower()
					GroupName = $sqlReader['group_name'].ToLower()
					Excluded = $false
				}
            }
        }
	}
	finally {
		if ($null -ne $sqlReader) { $sqlReader.Close(); $sqlReader.Dispose() }
		if ($null -ne $sqlCmd) { $sqlCmd.Dispose() }
		if ($null -ne $sqlConn) { $sqlConn.Close(); $sqlConn.Dispose() }
	}
}

function Test-Connectivity {
	[CmdletBinding()]
	param (
		[Parameter(Mandatory)]
		[string]$ConnStrTemplate,

		[Parameter(Mandatory)]
		$ServerList
	)

	foreach ($server in $serverList) {
		if ($server.Excluded) {
			Write-LogMessage -Message ("Skipping excluded server: $($server.CmsName) " +
				"instance: $($server.InstanceName)")
			continue
		}

		Write-LogMessage -Message ("Checking connection to server: $($server.CmsName) " +
			"instance: $($server.InstanceName)...")
		$connStr = $ConnStrTemplate -f $server.InstanceName, 'master'
		Invoke-Sqlcmd -ConnectionString $connStr -Query 'select 1' -AbortOnError | Out-Null
	}
}

function Invoke-CmsServerBatch {
	[CmdletBinding()]
	param (
		[Parameter(Mandatory)]
		$Config,

		[Parameter(Mandatory)]
		[string]$InstanceName,

		[Parameter(Mandatory)]
		$DatabaseList
	)

	$databaseNum = 0
	foreach ($database in $DatabaseList) {
		$databaseNum++

		Write-LogMessage -Message ("  Running batch file on database $database " +
			"(database $databaseNum of $($DatabaseList.Count)) ...")

		$connStr = $Config.ConnStrTemplate -f $InstanceName, $database
		Invoke-Sqlcmd -ConnectionString $connStr -Query $Config.Cmd -AbortOnError | Out-Null
	}
}

function Invoke-CmsServerCommand {
	[CmdletBinding()]
	param (
		[Parameter(Mandatory)]
		$Config,

		[Parameter(Mandatory)]
		$Server,

		[Parameter(Mandatory)]
		$DatabaseList,

		[Parameter(Mandatory)]
		[bool]$AddHeader
	)

	$rowCountThreshold = 1000
	$columnSeparator = ';'
	$addHeader = $AddHeader
	$sqlConn = $null
	$sqlCmd = $null
	$sqlReader = $null
	$writer = $null

	try {
		$sqlConn = [SQLConnection]::new()
        $sqlConn.ConnectionString = $Config.ConnStrTemplate -f $Server.InstanceName, 'master'
        $sqlConn.Open()

		# get a "real" server name
		$sqlCmd = [SqlCommand]::new('select @@servername', $sqlConn) 
		$sqlCmd.CommandType = [System.Data.CommandType]::Text
		$sqlCmd.CommandTimeout = $Config.QueryTimeout
		$realServerName = ([string]$sqlCmd.ExecuteScalar()).ToUpper()

		$writer = [System.IO.StreamWriter]::new($Config.OutputFile, $true)

		$sqlCmd.CommandText = $Config.Cmd
		$databaseNum = 0
		foreach ($database in $DatabaseList) {
			$databaseNum++

			Write-LogMessage -Message ("  Running command file on database $database " + `
				"(database $databaseNum of $($DatabaseList.Count)) ...")

			$sqlConn.ChangeDatabase($database)
			$sqlReader = $sqlCmd.ExecuteReader()

			$rowCount = 0
			if ($sqlReader.HasRows) {
				# Add header for the first row
				if ($addHeader) {
					$line = @(
						'CMS_GROUP_NAME',
						'CMS_SERVER_NAME',
						'INSTANCE_NAME',
						'DATABASE_NAME'
					)
					for ($i = 0; $i -lt $sqlReader.FieldCount; $i++) {
						$line += $sqlReader.GetName($i).ToUpper()
					}
					$writer.WriteLine($line -join $columnSeparator)
					$addHeader = $false
				}

				while ($sqlReader.Read()) {
					$rowCount ++

					$line = @(
						$Server.GroupName,
						$Server.CmsName,
						$realServerName,
						$database
					)
					for ($i = 0; $i -lt $sqlReader.FieldCount; $i++) {
						if ($sqlReader[$i] -isnot [DBNull]) {
							$line += $sqlReader[$i].ToString()
						}
						else {
							$line += ''
						}
					}
					$writer.WriteLine($line -join $columnSeparator)
					if ($rowCount % $rowCountThreshold -eq 0) {
						Write-LogMessage -Message "    $rowCount rows saved ..."
					}
				}
			}
			$sqlReader.Close()
			$sqlReader.Dispose()

			if ($rowCount -ge $rowCountThreshold) {
				Write-LogMessage -Message "    $rowCount total rows saved ..."
			}
		}
	}
	finally {
		if ($null -ne $sqlReader) {
			$sqlReader.Close()
			$sqlReader.Dispose()
		}
		if ($null -ne $sqlCmd) {
			$sqlCmd.Dispose()
		}
		if ($null -ne $sqlConn) {
			$sqlConn.Close()
			$sqlConn.Dispose()
		}
		if ($null -ne $writer) {
			$writer.Close()
		}
	}
}

function Get-DatabaseList {
	[CmdletBinding()]
	param (
		[Parameter(Mandatory)]
		[string]$ConnStr,

		[string]$Cmd
	)

	Invoke-Sqlcmd -ConnectionString $ConnStr -Query $Cmd -AbortOnError `
		| Select-Object -ExpandProperty name
}

function Invoke-EasyCMS {
	<#
	.SYNOPSIS
		Executes a SQL command or batch across multiple SQL Server instances registered in CMS

	.DESCRIPTION
		Invoke-CMS connects to a Central Management Server (CMS), retrieves a list of registered SQL Server instances,
		and executes either a batch script or a query against databases on those instances

		The function supports:
		- Filtering instances using an exclusion list
		- Optional connectivity checks before execution
		- Batch mode. Execute script with one or multiple batches without collecting results
		- Command mode. Execute query (usually some kind of a SELECT) and export results to a CSV file

	.PARAMETER ConfigFile
		Path to the JSON configuration file

		The configuration file must include:
		- CMS connection string
		- Target CMS group name
		- Command or batch script file paths
		- Database list query
		- Optional settings such as exclusions and timeouts

	.EXAMPLE
		Invoke-CMS -ConfigFile ".\config.json"
	#>
	[CmdletBinding(SupportsShouldProcess = $true)]
	param (
		[Parameter(Mandatory)]
		[string]$ConfigFile
	)

	Set-StrictMode -Version Latest

	try {
		$config = Read-Config -ConfigFile $ConfigFile
		$serverList = @(Get-CmsServerList -ConnStr $config.CmsConnStr `
			-GroupName $config.CmsGroupName)

		foreach ($server in $serverList) {
			if ($config.ExcludeCmsNames.Contains($server.CmsName)) {
				$server.Excluded = $true
			}
		}

		$dryRun = $true
		$connStrParser = [SqlConnectionStringBuilder]::new($config.CmsConnStr)
		$mode = if ($config.BatchMode) { 'Batch' } else { 'Command' }
		$target = "CMS: $($connStrParser.DataSource), Mode: $mode"

		if ($PSCmdlet.ShouldProcess($target)) {
			$dryRun = $false
		}
		else {
			Write-Verbose 'Dry run'
		}

		if ($config.TestConnectivity) {
			Test-Connectivity -ConnStrTemplate $config.ConnStrTemplate -ServerList $serverList
		}

		if (-not $dryRun) {
			if (-not $config.BatchMode) {
				if (Test-Path $config.OutputFile) {
					$answer = Read-Host 'Overwrite existing output file? [y/n]'
					if ($answer -ne 'y') {
						return
					}
					# Delete output file
					Remove-Item $config.OutputFile
				}
			}

			$serverNum = 0
			$addHeader = $true
			foreach ($server in $serverList) {
				$serverNum++

				if ($server.Excluded) {
					Write-LogMessage -Message ("Skipping excluded server: $($server.CmsName) " +
						"instance: $($server.InstanceName)")
					continue
				}

				Write-LogMessage -Message ("Selecting database list from server $($server.CmsName) " +
					"(server $serverNum of $($serverList.Count)) ...")

				$connStr = $config.ConnStrTemplate -f $server.InstanceName, 'master'
				$databaseList = @(Get-DatabaseList -ConnStr $connStr -Cmd $config.DbListQuery)

				Write-LogMessage -Message "Total $($databaseList.Count) database(s) found"

				if ($databaseList.Count -eq 0) {
					continue
				}

				if ($config.BatchMode) {
					Invoke-CmsServerBatch -Config $config -InstanceName $server.InstanceName `
						-DatabaseList $databaseList
				}
				else {
					Invoke-CmsServerCommand -Config $config -Server $server `
						-DatabaseList $databaseList -AddHeader $addHeader
					$addHeader = $false
				}
			}
		}
	}
	catch {
		Write-Error "Failed to invoke CMS: $_"
		throw
	}
}

Clear-Host

$configFile = Join-Path $PSScriptRoot 'config.json'
Invoke-EasyCMS -ConfigFile $configFile `
	-Verbose `
	#-WhatIf
