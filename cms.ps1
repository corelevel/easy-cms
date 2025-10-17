Clear-Host

function Write-LogMessage
{
    Param
    (
        [parameter(Mandatory)]
        [string]$Message,
        [parameter()]
        [System.ConsoleColor]$Color = [System.ConsoleColor]::White
    )
    $timeStamp = (Get-Date).ToString('[MM/dd/yy HH:mm:ss.ff]')
    $logMessage = $timeStamp + ' ' + $Message

    Write-Host $logMessage -ForegroundColor $Color
}

class ScriptSettings
{
    [string]$CMSSServerName
    [string]$CMSSRootGroupName
    [string]$CMSSConnectionString
    [string]$DatabaseListQuery
    [string]$Command
    [string]$OutputFile
    [string]$EndServerConnectionStringTemplate
    [int]$CommandTimeout
    [bool]$CheckConnectivity
    [System.Collections.Generic.List[string]]$ExcludedServerList
    
    ScriptSettings([string]$settingsFile)
    {
        $xml = [System.Xml.XmlDocument]::new()
        $xml.Load($settingsFile)

        $this.CMSSServerName = $xml.Settings.CMSSServerName
        $this.CMSSRootGroupName = $xml.Settings.CMSSRootGroupName
        $this.CMSSConnectionString = $xml.Settings.CMSSConnectionString
        $this.CommandTimeout = [int]$xml.Settings.CommandTimeout

        if ($xml.Settings.DatabaseListQueryFile) {
            $this.DatabaseListQuery = Get-Content -Path $xml.Settings.DatabaseListQueryFile
        }
        else {
            $this.DatabaseListQuery = Get-Content -Path ($PSScriptRoot + '\database_list_query.sql')
        }

        if ($xml.Settings.CommandFile) {
            $this.Command = Get-Content -Path $xml.Settings.CommandFile
        }
        else {
            $this.Command = Get-Content -Path ($PSScriptRoot + '\command.sql')
        }

        if (-not $this.OutputFile) {
            $this.OutputFile = $PSScriptRoot + '\output_{0:yyyy-MM-dd}T{0:HH-mm-ss}.csv' -f (Get-Date)
        }

        $this.EndServerConnectionStringTemplate = $xml.Settings.EndServerConnectionStringTemplate
        if ($xml.Settings.CheckConnectivity -eq [bool]::TrueString) {
            $this.CheckConnectivity = $true
        }
        else {
            $this.CheckConnectivity = $false
        }
        
        $this.ExcludedServerList = [System.Collections.Generic.List[string]]::new()
        foreach($server in $xml.Settings.ExcludedServer.Server) {
            [void]$this.ExcludedServerList.Add($server)
        }
    }
}

class SQLServer
{
    [int]$Id
    [string]$Name
    [string]$ServerName
    [string]$Description
    [string]$GroupName
    [string]$Path
}

class CMSS
{
    [string]$ConnectionString
    [System.Collections.Generic.List[SQLServer]]$ServerList

    CMSS([string]$connectionString)
    {
        $this.ConnectionString = $connectionString
        $this.ServerList = [System.Collections.Generic.List[SQLServer]]::new()
    }

    ReadServerList([string]$groupName)
    {
        $this.ServerList.Clear()

        Write-LogMessage -Message ('Reading server list from CMSS server')

        if ([string]::IsNullOrEmpty($groupName)) {
            $groupName = 'DatabaseEngineServerGroup'
        }

        $sqlConnection = [System.Data.SqlClient.SQLConnection]::new()
        $sqlConnection.ConnectionString = $this.ConnectionString
        $sqlConnection.Open()

        [string]$command = 'set nocount on
declare @groups table (server_group_id int not null, [name] sysname not null, [path] nvarchar(500) not null, primary key(server_group_id));

with groups_hi (server_group_id, [name], [path]) as
(
select	server_group_id, [name], cast([name] as nvarchar(500))
from	dbo.sysmanagement_shared_server_groups_internal
where [name] = @name
union all
select	gr.server_group_id, gr.[name], cast(hi.[path] + ''\'' + gr.[name] as nvarchar(500))
from	dbo.sysmanagement_shared_server_groups_internal gr
		join groups_hi hi
		on gr.parent_id = hi.server_group_id
)
insert @groups(server_group_id, [name], [path])
select * from groups_hi

select	se.server_id, se.[name], se.server_name, se.[description], t.[name] group_name, t.[path]
from	@groups t
		join dbo.sysmanagement_shared_server_groups_internal gr
		on gr.server_group_id = t.server_group_id
		join dbo.sysmanagement_shared_registered_servers_internal se
		on se.server_group_id = t.server_group_id
order by se.server_name'

        $sqlCommand = [System.Data.SqlClient.SqlCommand]::new($command, $sqlConnection)
        $sqlCommand.CommandType = [System.Data.CommandType]::Text
        $pGroupName = $sqlCommand.Parameters.Add('@name', [System.Data.SqlDbType]::VarChar)
        $pGroupName.Value = $groupName
        $sqlReader = $sqlCommand.ExecuteReader()

        if ($sqlReader.HasRows) {
            while ($sqlReader.Read()) {
                $server = [SQLServer]::new()

                if ($sqlReader['server_id'] -isnot [DBNull]) {
                    $server.Id = $sqlReader['server_id']
                }
                if ($sqlReader['name'] -isnot [DBNull]) {
                    $server.Name = $sqlReader['name']
                }
                if ($sqlReader['server_name'] -isnot [DBNull]) {
                    $server.ServerName = $sqlReader['server_name']
                }
                if ($sqlReader['description'] -isnot [DBNull]) {
                    $server.Description = $sqlReader['description']
                }
                if ($sqlReader['group_name'] -isnot [DBNull]) {
                    $server.GroupName = $sqlReader['group_name']
                }
                if ($sqlReader['path'] -isnot [DBNull]) {
                    $server.Path = $sqlReader['path']
                }

                [void]$this.serverList.Add($server)
            }
        }

        $sqlReader.Close()
        $sqlConnection.Close()

        Write-LogMessage -Message ('Total {0} server(s) found' -f $this.serverList.Count)
    }
}

class CommandExecutor
{
    [System.Collections.Generic.List[SQLServer]]$ServerList
    [System.Collections.Generic.List[string]]$ExcludedServerList
    [string]$DatabaseListQuery
    [string]$Command
    [string]$OutputFile
    [string]$ConnectionStringTemplate
    [int]$CommandTimeout
    [int]$RowCountThreshold = 1000

    CommandExecutor([System.Collections.Generic.List[SQLServer]]$serverList, [ScriptSettings]$settings)
    {
        $this.ServerList = $serverList
        $this.ExcludedServerList = $settings.ExcludedServerList
        $this.DatabaseListQuery = $settings.DatabaseListQuery
        $this.Command = $settings.Command
        $this.OutputFile = $settings.OutputFile
        $this.ConnectionStringTemplate = $settings.EndServerConnectionStringTemplate
        $this.CommandTimeout = $settings.CommandTimeout
    }

    CheckConnectivity()
    {
        foreach ($server in $this.ServerList) {
            if ($this.IsServerExcluded($server.ServerName)) {
                Write-LogMessage -Message ('Excluded server {0} ...' -f $server.ServerName) -color ([System.ConsoleColor]::Yellow)
                Write-LogMessage -Message 'Skipped!' -color ([System.ConsoleColor]::Yellow)
                continue
            }

            Write-LogMessage -Message ('Checking connection to {0} database server ...' -f $server.ServerName)

            $sqlConnection = [System.Data.SqlClient.SQLConnection]::new()
            $sqlConnection.ConnectionString = $this.ConnectionStringTemplate -f $server.ServerName
            $sqlConnection.Open()
            $sqlConnection.Close()

            Write-LogMessage -Message 'Success!'
        }
    }

    Execute()
    {
        if (Test-Path $this.OutputFile) {
            $answer = Read-Host "Overwrite existing output file? [y/n]"
            if ($answer -ne 'y') {
                return
            }
            # Delete output file
            Remove-Item $this.OutputFile
        }

        $is_first_run = $true
        $srv_count = $this.ServerList.Count
        $srv_num = 0

        foreach ($server in $this.ServerList) {
            $srv_num++

            if ($this.IsServerExcluded($server.ServerName)) {
                Write-LogMessage -Message ('Excluded server {0} (server {1} of {2}) ...' -f $server.ServerName, $srv_num, $srv_count) -color ([System.ConsoleColor]::([System.ConsoleColor]::Yellow))
                Write-LogMessage -Message 'Skipped!' -color ([System.ConsoleColor]::([System.ConsoleColor]::Yellow))
                continue
            }

            Write-LogMessage -Message ('Selecting database list from server {0} (server {1} of {2}) ...' -f $server.ServerName, $srv_num, $srv_count)

            $sqlConnection = [System.Data.SqlClient.SQLConnection]::new()
            $sqlConnection.ConnectionString = $this.ConnectionStringTemplate -f $server.ServerName
            $sqlConnection.Open()

            # Get database list from the server
            $sqlCommand = [System.Data.SqlClient.SqlCommand]::new($this.DatabaseListQuery, $sqlConnection) 
            $sqlCommand.CommandType = [System.Data.CommandType]::Text
            $sqlCommand.CommandTimeout = $this.CommandTimeout
            $sqlReader = $sqlCommand.ExecuteReader()

            $databaseList = [System.Collections.Generic.List[string]]::new()

            if ($sqlReader.HasRows) {
                while ($sqlReader.Read()) {
                    if ($sqlReader['name'] -isnot [DBNull]) {
                        [void]$databaseList.Add($sqlReader['name'])
                    }
                }
            }
            $sqlReader.Close()

            Write-LogMessage -Message ('Total {0} database(s) found' -f $databaseList.Count)

            $db_count = $databaseList.Count
            $db_num = 0

            # get "real" server name
            $sqlCommand.CommandText = 'select upper(@@servername)'
            $serverName = [string]$sqlCommand.ExecuteScalar()

            # Run the query on each database and save the result to the file
            $sqlCommand.CommandText = $this.Command

            foreach ($database in $databaseList) {
                $db_num++

                Write-LogMessage -Message ('  Running the query on database [{0}] (database {1} of {2}) ...' -f $database, $db_num, $db_count)

                $sqlConnection.ChangeDatabase($database)
                $sqlReader = $sqlCommand.ExecuteReader()

                $row_count = 0
                if ($sqlReader.HasRows) {
                    # Add header for first row
                    if ($is_first_run) {
                        'CMSS_GROUP_NAME;CMSS_SERVER_NAME;SERVER_NAME;DATABASE_NAME;' | Out-File $this.OutputFile -Append -NoNewline
                        for ($i = 0; $i -lt $sqlReader.FieldCount; $i++) {
                            $sqlReader.GetName($i) + ';' | Out-File $this.OutputFile -Append -NoNewline
                        }
                        '' | Out-File $this.OutputFile -Append
                        $is_first_run = $false
                    }

                    while ($sqlReader.Read()) {
                        $row_count ++

                        $server.GroupName + ';' + $server.ServerName + ';' + $serverName + ';' + $database + ';' | Out-File $this.OutputFile -Append -NoNewline
                        for ($i=0; $i -lt $sqlReader.FieldCount; $i++) {
                            if ($sqlReader[$i] -isnot [DBNull]) {
                                $sqlReader[$i].ToString() + ';' | Out-File $this.OutputFile -Append -NoNewline
                            }
                            else {
                                ';' | Out-File $this.OutputFile -Append -NoNewline
                            }
                        }
                        if ($row_count % $this.RowCountThreshold -eq 0) {
                            Write-LogMessage -Message ('    {0} rows saved ...' -f $row_count)
                        }
                        '' | Out-File $this.OutputFile -Append
                    }
                }
                $sqlReader.Close()

                if ($row_count -ge $this.RowCountThreshold) {
                    Write-LogMessage -Message ('    {0} total rows saved ...' -f $row_count)
                }
            }
            $sqlConnection.Close()
        }
        Write-LogMessage -Message 'Success!'
    }

    [bool]IsServerExcluded([string]$serverName)
    {
        return $this.ExcludedServerList.Contains($serverName)
    }
}

[string]$settingsFile = $PSScriptRoot + '\settings.xml'

try
{
    $settings = [ScriptSettings]::new($settingsFile)

    $cms = [CMSS]::new($settings.CMSSConnectionString)
    $cms.ReadServerList($settings.CMSSRootGroupName)

    $ce = [CommandExecutor]::new($cms.ServerList, $settings)
    if ($settings.CheckConnectivity) {
        $ce.CheckConnectivity()
    }
    $ce.Execute()
    exit 0
}
catch [System.Data.SqlClient.SqlException]
{
    Write-LogMessage -Message $_.Exception.ToString()
    exit 1
}
catch
{
	Write-LogMessage -Message $_.Exception.ToString()
    Write-LogMessage -Message 'Error occurred. Please check log'
    exit 1
}