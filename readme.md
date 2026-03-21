# Invoke-CMS

Execute SQL scripts across multiple SQL Server instances using [Central Management Server (CMS)](https://learn.microsoft.com/en-us/ssms/register-servers/register-servers).

This tool is designed for DBAs and engineers who need a simple, reliable way to run queries or batch scripts across many servers and databases - without manual effort.

## Features
- Execute scripts across all CMS-registered instances
- *Batch mode* - executes script with one or multiple batches
- *Command mode* - executes script and collect results into CSV
- Built-in connectivity testing
- Exclude servers by name

## Use Cases
- Running administrative queries across environments
- Data collection from multiple SQL Server instances
- Executing DML/DDL migration scripts
- Validating deployments
- Checking database/server settings

## Requirements
PowerShell with the [SQL Server module](https://learn.microsoft.com/en-us/powershell/sql-server/download-sql-server-ps-module) installed. To install it just run:
```powershell
Install-Module SqlServer
```

## Configuration
- All settings are defined in a *config.json* file. Example *config.json*
```json
{
    "cmsConnStr": "Data Source=CMS-SERVER;Initial Catalog=msdb;Integrated Security=True;Application Name=cmss;",
    "cmsGroupName": "MyGroupName",
    "batchMode": false,
    "connStrTemplate": "Data Source={0};Initial Catalog={1};Integrated Security=True;Application Name=cmss;",
    "testConnectivity": true,
    "queryTimeout": 25,
    "dbListQueryFile": "",
    "cmdFile": "",
    "batchFile": "",
    "outputFile": "",
    "excludeCmsNames": [
        "invisible",
		"always-broken"
    ]
}
```

## Modes
### Command Mode (*batchMode* = false)
- Executes a query (usually some kind of a *SELECT*)
- Writes results to CSV
- Includes metadata columns:
	- CMS group name
	- CMS server name
	- Instance name
	- Database name
- The output *csv* file will have this structure:

| CMS_GROUP_NAME | CMS_SERVER_NAME |  INSTANCE_NAME  | DATABASE_NAME | Column1 | ... | ColumnN |
| :------------: | :-------------: | :-------------: | :-----------: | :-----: | :-: | :-----: |
| CMS group name | CMS server name | SQL Server name | Database name |  Value  | ... |  Value  |

### Batch Mode (*batchMode* = true)
- Executes script with one or multiple batches
- No output file
- Suitable for:
	- Data fixes
	- Migrations
	- Maintenance tasks

## Overriding Default File Paths
By default, the script looks for the following files in the script directory:
- *database_list_query.sql*
- *command.sql*
- *batch.sql*
- *output_{yyyy-MM-dd_HH-mm-ss}.csv* (auto-generated)

You can override these defaults by specifying custom paths in the configuration file. For example:
```json
{
  "dbListQueryFile": "C:\\scripts\\my_db_list.sql",
  "cmdFile": "C:\\scripts\\my_command.sql",
  "batchFile": "C:\\scripts\\my_batch.sql",
  "outputFile": "C:\\output\\results.csv"
}
```

## Getting started
- Modify *config.json* file
	- Set *cmsConnStr*
	- Set *cmsGroupName*
	- Set *connStrTemplate*
- Modify *database_list_query.sql* to define a filter for databases on each server
- Modify *command.sql* with your query
- Run *cms.ps1* script

## License
[MIT License](http://en.wikipedia.org/wiki/MIT_License)