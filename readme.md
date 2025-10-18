# SQL Server CMS helper tool
- This tool will allow you to define complex filters for databases and servers registered in a [Central Management Server (CMS)](https://learn.microsoft.com/en-us/ssms/register-servers/register-servers). 
- It will run your query on each server and each database and save results into a *csv* file(in command mode)
- The output *csv* file will have this structure:

| CMSS_GROUP_NAME | CMSS_SERVER_NAME          |          SERVER_NAME          | DATABASE_NAME | Column name 1 | ... | Column name N |
| --------------- | ------------------------- | :---------------------------: | ------------- | ------------- | --- | ------------- |
| CMS group name  | Name of the server in CMS | Real SQL Server instance name | Database name | Value 1       | ... | Value N       |

## Getting started
- Modify *settings.xml* file
	- Set *CMSSConnectionString*
	- Set *CMSSRootGroupName*
- Modify *database_list_query.sql* to define a filter for databases on each server
- Modify *command.sql* with your query
- Run *cms.ps1* script
