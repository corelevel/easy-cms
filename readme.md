# SQL Server CMS helper tool
- This tool will allow you to define complex filters for databases and servers registered in a [Central Management Server (CMS)](https://learn.microsoft.com/en-us/ssms/register-servers/register-servers). 
- It will run your query on each server and each database. Tool has two modes:
	- *Command* mode - tool will run your script(usually some kind of *select*) and save results into a *csv* file
	- *Batch* mode - tool will run your script(may contain multiple batches separated by *go*) without saving any results
- The output *csv* file have this structure:

| CMSS_GROUP_NAME |     CMSS_SERVER_NAME      |          SERVER_NAME          | DATABASE_NAME | Column 1 | ... | Column N |
| :-------------: | :-----------------------: | :---------------------------: | :-----------: | :------: | :-: | :------: |
| CMS group name  | Name of the server in CMS | Real SQL Server instance name | Database name |  Value   | ... |  Value   |

## Getting started
- Modify *settings.xml* file
	- Set *CMSSConnectionString*
	- Set *CMSSRootGroupName*
- Modify *database_list_query.sql* to define a filter for databases on each server
- Modify *command.sql* with your query
- Run *cms.ps1* script
