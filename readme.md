# SQL Server CMS helper script
- This script will allow you to define complex filters for databases and servers
- It will run your query on each server and each database and save results to a *csv* file(in command mode)

## Getting started
- Modify *settings.xml* file
	- Set *CMSSConnectionString*
	- Set *CMSSRootGroupName*
- Modify *database_list_query.sql* to define a filter for databases on each server
- Modify *command.sql* with your query
- Run *cms.ps1* script