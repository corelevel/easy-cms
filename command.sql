set nocount on

-- Just an example. Get all DBs where Query Store isn't enabled
select	[name] [no_query_store]
from	sys.databases
where is_query_store_on = 0
	and database_id > 4
	and [name] not in (N'distribution')