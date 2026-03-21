set nocount on

-- Just an example
select	[name] [user_database_name]
from	sys.databases
where is_query_store_on = 0
	and database_id > 4
	and [name] not in (N'distribution')