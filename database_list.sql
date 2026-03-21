-- This query must return a single column named [name]
set nocount on
 
/*
Just an example
Selects all user databases and skips databases on AlwaysOn secondary replicas
*/
select	[name]
from	sys.databases
where 1 = 1
	and [name] not in (N'distribution')
	and [name] not in (select agbd.[database_name] from sys.availability_databases_cluster agbd)
	and database_id > 4
	and state_desc = N'ONLINE'
union all
select	db.[database_name]
from	sys.dm_hadr_availability_group_states st
		join sys.availability_groups gr
		on gr.group_id = st.group_id
		join sys.availability_databases_cluster db
		on db.group_id = gr.group_id
where 1 = 1
	and db.[database_name] not in (N'distribution')
	and st.primary_replica = @@servername