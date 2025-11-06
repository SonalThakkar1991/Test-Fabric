CREATE TABLE [Test].[table_list_cache_v3] (

	[GUID] varchar(50) NOT NULL, 
	[master_table_id] bigint NOT NULL, 
	[table_name] varchar(200) NOT NULL, 
	[source_system] varchar(50) NOT NULL, 
	[column_count] int NULL, 
	[created_on] varchar(50) NULL, 
	[created_by] varchar(50) NULL
);