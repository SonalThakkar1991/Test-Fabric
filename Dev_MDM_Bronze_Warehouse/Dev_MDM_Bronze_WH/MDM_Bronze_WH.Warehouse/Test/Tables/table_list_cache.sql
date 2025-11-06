CREATE TABLE [Test].[table_list_cache] (

	[GUID] varchar(50) NOT NULL, 
	[master_table_id] bigint NOT NULL, 
	[table_name] varchar(200) NULL, 
	[source_system] varchar(50) NULL, 
	[created_on] datetime2(6) NULL, 
	[created_by] varchar(50) NOT NULL
);