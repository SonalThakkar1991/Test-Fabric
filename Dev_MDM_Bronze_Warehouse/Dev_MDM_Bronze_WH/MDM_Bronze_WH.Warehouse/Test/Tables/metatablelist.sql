CREATE TABLE [Test].[metatablelist] (

	[GUID] varchar(50) NOT NULL, 
	[master_table_id] bigint NOT NULL, 
	[workspace_name] varchar(200) NULL, 
	[lakehouse_name] varchar(200) NULL, 
	[table_name] varchar(200) NULL, 
	[source_system] varchar(200) NULL, 
	[created_on] varchar(50) NULL, 
	[created_by] varchar(50) NOT NULL
);