CREATE TABLE [Test].[metacolumnslist] (

	[master_GUID] varchar(50) NOT NULL, 
	[column_name] varchar(200) NULL, 
	[master_table_id] bigint NOT NULL, 
	[workspace_name] varchar(200) NULL, 
	[lakehouse_name] varchar(200) NULL, 
	[column_id] bigint NOT NULL, 
	[GUID] varchar(50) NOT NULL
);