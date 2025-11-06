CREATE TABLE [Test].[table_classification] (

	[selection_id] bigint NOT NULL, 
	[GUID] varchar(36) NOT NULL, 
	[master_table_GUID] varchar(50) NULL, 
	[table_name] varchar(200) NULL, 
	[source_system] varchar(50) NULL, 
	[description] varchar(4000) NULL, 
	[table_type] varchar(50) NULL, 
	[selected] int NULL, 
	[suggested_table_name] varchar(200) NULL, 
	[master_table_id] bigint NULL, 
	[created_on] varchar(50) NULL, 
	[created_by] varchar(50) NOT NULL, 
	[modified_on] varchar(50) NULL, 
	[modified_by] varchar(50) NULL
);