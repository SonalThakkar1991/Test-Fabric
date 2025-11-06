CREATE TABLE [dbo].[match_merge_secondary] (

	[match_merge_primary_key] varchar(36) NULL, 
	[match_merge_secondary_key] varchar(36) NULL, 
	[secondary_workspace_name] varchar(255) NULL, 
	[secondary_database_name] varchar(255) NULL, 
	[secondary_source_system] varchar(255) NULL, 
	[secondary_table_name] varchar(255) NULL
);