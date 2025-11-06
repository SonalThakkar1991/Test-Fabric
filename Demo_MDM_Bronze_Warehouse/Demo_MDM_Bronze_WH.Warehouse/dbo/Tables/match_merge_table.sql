CREATE TABLE [dbo].[match_merge_table] (

	[match_merge_primary_key] varchar(36) NULL, 
	[match_merge_secondary_key] varchar(36) NULL, 
	[match_merge_field_mapping_key] varchar(255) NULL, 
	[workspace_name] varchar(255) NULL, 
	[database_name] varchar(255) NULL, 
	[source_system] varchar(255) NULL, 
	[table_name] varchar(255) NULL, 
	[match_merge_id] varchar(36) NULL
);