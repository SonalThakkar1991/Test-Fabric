CREATE TABLE [dbo].[match_merge_field_mapping_new] (

	[match_merge_primary_key] varchar(36) NULL, 
	[match_merge_secondary_key] varchar(36) NULL, 
	[primaryfield] varchar(255) NULL, 
	[secondaryfield] varchar(255) NULL, 
	[match_merge_field_mapping_key] varchar(36) NULL, 
	[column_match_field] bit NULL
);