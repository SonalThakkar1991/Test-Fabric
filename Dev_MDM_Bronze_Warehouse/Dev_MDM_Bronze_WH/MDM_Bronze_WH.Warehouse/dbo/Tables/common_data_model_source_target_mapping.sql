CREATE TABLE [dbo].[common_data_model_source_target_mapping] (

	[cdm_key] varchar(36) NULL, 
	[cdm_mapping_key] varchar(36) NULL, 
	[cdm_source_target_key] varchar(36) NULL, 
	[source_workspace_name] varchar(255) NULL, 
	[source_database_name] varchar(255) NULL, 
	[source_source_system] varchar(255) NULL, 
	[source_table_name] varchar(255) NULL, 
	[source_field_name] varchar(255) NULL, 
	[target_workspace_name] varchar(255) NULL, 
	[target_database_name] varchar(255) NULL, 
	[target_source_system] varchar(255) NULL, 
	[target_table_name] varchar(255) NULL, 
	[target_field_name] varchar(255) NULL, 
	[mapping_json] varchar(255) NULL
);