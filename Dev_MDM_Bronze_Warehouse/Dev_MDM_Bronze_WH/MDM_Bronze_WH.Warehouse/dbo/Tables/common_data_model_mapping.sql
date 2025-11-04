CREATE TABLE [dbo].[common_data_model_mapping] (

	[cdm_key] varchar(36) NULL, 
	[cdm_mapping_key] varchar(36) NULL, 
	[detail] varchar(500) NULL, 
	[comman_data_model] varchar(255) NULL, 
	[source_system] varchar(255) NULL, 
	[status] varchar(50) NULL, 
	[created_on] date NULL, 
	[created_by] varchar(100) NULL, 
	[modified_on] date NULL, 
	[modified_by] varchar(100) NULL, 
	[workspace_name] varchar(255) NULL, 
	[database_name] varchar(255) NULL, 
	[table_name] varchar(255) NULL
);