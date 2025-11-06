CREATE TABLE [dbo].[common_data_model_test] (

	[cdm_key] bigint NOT NULL, 
	[common_data_name] varchar(255) NOT NULL, 
	[entity_name] varchar(255) NOT NULL, 
	[created_on] date NULL, 
	[created_by] varchar(100) NULL, 
	[workspace_name] varchar(255) NULL, 
	[database_name] varchar(255) NULL
);


GO
ALTER TABLE [dbo].[common_data_model_test] ADD CONSTRAINT UK_common_data_model_cdm_key unique NONCLUSTERED ([cdm_key]);