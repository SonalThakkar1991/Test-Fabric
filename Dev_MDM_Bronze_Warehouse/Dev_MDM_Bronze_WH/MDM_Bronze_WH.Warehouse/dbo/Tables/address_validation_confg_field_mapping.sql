CREATE TABLE [dbo].[address_validation_confg_field_mapping] (

	[address_validation_confg_field_mappingid] varchar(36) NULL, 
	[address_validation_confg_mappingid] varchar(36) NULL, 
	[address_valid_confid] varchar(36) NULL, 
	[req_res] varchar(255) NULL, 
	[target_element] varchar(255) NULL, 
	[source_table] varchar(255) NULL, 
	[source_field] varchar(255) NULL
);