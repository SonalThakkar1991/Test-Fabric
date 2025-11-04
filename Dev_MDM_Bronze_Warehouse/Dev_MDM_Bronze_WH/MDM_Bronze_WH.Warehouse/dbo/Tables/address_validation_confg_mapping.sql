CREATE TABLE [dbo].[address_validation_confg_mapping] (

	[address_validation_confg_mappingid] varchar(36) NULL, 
	[address_valid_confid] varchar(36) NULL, 
	[address_confg_mapping_details] varchar(255) NULL, 
	[workspace_name] varchar(255) NULL, 
	[database_name] varchar(255) NULL, 
	[source_system] varchar(255) NULL, 
	[createdon] datetime2(0) NULL, 
	[createdby] varchar(255) NULL, 
	[modifiedon] datetime2(0) NULL, 
	[modifiedby] varchar(255) NULL
);