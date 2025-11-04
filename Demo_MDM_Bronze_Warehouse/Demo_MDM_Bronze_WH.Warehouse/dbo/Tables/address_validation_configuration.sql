CREATE TABLE [dbo].[address_validation_configuration] (

	[address_valid_confid] varchar(255) NULL, 
	[auth_key] varchar(255) NULL, 
	[gateway_name] varchar(255) NULL, 
	[endpoint_url] varchar(255) NULL, 
	[request_body] varchar(255) NULL, 
	[response_body] varchar(255) NULL, 
	[database_name] varchar(200) NULL, 
	[workspace_name] varchar(255) NULL, 
	[source_system] varchar(255) NULL, 
	[table_name] varchar(255) NULL
);