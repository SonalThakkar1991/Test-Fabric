CREATE TABLE [dbo].[address_validation_confg] (

	[address_valid_confid] varchar(36) NULL, 
	[GatewayName] varchar(255) NULL, 
	[Endpoint] varchar(max) NULL, 
	[Key] varchar(36) NULL, 
	[Request] varchar(max) NULL, 
	[Response] varchar(max) NULL
);