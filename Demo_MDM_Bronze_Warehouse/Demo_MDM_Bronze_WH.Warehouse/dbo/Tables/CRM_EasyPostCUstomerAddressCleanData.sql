CREATE TABLE [dbo].[CRM_EasyPostCUstomerAddressCleanData] (

	[customer_key] varchar(100) NOT NULL, 
	[customer_name] varchar(250) NULL, 
	[street1] varchar(1000) NULL, 
	[street2] varchar(1000) NULL, 
	[city] varchar(250) NULL, 
	[zip] varchar(250) NULL, 
	[country] varchar(250) NULL, 
	[merged_by] varchar(100) NULL, 
	[merged_on] datetime2(0) NULL
);