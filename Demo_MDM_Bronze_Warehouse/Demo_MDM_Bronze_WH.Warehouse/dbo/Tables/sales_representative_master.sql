CREATE TABLE [dbo].[sales_representative_master] (

	[sales_representative_id] int NOT NULL, 
	[sales_representative] varchar(250) NOT NULL, 
	[territory] int NULL, 
	[territory_representative_FK] int NULL, 
	[territory_CWID] varchar(250) NULL, 
	[merged_by] varchar(100) NULL, 
	[merged_on] datetime2(0) NULL
);