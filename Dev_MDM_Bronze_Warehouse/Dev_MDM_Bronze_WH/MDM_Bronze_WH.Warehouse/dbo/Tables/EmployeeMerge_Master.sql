CREATE TABLE [dbo].[EmployeeMerge_Master] (

	[Sales_Rep_id] int NOT NULL, 
	[SALES_REP] varchar(255) NOT NULL, 
	[TERRITORY] int NULL, 
	[TERRITORY_REP_FK] int NULL, 
	[TERR_CWID] varchar(255) NULL, 
	[Employee_Email] varchar(255) NULL, 
	[Position_Title] varchar(255) NULL, 
	[merged_by] varchar(100) NULL, 
	[merged_on] datetime2(0) NULL
);