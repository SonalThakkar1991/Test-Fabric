CREATE TABLE [dbo].[EmployeeMerge_v3] (

	[TERR_CWID] varchar(4000) NULL, 
	[SALES_REP] varchar(4000) NULL, 
	[TERRITORY] varchar(4000) NULL, 
	[Sales_Rep_id] bigint NULL, 
	[TERRITORY_REP_FK] varchar(4000) NULL, 
	[Match_Score] bigint NULL, 
	[created_on] varchar(50) NOT NULL, 
	[created_by] varchar(4000) NOT NULL
);