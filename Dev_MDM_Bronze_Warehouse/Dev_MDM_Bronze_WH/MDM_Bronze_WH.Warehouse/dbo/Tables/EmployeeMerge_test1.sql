CREATE TABLE [dbo].[EmployeeMerge_test1] (

	[TERR_CWID] varchar(4000) NULL, 
	[SALES_REP] varchar(4000) NULL, 
	[TERRITORY] varchar(4000) NULL, 
	[Sales_Rep_id] bigint NULL, 
	[TERRITORY_REP_FK] varchar(4000) NULL, 
	[match_score] bigint NULL, 
	[Match_Group_ID] bigint NULL, 
	[Selected] int NOT NULL, 
	[Match_Count] bigint NOT NULL, 
	[PrimaryKey] varchar(4000) NOT NULL
);