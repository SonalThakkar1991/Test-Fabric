CREATE TABLE [dbo].[EmployeeMerge_v2] (

	[TERRITORY] int NULL, 
	[TERRITORY_REP_FK] int NULL, 
	[SALES_REP] varchar(200) NULL, 
	[TERR_CWID] varchar(200) NULL, 
	[Sales_Rep_id] int NULL, 
	[match_score] int NULL, 
	[Employee_ID] int NULL, 
	[Employee_Name] varchar(200) NULL, 
	[Employee_Email] varchar(200) NULL, 
	[Position_Title] varchar(200) NULL, 
	[SourceSystem] varchar(200) NULL, 
	[PrimaryKey] varchar(200) NOT NULL, 
	[modified_on] varchar(200) NOT NULL, 
	[modified_by] varchar(200) NOT NULL, 
	[selected] int NOT NULL
);