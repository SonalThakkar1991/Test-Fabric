CREATE TABLE [dbo].[sales_representative_suggestion] (

	[TERRITORY] bigint NULL, 
	[TERRITORY_REP_FK] bigint NULL, 
	[SALES_REP] varchar(8000) NULL, 
	[TERR_CWID] varchar(8000) NULL, 
	[Sales_Rep_id] int NULL, 
	[match_score] int NULL, 
	[Employee_Email] varchar(8000) NULL, 
	[Position_Title] varchar(8000) NULL, 
	[Selected] bit NULL, 
	[Status] varchar(8000) NULL, 
	[GUID] varchar(8000) NULL, 
	[created_on] datetime2(6) NULL, 
	[created_by] varchar(8000) NULL, 
	[modified_on] datetime2(6) NULL, 
	[modified_by] varchar(8000) NULL
);