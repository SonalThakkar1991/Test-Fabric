CREATE TABLE [dbo].[validation_execution_master] (

	[Table_Name] varchar(8000) NULL, 
	[Table_Type] varchar(8000) NULL, 
	[Table_Description] varchar(8000) NULL, 
	[Column_Name] varchar(8000) NULL, 
	[AI_ColumnName] varchar(8000) NULL, 
	[validation_rule] varchar(8000) NULL, 
	[Val_Description] varchar(8000) NULL, 
	[AI_Reasoning] varchar(8000) NULL, 
	[Simplicity] varchar(8000) NULL, 
	[Index_Key] varchar(8000) NULL, 
	[Minimum_Range] varchar(8000) NULL, 
	[Maximum_Range] varchar(8000) NULL, 
	[Maximum_Date_Range] varchar(8000) NULL, 
	[Minimum_Date_Range] varchar(8000) NULL, 
	[Condition] varchar(8000) NULL, 
	[Selected] varchar(8000) NULL, 
	[Business_Rule_Name] varchar(8000) NULL, 
	[create_date] varchar(8000) NULL, 
	[SampleData] varchar(8000) NULL
);