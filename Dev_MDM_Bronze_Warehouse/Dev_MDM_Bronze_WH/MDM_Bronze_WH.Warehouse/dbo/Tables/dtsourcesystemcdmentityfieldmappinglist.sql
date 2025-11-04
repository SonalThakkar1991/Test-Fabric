CREATE TABLE [dbo].[dtsourcesystemcdmentityfieldmappinglist] (

	[SourceSystem] varchar(50) NOT NULL, 
	[SourceTableBronze] varchar(100) NOT NULL, 
	[SourceColumnBronze] varchar(100) NOT NULL, 
	[DataTypeBronze] varchar(50) NOT NULL, 
	[DestinationTableSilver] varchar(100) NOT NULL, 
	[DestinationColumnSilver] varchar(100) NOT NULL, 
	[DataTypeSilver] varchar(50) NOT NULL, 
	[Common_Data_model_Mapping_ID] int NULL, 
	[Common_Data_Mapping_List_id] int NULL, 
	[Common_Data_Mapping_Field_List_id] int NULL
);