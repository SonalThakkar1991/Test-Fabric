CREATE TABLE [dbo].[common_data_mapping_list] (

	[id] int NULL, 
	[comman_data_model_list] varchar(255) NOT NULL, 
	[silver_table_name] varchar(255) NOT NULL, 
	[source_system] varchar(100) NOT NULL, 
	[bronze_table] varchar(255) NOT NULL, 
	[Common_Data_Mapping_List_id] int NULL
);