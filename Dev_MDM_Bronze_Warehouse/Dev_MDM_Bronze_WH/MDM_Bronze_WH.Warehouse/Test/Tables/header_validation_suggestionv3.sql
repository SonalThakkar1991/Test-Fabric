CREATE TABLE [Test].[header_validation_suggestionv3] (

	[run_id] varchar(40) NULL, 
	[ran_at] varchar(80) NULL, 
	[master_table_GUID] varchar(50) NULL, 
	[flag] varchar(200) NULL, 
	[executed_by] varchar(200) NULL, 
	[status] varchar(50) NOT NULL, 
	[created_by] varchar(50) NOT NULL, 
	[created_on] varchar(50) NOT NULL, 
	[modified_by] varchar(50) NOT NULL, 
	[modified_on] varchar(50) NOT NULL
);