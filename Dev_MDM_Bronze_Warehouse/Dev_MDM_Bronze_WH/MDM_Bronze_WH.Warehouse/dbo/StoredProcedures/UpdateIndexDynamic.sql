-- UPDATE dbo.package_y
-- SET Column1 = 'NewValue', Column2 = 123
-- WHERE SomeColumn = 'SomeCondition';


-- UPDATE dbo.package_table_list_cache
-- SET master_table_id=99
-- WHERE table_name='table_list_cache';


-- SELECT * FROM dbo.package_table_list_cache;


CREATE PROCEDURE dbo.UpdateIndexDynamic
    @TargetSchemaTable NVARCHAR(256),  -- e.g., 'dbo.package_table_selection'
    @KeyColumn NVARCHAR(128),           -- e.g., 'Table_Name'
    @KeyValue NVARCHAR(256),            -- the value to match
    @NewIndexValue INT
AS
BEGIN
    SET NOCOUNT ON;

    -- Optionally, validate the dynamic table exists and the columns are valid
    DECLARE @sql NVARCHAR(MAX) = N'
    UPDATE ' + QUOTENAME(@TargetSchemaTable) + '
    SET ' + QUOTENAME('Index') + ' = @idxVal
    WHERE ' + QUOTENAME(@KeyColumn) + ' = @kv
    ';

    EXEC sp_executesql
        @sql,
        N'@idxVal INT, @kv NVARCHAR(256)',
        @idxVal = @NewIndexValue,
        @kv = @KeyValue;
END;