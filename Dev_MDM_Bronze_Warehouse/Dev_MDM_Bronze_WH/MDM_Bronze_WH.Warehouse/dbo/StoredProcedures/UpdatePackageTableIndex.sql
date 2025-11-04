-- UPDATE dbo.package_y
-- SET Column1 = 'NewValue', Column2 = 123
-- WHERE SomeColumn = 'SomeCondition';


-- UPDATE dbo.package_table_list_cache
-- SET master_table_id=99
-- WHERE table_name='table_list_cache';


-- SELECT * FROM dbo.package_table_list_cache;


CREATE PROCEDURE dbo.UpdatePackageTableIndex
    @KeyValue NVARCHAR(256),
    @NewIndexValue INT
AS
BEGIN
    SET NOCOUNT ON;

    -- Simple UPDATE (GraphQL supports this in Warehouse, not Lakehouse)
    UPDATE dbo.package_table_selection
    SET [Index] = @NewIndexValue
    WHERE Table_Name = @KeyValue;

    -- Return the updated row
    SELECT Table_Name, [Index]
    FROM dbo.package_table_selection
    WHERE Table_Name = @KeyValue;
END;