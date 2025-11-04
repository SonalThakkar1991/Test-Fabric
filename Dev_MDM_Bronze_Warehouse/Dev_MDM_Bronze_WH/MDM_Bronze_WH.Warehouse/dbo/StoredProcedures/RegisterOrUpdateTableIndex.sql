CREATE   PROCEDURE dbo.RegisterOrUpdateTableIndex
  @TableName NVARCHAR(256),
  @NewIndexValue INT
AS
BEGIN
  SET NOCOUNT ON;

  -- Validate
  IF @NewIndexValue < 0
    THROW 50006, 'Index must be a non-negative integer.', 1;

  -- Update if exists, else insert
  IF EXISTS (SELECT 1 FROM dbo.package_list_cache WHERE Table_Name = @TableName)
  BEGIN
      UPDATE dbo.package_list_cache
      SET [Index] = @NewIndexValue
      WHERE Table_Name = @TableName;
  END
  ELSE
  BEGIN
      INSERT INTO dbo.package_list_cache (Table_Name, [Index])
      VALUES (@TableName, @NewIndexValue);
  END

  -- Return result (GraphQL will map this SELECT as the mutation return type)
  SELECT Table_Name, [Index]
  FROM dbo.package_list_cache
  WHERE Table_Name = @TableName;
END;