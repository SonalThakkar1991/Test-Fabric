CREATE   PROCEDURE dbo.RegisterOrUpdateTablemaster_table_id
  @TableName NVARCHAR(256),
  @new_master_table_idValue INT
AS
BEGIN
  SET NOCOUNT ON;

  -- Validate
  IF @new_master_table_idValue < 0
    THROW 50006, 'master_table_id must be a non-negative integer.', 1;

  -- Update if exists, else insert
  IF EXISTS (SELECT 1 FROM dbo.package_table_list_cache WHERE table_name = @TableName)
  BEGIN
      UPDATE dbo.package_table_list_cache
      SET [master_table_id] = @new_master_table_idValue
      WHERE table_name = @TableName;
  END
  ELSE
  BEGIN
      INSERT INTO dbo.package_table_list_cache (table_name, [master_table_id])
      VALUES (@TableName, @new_master_table_idValue);
  END

  -- Return result (GraphQL will map this SELECT as the mutation return type)
  SELECT table_name, [master_table_id]
  FROM dbo.package_table_list_cache
  WHERE table_name = @TableName;
END;