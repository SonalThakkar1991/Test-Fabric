from notebookutils import mssparkutils

def run_dynamic_sql(table_name, index_value):
    sql = f"""
    UPDATE dbo.{table_name}
    SET Index = {index_value}
    WHERE Table_Name = '{table_name}'
    """
    spark.sql(sql)

# Accept values from API/job parameters
table_name = mssparkutils.env.getJobInput("table_name")
index_value = mssparkutils.env.getJobInput("index_value")

run_dynamic_sql(table_name, index_value)