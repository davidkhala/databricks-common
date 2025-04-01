from databricks.sdk.runtime import dbutils, spark
from davidkhala.spark.sink.jdbc import SQLServer

dbutils.widgets.text("source_table", "samples.nyctaxi.trips")
dbutils.widgets.text("target_table", "trips")

# Choose "append" to add rows to the table or "overwrite" to replace the table
dbutils.widgets.dropdown("mode", "append", ["append", "overwrite"])

source_df = spark.read.table(dbutils.widgets.get("source_table"))

# Write the DataFrame to SQL Server using JDBC
batch_write = SQLServer(source_df,
                        server=dbutils.secrets.get(scope="sqlserver", key="server"),
                        database=dbutils.secrets.get(scope="sqlserver", key="database"),
                        table=dbutils.widgets.get("target_table"),
                        user=dbutils.secrets.get(scope="sqlserver", key="user"),
                        password=dbutils.secrets.get(scope="sqlserver", key="password"),
                        mode=dbutils.widgets.get("mode"),
                        )
batch_write.start()

# also accept number type
dbutils.notebook.exit(source_df.count())
