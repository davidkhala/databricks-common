
dbutils.widgets.text("source_table", "`azure-open-datasets`.nyctlc.yellow")
dbutils.widgets.text("target_table", "yellow")

# Choose "append" to add rows to the table or "overwrite" to replace the table
dbutils.widgets.dropdown("mode","append", ["append", "overwrite"] )

server = dbutils.secrets.get(scope = "sqlserver", key = "server")
database = dbutils.secrets.get(scope = "sqlserver", key = "database")


source_df = spark.read.table(dbutils.widgets.get("source_table"))

# Write the DataFrame to SQL Server using JDBC
source_df.write.jdbc(
    url=f"jdbc:sqlserver://{server};databaseName={database}",
    table=dbutils.widgets.get("target_table"),
    mode=dbutils.widgets.get("mode"),  
    properties={
        "user": dbutils.secrets.get(scope = "sqlserver", key = "user"),
        "password": dbutils.secrets.get(scope = "sqlserver", key = "password"),
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
)
# also accept number type
dbutils.notebook.exit(source_df.count())
