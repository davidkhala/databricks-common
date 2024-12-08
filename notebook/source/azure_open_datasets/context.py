catalog = 'azureopendatastorage'
default_volume = 'volume'

from databricks.sdk.runtime import spark


def copy_to_current():
    schema = "nyctlc"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    tables = ["yellow", "green", "fhv"]
    for table in tables:
        df = spark.table(f"{catalog}.{schema}.{table}")
        df.write.mode("overwrite").saveAsTable(f"{schema}.{table}")
