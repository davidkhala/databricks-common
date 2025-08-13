# Connection

- `Snowflake warehouse`: Name of the foreign snowflake warehouse.
  - e.g. `COMPUTE_WH`
- `Database`: Database name in Snowflake that can be mapped to a Unity Catalog 'catalog' object.
  - e.g. `SNOWFLAKE_SAMPLE_DATA`

# query federation

# catalog federation

Including steps like setting up query federation, additional steps

1. create an external location for the paths to the Apache Iceberg tables registered in Snowflake Enterprise Edition (**EE**).
    - External locations are Unity Catalog securable objects that associate storage credentials with cloud storage container paths
    - using Catalog Explorer
2. Create a connection to Snowflake Horizon Catalog and create a foreign catalog.
    - You must specify a location in cloud storage where metadata will be stored for Iceberg tables in this catalog.



# Limit

- [known limits](https://docs.databricks.com/aws/en/query-federation/snowflake#catalog-federation-limitations)
