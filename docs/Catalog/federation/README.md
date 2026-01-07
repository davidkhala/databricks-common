
Connection and `foreign catalog` are common concepts for both query federation and catalog federation

# Connection

## Create

permission: a user with the `CREATE CONNECTION` privilege on the Unity Catalog metastore attached to the workspace.

by Catalog Explorer (UI)

- Easiest: `foreign catalog` creation is included

by SQL

## Drop

permission: Connection owner

```sql
DROP CONNECTION [IF EXISTS] :connection_name;
```

## Get/List

permission: `USE CONNECTION` privilege on the metastore

List

```sql
SHOW CONNECTIONS [LIKE :pattern];
```

Get

```sql
DESCRIBE CONNECTION :connection_name;
```

# `foreign catalog`

A foreign catalog is a securable object in Unity Catalog that mirrors a database in an external data system
- you can use Unity Catalog query syntax and data governance tools to manage Databricks user access to the warehouse.

## Create

permission: `CREATE CATALOG` permission on the metastore and have the `CREATE FOREIGN CATALOG` privilege on the connection.

## Manage
work in the same way as normal catalog managed by Unity Catalog


# query federation
## Migrate from legacy
<https://docs.databricks.com/aws/en/query-federation/migrate>

## Performance
[optimize](https://docs.databricks.com/aws/en/query-federation/performance-recommendations)




# catalog federation
for sources support [Hive metastore](https://docs.databricks.com/aws/en/query-federation/hms-federation-external) (HMS)
- AWS Glue

for iceberg table
- Snowflake

better pushdown capabilities
- [Salesforce Data Cloud file sharing](https://docs.databricks.com/aws/en/query-federation/salesforce-data-cloud-file-sharing)
  - magically like S3 Select
