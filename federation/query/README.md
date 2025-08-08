# query federation

## Connection

### Create

permission: a user with the `CREATE CONNECTION` privilege on the Unity Catalog metastore attached to the workspace.

by Catalog Explorer (UI)

- Easiest: `foreign catalog` creation is included

by SQL

### Drop

permission: Connection owner

```sql
DROP CONNECTION [IF EXISTS] :connection_name;
```

### Get/List

permission: `USE CONNECTION` privilege on the metastore

List

```sql
SHOW CONNECTIONS [LIKE :pattern];
```

Get

```sql
DESCRIBE CONNECTION :connection_name;
```

## `foreign catalog`

mirrors your federated data warehouse in Unity Catalog

- you can use Unity Catalog query syntax and data governance tools to manage Databricks user access to the warehouse.

### Create

permission: `CREATE CATALOG` permission on the metastore and have the `CREATE FOREIGN CATALOG` privilege on the connection.

# Migrate from legacy
<https://docs.databricks.com/aws/en/query-federation/migrate>

#
