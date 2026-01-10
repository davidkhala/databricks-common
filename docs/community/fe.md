[wiki](https://github.com/davidkhala/databricks-common/wiki/Community#free-edition)

# Free Edition


## limit
- No Preview: you cannot opt-in any beta features, neither workspace-level nor account-level
- up to 1 SQL warehouses
  - > You have reached the maximum number of SQL warehouses. Delete an existing warehouse to create a new one.
- You cannot deploy app under **Compute** / [Apps]
  - since it is servermore
  - `**Compute error** \n Failed to create app service principal due to unexpected error.`
- You are not allowed to create catalog via API
  - `databricks.sdk.errors.platform.InvalidParameterValue: Please use the UI to create a catalog with Default Storage.`