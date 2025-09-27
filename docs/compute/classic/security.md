# [permissions](https://learn.microsoft.com/en-us/azure/databricks/compute/use-compute#permissions)
- `CAN ATTACH TO`: Allows you to attach your notebook to compute and view the compute metrics and Spark UI.
- `CAN RESTART`: Allows you to start, restart, and terminate compute. Includes `CAN ATTACH TO`.
- `CAN MANAGE`: Allows you to edit compute details, permissions, and size. Includes `CAN RESTART` permission.
- `NO PERMISSIONS`: No permissions on the compute.


 

# Compute Policy
- Requires Premium plan

It used to 
- limit a user or group’s compute creation permissions (max cluster count, max cost per cluster)
- Simplify UI to enable more users to create own clusters
  - grouping permission rules 
- Apply cluster-scoped library installations

Default policies (prebuilt)
## Personal Compute
- available to all users in your workspace
- a single-node compute resource
- minimal configuration option

## Shared Compute
- Allow more compute resources (multi-node)
- Uses the Shared access mode
- Defaults to latest Databricks **Runtime LTS** version
- Can only be used by workspace admins

## Power User Compute
- Similar to **Shared Compute** policy
- Defaults to the latest Databricks **Runtime ML** version (newer than latest LTS)
