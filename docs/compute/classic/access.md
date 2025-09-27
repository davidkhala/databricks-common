Compute access modes
# access mode: who can run
- aka. shared access mode
- permission: Allows any user with `CAN ATTACH TO` permission to attach and concurrently execute workloads
  - achieved by Lakeguard's user workload isolation,

## Limit
- R not supported
- Databricks Runtime for ML is not supported.
  - Solution: install any ML library not bundled with the Databricks Runtime as a compute-scoped library.
  - GPU-enabled compute is not supported.
- clients (incl. DBUtils) can only read from cloud storage using an external location.
- Custom containers are not supported.
- DBFS root and mounts do not support FUSE.
  - FUSE: Filesystem in Userspace. 允许用户在不修改内核的情况下创建自己的文件系统
  - You cannot access by POSIX-style path. e.g `/dbfs/...`
  - Solution： by `dbutils.fs` with path `dbfs:/...`
- [Scala kernel limit](https://docs.databricks.com/aws/en/compute/standard-limitations#scala-kernel-limitations)

sparkless
- RDD APIs are not supported.
  - Spark Context (sc), `spark.sparkContext` are not supported in scala
    - including sc dependent `sqlContext`
  - Solution: use sparkSession
  - Solution: use Python
- Spark-submit job tasks are not supported.
  - solution: use [JAR task](https://docs.databricks.com/aws/en/jobs/jar)
- When creating a DataFrame from local data using spark.createDataFrame, row sizes cannot exceed 128MB.
- config `spark.executor.extraJavaOptions` is not supported.
# Dedicated
permission: The compute resource is assigned to a single user or group.