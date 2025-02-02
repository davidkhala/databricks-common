> Formatting cells with language Scala is unsupported.

# Databricks notebook authentication
> Default Databricks notebook authentication works only on the cluster’s driver node
- not supported in cluster’s worker or executor nodes.

runtime context
- [sc:SparkContext](https://github.com/davidkhala/spark/tree/main/context)
  - > sc is not supported on serverless compute, consider using `spark` instead. 
- [spark:SparkSession](https://github.com/davidkhala/spark/tree/main/session)