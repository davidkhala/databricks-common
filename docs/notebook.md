> Formatting cells with language Scala is unsupported.

# Databricks notebook authentication
> Default Databricks notebook authentication works only on the cluster’s driver node
- not supported in cluster’s worker or executor nodes.

runtime context
- SparkContext: Use RDD API only. Initialized by default as `sc`
  - > sc is not supported on serverless compute, consider using `spark` instead. 
- SparkSession: Initialized by default as `spark`