https://docs.databricks.com/en/connect/streaming/pub-sub.html
> This connector provides exactly-once processing (best effort)


But 
> Pub/Sub might publish duplicate records, and records might arrive to the subscriber out of order. You should write Databricks code to handle duplicate and out-of-order records.
- For dedup, try [Incremental batch](https://docs.databricks.com/en/connect/streaming/pub-sub.html#incremental-batch-processing-semantics-for-pubsub)