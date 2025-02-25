https://docs.databricks.com/en/connect/streaming/pub-sub.html
> This connector provides exactly-once processing (best effort)


# Limit
> Pub/Sub might publish duplicate records, and records might arrive to the subscriber out of order. You should write Databricks code to handle duplicate and out-of-order records.
- For dedup, you need to do it in memory table