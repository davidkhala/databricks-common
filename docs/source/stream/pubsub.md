pubsub subscription delay  
- around 20~30 seconds more to stream into Databricks table than subscribe from pubsub python sdk (native)

writeStream limits
- API `.foreach` and `.foreachBatch` are not supported (could not be triggered)