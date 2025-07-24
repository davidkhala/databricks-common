# Streaming Object Storage: [Auto Loader](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/index.html)
Auto Loader incrementally processes new data files as they arrive in cloud storage.
- built upon Spark Structured Streaming
- ? depends on classic compute

```python
spark.readStream.format("cloudFiles").load("/Volumes/catalog/schema/files")
```

和其他streaming table建表方式一样
```SQL
CREATE OR REFRESH STREAMING TABLE <table>
SCHEDULE EVERY 1 hour
AS SELECT <columns>
FROM STREAM read_files(
    '<dir_path>',
)
```

