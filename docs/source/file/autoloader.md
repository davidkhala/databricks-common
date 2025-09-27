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
- 当前 SQL Streaming Table 不支持 trigger=availableNow。所以只能SCHEDULE

## [schema inference and evolution](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema)

automatically detect the schema of loaded data

[Behavior mode](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema#how-does-auto-loader-schema-evolution-work)

- `addNewColumns`: The data types of existing columns remain unchanged.
  1. New columns are added to the end of schema
  2. Stream fails with `UnknownFieldException`
- `failOnNewColumns`: strict
  - Stream fails.
  - Stream does not restart unless the provided schema is updated or no conflict anymore
- `rescue`
  - Schema is not evolved
  - No failure
  - All new columns are recorded in the [rescued data column](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema#rescue).
- `none`: ignored
  - Schema is not evolved
  - No failure
  - new columns are ignored

Supported format
- JSON
- CSV
- XML: since Runtime 14.3
- Avro
- Parquet: since Runtime 11.3

