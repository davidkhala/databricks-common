[setup query federation](https://docs.databricks.com/aws/en/query-federation/bigquery)

# Data type mappings

BigQuery type -> Spark Type

- `bignumeric`, `numeric` -> `DecimalType`
- `int64` -> `LongType`
- `float64` -> `DoubleType`
- `array`, `geography`, `interval`, `json`, `string`, `struct` -> `VarcharType`
  - JSON column need to be rebuilt
- `bytes` -> `BinaryType`
- `bool` -> `BooleanType`
- `date` -> `DateType`
- `datetime`, `time`, `timestamp`
  - case when `preferTimestampNTZ = false` or default, -> `TimestampType`
  - case when `preferTimestampNTZ = true`, -> `TimestampNTZType`

# Limits

- The following pushdowns are not supported:
  - Windows functions
