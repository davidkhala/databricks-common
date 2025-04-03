# [pip `databricks-connect`](https://pypi.org/project/databricks-connect/)
- `databricks-connect` is not open source.
- Its dependency numpy `1.26.4` requires g++ for local build
- It conflicts with pypi `pyspark`
# test
A connection attempt like ping

prerequisite
- login already
## Inline test run
```shell
pip install pipx
pipx run databricks-connect test
```
## Global test run
It is not recommended
- it will overwrite your global `numpy` installation
```
pip install databricks-connect --break-system-packages
databricks-connect test
```

# Difference between Spark Connect
python module paths

| class | import from Spark Connect | import from Databricks Connect|
| ---- | ---- | ---- |
| `StreamingQuery` | `from pyspark.sql.connect.streaming import StreamingQuery` | `from pyspark.sql.connect.streaming.query import StreamingQuery` |
| `DataStreamWriter` | `from pyspark.sql.connect.streaming import DataStreamWriter` | `from pyspark.sql.connect.streaming.readwriter import DataStreamWriter`|
