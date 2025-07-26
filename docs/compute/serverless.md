<img width="1010" height="568" alt="image" src="https://github.com/user-attachments/assets/c48fc6ee-05c1-4886-8a03-879c0e1dec09" />

# Serverless compute
runtime
- Python 3.10.12
- databricks-connect 14.3.4
- serverless compute utilizes Databricks secure shared access mode.
## Serverless client images
- A version grouping to wrap compatible python dependencies used for notebook and job
- Configure in notebook **Environment** panel > **Client version**
- version named like a sequence (start with `1`)


## Limit
[General limits](https://docs.databricks.com/en/compute/serverless/limitations.html)
- Due to managed by Databricks, below features are not supported
  - GLOBAL TEMPORARY VIEW
  - [Databricks Container Services](https://docs.databricks.com/en/compute/custom-containers.html)
  - [web terminal](https://docs.databricks.com/en/compute/web-terminal.html): shell access to compute
  - Spark UI
  - Spark logs when using serverless notebooks and jobs
  - Compute event logs
  - Compute policies
  - Compute-scoped init scripts
  - Environment variables
    - Solution: using widgets to create job and task parameters
  - Compute-scoped libraries
    - Solution: [notebook-scoped python libs](https://docs.databricks.com/en/libraries/notebooks-python-libraries.html)
    - > serverless compute does not support JAR file installation
      - you cannot use a JDBC/ODBC driver to ingest data from an external data source
      - Solutions:
        - SQL-based building blocks like `COPY INTO` and [`streaming tables`](https://docs.databricks.com/en/tables/streaming.html).
        - Auto Loader
        - Lakehouse Federation
        - Delta Sharing
  - Instance pools
- > Scala and R are not supported.
- > Spark RDD APIs are not supported.
  - Spark Context (sc), `spark.sparkContext`, and `sqlContext` are not supported.
- Limits for scale
  - > No query can run longer than 48 hours.
  - > Notebooks have access to 8GB memory


> Support for data sources is limited to
- AVRO
- BINARYFILE
- CSV
- DELTA
- JSON
- KAFKA
  - The Intermediate layer for other streaming system 
- ORC
- PARQUET
- ORC
- TEXT
- XML


Limits for structure Streaming
  - > There is no support for default or time-based trigger intervals. 
    - `pyspark.errors.exceptions.connect.AnalysisException: [INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED] Trigger type ProcessingTime is not supported for this cluster type.`
    - Only Trigger.AvailableNow is supported
  - Other [limits inherit from Shared Access mode compute](https://docs.databricks.com/en/compute/access-mode-limitations.html#streaming-shared)
