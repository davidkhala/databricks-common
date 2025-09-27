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

- Scala and R are not supported.
- Apache Spark MLlib is not supported.

[limits inherit from Shared Access mode compute](https://docs.databricks.com/en/compute/access-mode-limitations.html#streaming-shared)
- GPUs are not supported.
- R are not supported
- Databricks Runtime for ML is not supported.

Ephemeral introduce no support for

- GLOBAL TEMPORARY VIEW

No Compute and its relevant

- Compute event logs
- Compute policies
- Compute-scoped init scripts
- [Databricks Container Services](https://docs.databricks.com/en/compute/custom-containers.html)
- [web terminal](https://docs.databricks.com/en/compute/web-terminal.html): shell access to compute
- Compute-scoped libraries
    - > serverless compute does not support JAR file installation (JAR library)
        - you cannot use a JDBC/ODBC driver to ingest data from an external data source
        - Solutions:
            - SQL-based building blocks like `COPY INTO` and [
              `streaming tables`](https://docs.databricks.com/en/tables/streaming.html).
            - Auto Loader
            - Lakehouse Federation
            - Delta Sharing
    - Solution: [notebook-scoped python libs](https://docs.databricks.com/en/libraries/notebooks-python-libraries.html)
- Environment variables
    - Solution: using widgets to create job and task parameters

Sparkless

- Spark UI
- Spark logs
- Instance pools
- Spark RDD APIs are not supported.
    - Spark Context (sc), `spark.sparkContext`, and `sqlContext` are not supported.

[Support for data sources](https://docs.databricks.com/aws/en/compute/serverless/limitations#supported-data-sources)

Limits for structure Streaming

- > There is no support for default or time-based trigger intervals.
  - Only Trigger.AvailableNow is supported
  - ```
    pyspark.errors.exceptions.connect.AnalysisException: [INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED] Trigger type ProcessingTime is not supported for this cluster type.
    ```

    

