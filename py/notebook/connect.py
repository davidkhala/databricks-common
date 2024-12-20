class SparkWare:
    def __init__(self, spark_instance=None):
        if not spark_instance:
            from databricks.sdk.runtime import spark
            self.spark = spark
        else:
            self.spark = spark_instance
        self.catalog = self.spark.catalog.currentCatalog()
        self.schema = self.spark.catalog.currentDatabase()
