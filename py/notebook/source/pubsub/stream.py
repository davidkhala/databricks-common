from databricks.sdk.runtime import dbutils, spark

# Databricks notebook source
# This notebook will run continuously to listen for any messages coming to the Pub/Sub topic

dbutils.widgets.text("subscriptionId", "spark")
dbutils.widgets.text("topicId", "databricks")
dbutils.widgets.text("projectId", "gcp-data-davidkhala")

# Retrieve GCP credentials from Databricks secrets
gcp_auth_options = {
    "clientId": dbutils.secrets.get(scope="gcp", key="clientId"),
    "clientEmail": dbutils.secrets.get(scope="gcp", key="clientEmail"),
    "privateKey": dbutils.secrets.get(scope="gcp", key="privateKey"),
    "privateKeyId": dbutils.secrets.get(scope="gcp", key="privateKeyId")
}

# Set up the streaming DataFrame to listen to the Pub/Sub topic
pubsub_df = spark.readStream \
    .format("pubsub") \
    .option("subscriptionId", dbutils.widgets.get("subscriptionId")) \
    .option("topicId", dbutils.widgets.get("topicId")) \
    .option("projectId", dbutils.widgets.get("projectId")) \
    .options(**gcp_auth_options) \
    .load()
