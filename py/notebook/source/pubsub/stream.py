from databricks.sdk.runtime import dbutils, spark

# Databricks notebook source

dbutils.widgets.text("subscriptionId", "spark")
dbutils.widgets.text("topicId", "databricks")
dbutils.widgets.text("projectId", "gcp-data-davidkhala")

# Retrieve GCP credentials from Databricks secrets
options = {
    "clientId": dbutils.secrets.get(scope="gcp", key="clientId"),
    "clientEmail": dbutils.secrets.get(scope="gcp", key="clientEmail"),
    "privateKey": dbutils.secrets.get(scope="gcp", key="privateKey"),
    "privateKeyId": dbutils.secrets.get(scope="gcp", key="privateKeyId"),
    "projectId": dbutils.widgets.get("projectId")
}
from davidkhala.databricks.gcp.pubsub import PubSub, AuthOptions

pubsub = PubSub(AuthOptions(**options), spark)
pubsub_df = pubsub.read_stream(dbutils.widgets.get("topicId"), dbutils.widgets.get("subscriptionId"))
