// Databricks notebook source
// This notebook will run continuously to listen any message come to pub/sub topic
 
val authOptions: Map[String, String] =
  Map("clientId" -> dbutils.secrets.get(scope = "gcp", key = "clientId"),
      "clientEmail" -> dbutils.secrets.get(scope = "gcp", key = "clientEmail"),
      "privateKey" -> dbutils.secrets.get(scope = "gcp", key = "privateKey"),
      "privateKeyId" -> dbutils.secrets.get(scope = "gcp", key = "privateKeyId")
     )

spark.readStream
  .format("pubsub")
  .option("subscriptionId", "community") 
  .option("topicId", "databricks") 
  .option("projectId", "gcp-data-davidkhala") 
  .options(authOptions)
  .load()
