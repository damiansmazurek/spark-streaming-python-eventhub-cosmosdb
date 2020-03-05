# Databricks notebook source
# DBTITLE 1,Configuration of Structure Streaming from EventHub
conf = {}
conf["eventhubs.connectionString"] = "Endpoint=sb://{namespace name}.servicebus.windows.net/;SharedAccessKeyName=receiver;SharedAccessKey=.........;EntityPath={hubname}"

stream_df = (
  spark
    .readStream
    .format("eventhubs")
    .options(**conf)
    .load()
)

# COMMAND ----------

# DBTITLE 1,Write stream into memory
serialized_df = stream_df.select(stream_df["body"].cast("string"), stream_df["enqueuedTime"], stream_df["partition"])

query_stream = (
  serialized_df
    .writeStream
    .format("memory")
    .queryName("rawdata")
    .start()
)


# COMMAND ----------

# MAGIC %sql select count(*) as messages, partition  from rawdata group by partition order by partition

# COMMAND ----------

# MAGIC %sql select count(*) as messages, unix_timestamp(enqueuedTime) as timestamp  from rawdata group by timestamp order by timestamp 

# COMMAND ----------

# DBTITLE 1,Save data to CosmosDB
from pyspark.sql import functions as F

writeConfig = {
 "Endpoint" : "yours_cosmosdbenpoint",
 "Masterkey" : "",
 "Database" : "db name",
 "Collection" : "collection/container name",
"Upsert": "true",
    "WritingBatchSize": "500"
}

maxtransmision_df = serialized_df.withWatermark("enqueuedTime", "10 minutes").groupby(F.unix_timestamp("enqueuedTime")).count()

streamingQueryWriter = (
  maxtransmision_df
    .writeStream
    .format("com.microsoft.azure.cosmosdb.spark.streaming.CosmosDBSinkProvider")
    .outputMode("update")
    .options(**writeConfig)
    .option("checkpointLocation", "/streamingcheckpointlocation")
    .start()
)
