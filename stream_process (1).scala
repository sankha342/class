// Databricks notebook source
import org.apache.spark.sql.types._
import org.apache.spark.eventhubs._
import org.apache.spark.sql.types.{StructType,StructField,StringType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode.Update

// COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.siladityasg.dfs.core.windows.net",
    dbutils.secrets.get(scope="myScope",key="accessKey"))

// COMMAND ----------

val stream_table=dbutils.widgets.get("stream_table")
val tmpdir=dbutils.widgets.get("tmpdir")
val checkpointing_path=dbutils.widgets.get("checkpointing_path")
val schema= StructType(Array(StructField("OrderID",StringType,true),StructField("Quantity",StringType,true),StructField("UnitPrice",StringType,true),StructField("DiscountCategory",StringType,true)))
 
val connectionString = "Endpoint=sb://siladityaeventhub.servicebus.windows.net/;SharedAccessKeyName=siladityaSASpolicy1;SharedAccessKey=HGeV8bvXc0QGQ0ezEcRH5lXL4/K1XM+szpUOi5E3rXY=;EntityPath=siladityaeventhub"
 
val eventHubsConf = EventHubsConf(connectionString)
 
val eventhubs = spark.readStream
  .format("eventhubs")
  .options(eventHubsConf.toMap)
  .load()
 
val data=eventhubs.withColumn("Body", $"body".cast(StringType))
 
val df=data.withColumn("Body",from_json(col("Body"),schema))
 
val df1 = df.select(col("Body.*"))
 
df1.writeStream.format("com.databricks.spark.sqldw").option("url", "jdbc:sqlserver://siladityawspace.sql.azuresynapse.net:1433;database=siladityapool1;user=sankha342;password=Sankha@1991;encrypt=true;trustServerCertificate=false;").option("tempDir", tmpdir).option("forwardSparkAzureStorageCredentials", "true").option("dbTable", stream_table).option("checkpointLocation", checkpointing_path).outputMode("Append").start()
