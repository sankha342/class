// Databricks notebook source
val input_path=dbutils.widgets.get("input_path")
val tmpdir=dbutils.widgets.get("tmpdir")
val batch_table=dbutils.widgets.get("batch_table")

// COMMAND ----------

// # Application (Client) ID
val applicationId = dbutils.secrets.get(scope="myScope",key="clinetId")
 
// # Application (Client) Secret Key
val authenticationKey = dbutils.secrets.get(scope="myScope",key="clientSecret")
 
// # Directory (Tenant) ID
val tenantId = dbutils.secrets.get(scope="myScope",key="tenantId")
 
 
val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> applicationId,
  "fs.azure.account.oauth2.client.secret" -> authenticationKey,
  "fs.azure.account.oauth2.client.endpoint" -> s"https://login.microsoftonline.com/$tenantId/oauth2/token")
 
// Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = s"abfss://data@siladityasg.dfs.core.windows.net/$input_path",
  mountPoint = "/mnt/DatalakeGen1",
  extraConfigs = configs)

// COMMAND ----------

val df = spark.read.format("csv").option("header","true").load("/mnt/DatalakeGen1/")

// COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.siladityasg.dfs.core.windows.net",
    dbutils.secrets.get(scope="myScope",key="accessKey"))

// COMMAND ----------

//create table and insert data into azure synapse dedicated sql pool
 
import org.apache.spark.sql.functions._
val tablename=dbutils.widgets.get("batch_table")
val tmpdir=dbutils.widgets.get("tmpdir")
// val tmpdir="/mnt/DatalakeGen5/tmpdir"
 
// This is the connection to our Azure Synapse dedicated SQL pool
// val connection = "jdbc:sqlserver://siladityasw1.sql.azuresynapse.net:1433;database=siladityapool1;user=sysadmin;password=Sankha@1991#;encrypt=true;trustServerCertificate=false;"
val connection =
"jdbc:sqlserver://siladityawspace.sql.azuresynapse.net:1433;database=siladityapool1;user=sankha342@siladityawspace;password=Sankha@1991;encrypt=true;trustServerCertificate=false;"
 
// We can use the write function to write data
df.write
  .mode("append") // Here we are saying to append to the table
  .format("com.databricks.spark.sqldw")
  .option("url", connection)
  .option("tempDir", tmpdir) // For transfering to Azure Synapse, we need temporary storage for the staging data
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", tablename)
  .save()

// COMMAND ----------

dbutils.notebook.exit("Success")
