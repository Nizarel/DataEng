# Databricks notebook source


spark.sql("drop table if exists RfidBronze")


dbutils.fs.rm("dbfs:/user/hive/warehouse/RfidBronze", True)
dbutils.fs.rm("/chekpoint/RfidBronze", True)

# COMMAND ----------

from pyspark.sql.functions import explode, flatten, split, from_json, col, struct, create_map, to_json, expr, lit, array
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, MapType, ArrayType

class WMSRFIDTracking:    
    def __init__(self, eh_name, connection_string):
        self.base_data_dir = "/FileStore/rfid_streaming"
        self.eh_conf = {
            'eventhubs.connectionString': sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string),
            'ehName': eh_name,
            'eventhubs.consumerGroup': 'clientb'
        }
        

    def getSchema(self):
        self.schema = StructType([
                StructField("CRC", StringType(), True),
                StructField("PC", StringType(), True),
                StructField("TID", StringType(), True),
                StructField("antenna", IntegerType(), True),
                StructField("channel", DoubleType(), True),
                StructField("eventNum", IntegerType(), True),
                StructField("format", StringType(), True),
                StructField("idHex", StringType(), True),
                StructField("peakRssi", IntegerType(), True),
                StructField("phase", IntegerType(), True),
                StructField("timestamp", StringType(), True),
                StructField("type", StringType(), True)
        ])
        return self.schema

    
    def ReadRFIDRawStream(self):
        return (spark \
            .readStream \
            .format("eventhubs") \
            .options(**self.eh_conf) \
            .load()
            #.select(F.to_json(F.struct("*")).alias("body"))  
            #.withColumn('reading', from_json(col('body').cast('string'), self.getSchema())) \
            #.select('reading.*')
            #df_to_send = dfSource.select(F.to_json(F.struct("*")).alias("body"))          
              
         )

        
    def appendRawDelta(self, stageRawDF, deltaTab):
        return (stageRawDF.writeStream
                    .format("delta")
                    .option("checkpointLocation", f"{self.base_data_dir}/chekpoint/{deltaTab}")
                    .outputMode("append")
                    .toTable(deltaTab)
        )

# COMMAND ----------

eh_name = "iothubrfid"
connection_string = dbutils.secrets.get(scope="tjx", key="iotrfid")


livestream = WMSRFIDTracking(eh_name, connection_string)
df = livestream.ReadRFIDRawStream()
newdf = livestream.ReadRFIDRawStream2()
livestream.appendRawDelta(df,"RfidBronze7")
livestream.appendRawDelta(newdf,"NewRfidBronze7")

readInStreamBody = df.withColumn("body", df["body"].cast("string"))

df3 = df.select(F.to_json(F.struct("*")).alias("body")) 
livestream.appendRawDelta(df3,"RfidBronze9")
# Convert the 'body' column to string type and parse JSON using the specified schema
#df1 = df.withColumn("body", df["body"].cast("string"))
#df2 = df1.withColumn("exploded_data", from_json("body", schema))
#df3 = df2.select("exploded_data.timestamp", "exploded_data.type", "exploded_data.data.*")

display(df3)
readInStreamBody.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from RfidBronze7

# COMMAND ----------

from pyspark.sql.functions import split, get_json_object, col, to_unix_timestamp

dfBronze = spark.table("RfidBronze7")

schema555 = StructType([
    StructField("CRC", StringType(), True),
    StructField("PC", StringType(), True),
    StructField("TID", StringType(), True),
    StructField("antenna", IntegerType(), True),
    StructField("channel", DoubleType(), True),
    StructField("eventNum", IntegerType(), True),
    StructField("format", StringType(), True),
    StructField("idHex", StringType(), True),
    StructField("peakRssi", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])

dlmr = dfBronze.selectExpr("CAST(body AS STRING)", "systemProperties").withColumn("string", col("body").cast("string")) \
    .withColumn("string", expr("replace(string, '[', '')")) \
    .withColumn("string", expr("replace(string, ']', '')")) \
    .withColumn("string", expr("replace(string, '{\"data\":', '')")) \
    .withColumn("string", expr("replace(string, '\"reads\":1},', '')"))

decoded_df2 = dlmr.select(from_json(col("string"), schema555).alias("Payload"), col("systemProperties.iothub-connection-device-id").alias("device-id"))

rfidDF = decoded_df2.select('Payload.idHex','Payload.timestamp', 'device-id')

rfidDF2 = rfidDF.select("*") \
                .withColumnRenamed('idHex','RfidTag') \
                .withColumn('timestamp', col('timestamp').cast('timestamp'))  \
                .withColumn('epochTimestamp', (col('timestamp').cast('double') * 1000).cast('long'))



rfidDF2.display()


# COMMAND ----------

rfidDF2.printSchema()
