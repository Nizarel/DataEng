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
        
    # def getSchema(self):
    #     self.schema = StructType([
    #         StructField("data", StructType([
    #             StructField("CRC", StringType()),
    #             StructField("PC", StringType()),
    #             StructField("TID", StringType()),
    #             StructField("antenna", IntegerType()),
    #             StructField("channel", IntegerType()),
    #             StructField("eventNum", IntegerType()),
    #             StructField("format", StringType()),
    #             StructField("idHex", StringType()),
    #             StructField("peakRssi", DoubleType()),
    #             StructField("phase", DoubleType()),
    #             StructField("reads", IntegerType())
    #         ])),
    #         StructField("timestamp", StringType()),
    #         StructField("type", StringType()),
    #         StructField("EventProcessedUtcTime", StringType()),
    #         StructField("PartitionId", IntegerType()),
    #         StructField("EventEnqueuedUtcTime", StringType()),
    #         StructField("IoTHub", StructType([
    #             StructField("MessageId", StringType()),
    #             StructField("CorrelationId", StringType()),
    #             StructField("ConnectionDeviceId", StringType()),
    #             StructField("ConnectionDeviceGenerationId", StringType()),
    #             StructField("EnqueuedTime", StringType()),
    #         ]))
    #     ])
    #     return self.schema

    def getSchema(self):
        self.schema = StructType([
            StructField("data", StructType([
                #StructField("CRC", StringType()),
                #StructField("PC", StringType()),
                StructField("TID", StringType(), True),
                #StructField("antenna", IntegerType()),
                #StructField("channel", IntegerType()),
                #StructField("eventNum", IntegerType()),
                #StructField("format", StringType()),
                StructField("idHex", StringType(), True)
                #StructField("peakRssi", DoubleType()),
                #StructField("phase", DoubleType()),
                #StructField("reads", IntegerType())
            ]), True),
            #StructField("timestamp", StringType()),
            #StructField("type", StringType())
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

from pyspark.sql.functions import from_json, explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

eh_name = "iothubrfid"
connection_string = dbutils.secrets.get(scope="tjx", key="iotrfid")

# Define the schema for the JSON data
schema = StructType([
    StructField("data", StructType([
        StructField("CRC", StringType()),
        StructField("PC", StringType()),
        StructField("TID", StringType()),
        StructField("antenna", IntegerType()),
        StructField("channel", IntegerType()),
        StructField("eventNum", IntegerType()),
        StructField("format", StringType()),
        StructField("idHex", StringType()),
        StructField("peakRssi", DoubleType()),
        StructField("phase", DoubleType()),
        StructField("reads", IntegerType())
    ])),
    StructField("timestamp", StringType()),
    StructField("type", StringType())
])

livestream = WMSRFIDTracking(eh_name, connection_string)
df = livestream.ReadRFIDRawStream()

# Convert the 'body' column to string type and parse JSON using the specified schema
df1 = df.withColumn("body", df["body"].cast("string"))
df2 = df1.withColumn("exploded_data", from_json("body", schema))
df3 = df2.select("exploded_data.timestamp", "exploded_data.type", "exploded_data.data.*")

display(df3)

# COMMAND ----------

readInStreamBody = df.withColumn("body", df["body"].cast("string"))

#.select(explode(from_json("body", livestream.getSchema())).alias("exploded_data"))
display(readInStreamBody)

# COMMAND ----------

eh_name = "iothubrfid"
connection_string = dbutils.secrets.get(scope="tjx", key="iotrfid")

livestream = WMSRFIDTracking(eh_name, connection_string)
df = livestream.ReadRFIDRawStream()
readInStreamBody = df.withColumn("body", df["body"].cast("string"))


livestream.appendRawDelta(readInStreamBody, "RfidBronze")
#.select(explode(from_json("body", livestream.getSchema())).alias("exploded_data"))
display(readInStreamBody)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from RfidBronze

# COMMAND ----------

from pyspark.sql.functions import split

dfBronze = spark.table("RfidBronze")

dfBronze2 = dfBronze.withColumn("body_array", split(dfBronze.body, ",")) \
                    .select(explode("body_array").alias("r"))

dfBronze2.display()

# COMMAND ----------

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType

# Define the schema for the data structure in the JSON body
schema_json = StructType([
    StructField("data", StructType([
        StructField("TID", StringType(), True),
        StructField("idHex", StringType(), True)
    ]), True)
])

schema_json4 = StructType([
    StructField("data", StructType([
        StructField("CRC", StringType(), nullable=True),
        StructField("PC", StringType(), nullable=True),
        StructField("TID", StringType(), nullable=True),
        StructField("antenna", IntegerType(), nullable=True),
        StructField("channel", DoubleType(), nullable=True),
        StructField("eventNum", IntegerType(), nullable=True),
        StructField("format", StringType(), nullable=True),
        StructField("idHex", StringType(), nullable=True),
        StructField("peakRssi", LongType(), nullable=True),
        StructField("phase", LongType(), nullable=True),
        StructField("reads", LongType(), nullable=True)
    ]), nullable=True),
    StructField("timestamp", StringType(), nullable=True)
])

# Read the data from "RfidBronze" table and parse the JSON data
dfBronze = spark.table("RfidBronze")
dfBronze2 = dfBronze.select(from_json(col("body"), schema_json4).alias("data")).select("data.*")

dfBronze3 = dfBronze.select(from_json(col("body"), schema_json).alias("data")).select("data.*")

# Display the resulting DataFrames
dfBronze.display()
dfBronze2.display()

dfBronze2.printSchema()
dfBronze3.printSchema()


# COMMAND ----------

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import json

# Define the schema for the data structure in the JSON body
schema_json = StructType([
    StructField("data", StructType([
        StructField("CRC", StringType()),
        StructField("PC", StringType()),
        StructField("TID", StringType()),
        StructField("antenna", IntegerType()),
        StructField("channel", DoubleType()),
        StructField("eventNum", IntegerType()),
        StructField("format", StringType()),
        StructField("idHex", StringType()),
        StructField("peakRssi", IntegerType()),
        StructField("phase", DoubleType()),
        StructField("reads", IntegerType())
    ])),
    StructField("timestamp", StringType()),
    StructField("type", StringType())
])

# Sample JSON string
json_str = "{\"body\":[{\"data\":{\"CRC\":\"dd3d\",\"PC\":\"3000\",\"TID\":\"Error: General Error 0x04\",\"antenna\":1,\"channel\":867.8,\"eventNum\":50,\"format\":\"epc\",\"idHex\":\"e28068900000400e7c91e063\",\"peakRssi\":-55,\"phase\":0.0,\"reads\":1},\"timestamp\":\"2024-02-01T01:25:58.569+0100\",\"type\":\"SIMPLE\"}]}"

# Parse the input JSON string
json_data = json.loads(json_str)

# Convert the JSON data to a DataFrame
df = spark.createDataFrame([json_data])

# Apply the schema to the JSON body column and extract the fields
df_with_schema = df.select(from_json(col("body"), schema_json).alias("exploded_data")).select("exploded_data.*")

# Display the resulting DataFrame
df_with_schema.show()

# COMMAND ----------

readInStreamBody.printSchema()
