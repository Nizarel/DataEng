# Databricks notebook source
from pyspark.sql.functions import explode, split, from_json, col, struct, create_map, to_json, expr, lit, array
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

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
            StructField('data.idHex', StringType()),
            StructField('IoTHub.ConnectionDeviceId', StringType()),           
            StructField('timestamp', TimestampType())

        ])
        return self.schema
    
    def ReadRFIDRawStream(self):
        return (spark \
            .readStream \
            .format("eventhubs") \
            .options(**self.eh_conf) \
            .load() 
           # .withColumn('reading', from_json(col('body').cast('string'), self.getSchema())) \
           # .select('reading.*')        
              
         )

# COMMAND ----------

eh_name = "iothubrfid"
connection_string = dbutils.secrets.get(scope="tjx", key="iotrfid")

livestream = WMSRFIDTracking(eh_name, connection_string)
df = livestream.ReadRFIDRawStream()

# COMMAND ----------

df.display()
