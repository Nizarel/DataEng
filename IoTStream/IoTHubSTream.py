# Databricks notebook source
# #!pip install azure-eventhub
# from pyspark.sql.functions import explode, split, from_json, col, struct, create_map, to_json
# from pyspark.sql import functions as F
# from pyspark.sql.types import StructType, StructField, StringType, LongType, FloatType, BooleanType

# dbutils.widgets.text("tjxrm", "tjxrm")

# schema = StructType([
#   StructField("ident", StringType()),
#   StructField("timestamp", LongType()),
#   StructField("position.latitude", FloatType()),
#   StructField("position.longitude", FloatType()),
#   StructField("can.vehicle.speed", LongType()),
#   StructField("can.fuel.level", LongType()),
#   StructField("can.fuel.consumed", FloatType()),
#   StructField("can.vehicle.mileage", FloatType()),
#   StructField("can.engine.ignition.status", BooleanType()),
#   StructField("gsm.signal.level", LongType()),
#   StructField("movement.status", BooleanType()),
#   StructField("din.2", BooleanType()),
#   StructField("din.3", BooleanType())
#   ])

# IOT_CS = "Endpoint=sb://iothub-ns-tjxrm-25438612-f8fb4cd2a3.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=1XFYLAdzaHII+Q/aEoMqAnzKhIUFT7I1gAIoTEMFnGw=;EntityPath=tjxrm" # dbutils.secrets.get('iot','iothub-cs') # IoT Hub connection string (Event Hub Compatible)
# ehConf = { 
#   'eventhubs.connectionString':sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(IOT_CS),
#   'ehName':dbutils.widgets.get("tjxrm")
# }


# deviceDF = spark \
#     .readStream \
#     .format("eventhubs") \
#     .options(**ehConf) \
#     .load() \
#     .withColumn('reading', from_json(col('body').cast('string'), schema)) \
#     .select('reading.*')

# deviceDF = deviceDF.withColumn('geojson_point', to_json(create_map(struct(col("`position.longitude`").alias('lng')), struct(col("`position.latitude`").alias('lat')))))

# COMMAND ----------

from pyspark.sql.functions import explode, split, from_json, col, struct, create_map, to_json, expr
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, FloatType, BooleanType

class WacomosVehiculeStream:
    def __init__(self, eh_name, connection_string):
        self.eh_conf = {
            'eventhubs.connectionString': sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string),
            'ehName': eh_name
        }
        self.schema = StructType([
            StructField("ident", StringType()),
            StructField("timestamp", LongType()),
            StructField("position.latitude", FloatType()),
            StructField("position.longitude", FloatType()),
            StructField("can.vehicle.speed", LongType()),
            StructField("can.fuel.level", LongType()),
            StructField("can.fuel.consumed", FloatType()),
            StructField("can.vehicle.mileage", FloatType()),
            StructField("can.engine.ignition.status", BooleanType()),
            StructField("gsm.signal.level", LongType()),
            StructField("movement.status", BooleanType()),
            StructField("din.2", BooleanType()),
            StructField("din.3", BooleanType())
        ])
        self.dataframe = None
        
    def ReadVehRawStream(self):
         return (spark \
            .readStream \
            .format("eventhubs") \
            .options(**self.eh_conf) \
            .load() \
            .withColumn('reading', from_json(col('body').cast('string'), self.schema)) \
            .select('reading.*') \
            #.withColumnRenamed('ident', 'DeviceId') \
            #.withColumn('Position', struct(lit('Point').alias('type'), array(col('`position.longitude`'), col('`position.latitude`')).alias('coordinates'))))
         )
                   # .withColumn('Position', to_json(create_map(
                #struct(col("`position.longitude`").alias('lng')), 
                #struct(col("`position.latitude`").alias('lat')))
    def StageRawStream(self, VehRawDF):
        return (VehRawDF.withColumn("DeviceID", expr("ident"))
                        .withColumn('Position', struct(lit('Point').alias('type'), array(col('`position.longitude`'), col('`position.latitude`')).alias('coordinates')))
                        .withColumn("timestam", expr("timestam"))
                )

    def start(self):
        if not self.dataframe:
            self.dataframe = self.ReadVehRawStream ()
    
    def stop(self):
        for query in spark.streams.active:
            if query.name == self.eh_conf['ehName']:
                query.stop()
                self.dataframe = None
        
    def display_dataframe(self):
        if self.dataframe:
            display(self.dataframe)


# COMMAND ----------

# Instantiate EventHubStream object
eh_name = "tjxrm"
connection_string = "Endpoint=sb://iothub-ns-tjxrm-25438612-f8fb4cd2a3.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=1XFYLAdzaHII+Q/aEoMqAnzKhIUFT7I1gAIoTEMFnGw=;EntityPath=tjxrm"
my_event_hub_stream = WacomosVehiculeStream(eh_name, connection_string)

# Start the stream
my_event_hub_stream.start()

# Display resulting dataframe
my_event_hub_stream.display_dataframe()

# Stop the stream
# my_event_hub_stream.stop()
