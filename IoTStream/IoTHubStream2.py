# Databricks notebook source
from pyspark.sql.functions import explode, split, from_json, col, struct, create_map, to_json, expr, lit, array
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, FloatType, BooleanType

class WMSLiveTracking:
    def __init__(self, eh_name, connection_string):
        self.base_data_dir = "/FileStore/wcm_streaming"
        self.eh_conf = {
            'eventhubs.connectionString': sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string),
            'ehName': eh_name
        }
    
    def getSchema(self):
        self.schema = StructType([
            StructField("ident", StringType()),
            StructField("timestamp", LongType()),
            StructField("position.latitude", FloatType()),
            StructField("position.longitude", FloatType()),
            StructField("can.vehicle.speed", LongType()),
            StructField("position.speed", LongType()),
            StructField("can.fuel.level", LongType()),
            StructField("can.fuel.consumed", FloatType()),
            StructField("can.vehicle.mileage", FloatType()),
            StructField("can.engine.ignition.status", BooleanType()),
            StructField("engine.ignition.on.duration", LongType()),
            StructField("gsm.signal.level", LongType()),
            StructField("movement.status", BooleanType()),
            StructField("din.2", BooleanType()),
            StructField("din.3", BooleanType())
        ])
        return self.schema

    def ReadVehRawStream(self):
         return (spark \
            .readStream \
            .format("eventhubs") \
            .options(**self.eh_conf) \
            .load() \
            .withColumn('reading', from_json(col('body').cast('string'), self.getSchema())) \
            .select('reading.*')           
         )

    def StageRawStream(self, VehRawDF):
        return (VehRawDF.select("*")
                .withColumnRenamed('ident','DeviceID')
                .withColumn('Position', struct(lit('Point').alias('type'), array(col('`position.longitude`'), col('`position.latitude`')).alias('coordinates')))
                .withColumnRenamed('can.vehicle.speed','VehicleSpeed')
                .withColumnRenamed('position.speed','PositionSpeed')
                .withColumnRenamed('can.fuel.level','FuelLevel')
                .withColumnRenamed('can.fuel.consumed','FuelConsumed')
                .withColumnRenamed('can.vehicle.mileage','VehicleMileage')
                .withColumnRenamed('can.engine.ignition.status','IgnitionStatus')
                .withColumnRenamed('engine.ignition.on.duration','IgnitionOnDuration')
                .withColumnRenamed('gsm.signal.level','GsmSignalLevel')
                .withColumnRenamed('movement.status','MovementStatus')
                .withColumnRenamed('din.2','IsWashing')
                .withColumnRenamed('din.3','IsSweeping')
                .drop("position.latitude")
                .drop("position.longitude")
                )

    def appendInvoices(self, stageRawDF):
        return (stageRawDF.writeStream
                    .format("delta")
                    .option("checkpointLocation", f"{self.base_data_dir}/chekpoint/VehRaw")
                    .outputMode("append")
                    .toTable("VehiculesStreamBronze")
        )
    def process(self):
        print(f"Starting Vehicules Processing Stream...", end='')
        canbusrawDF  = self.ReadVehRawStream()
        canbustageDF = self.StageRawStream(canbusrawDF)
        sQuery = self.appendInvoices(canbustageDF)
        print("Done\n")
        return sQuery    



# COMMAND ----------

eh_name = "tjxrm"
connection_string = dbutils.secrets.get(scope="tjx", key="IoTCon")
livestream = WMSLiveTracking(eh_name, connection_string)
df = livestream.ReadVehRawStream()

df2 = livestream.StageRawStream(df)
livestream.appendInvoices(df2)

df2.display()
