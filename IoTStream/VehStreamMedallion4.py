# Databricks notebook source
from pyspark.sql.functions import explode, split, from_json, col, struct, create_map, to_json, expr, lit, array
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, FloatType, BooleanType

class WMSLiveTrackingBronze:
    def __init__(self, eh_name):
        self.base_data_dir = "/FileStore/wcm_streaming"
        #self.BatchSize = 2000
        self.eh_conf = {
            'eventhubs.connectionString': sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string),
            'ehName': eh_name
        }
    
    def getSchemaCanBus(self):
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
      try:
        stream = spark \
            .readStream \
            .format("eventhubs") \
            .options(**self.eh_conf) \
            .load() \
            .withColumn('reading', from_json(col('body').cast('string'), self.getSchemaCanBus())) \
            .select('reading.*')  
        
        return stream

      except Exception as e:
        print("Failed to read stream from Event Hubs: ", str(e))
        raise e
    

    def process(self):
      try:
        print("Starting Vehicules Processing Stream...")
        canbusrawDF = self.ReadVehRawStream()
        #VehiculeCircuitInfosDF = self.ReadRefrWCMRaw("dbo.Vw_VehiculeCircuitInfos")
        
        sQuery1 = (canbusrawDF
                    .writeStream
                    .format("delta")
                    .option("checkpointLocation", f"{self.base_data_dir}/chekpoint/VehiculesStreamBronze")
                    .outputMode("append")
                    .table("VehiculesStreamBronze"))
        
#        sQuery2 = (VehiculeCircuitInfosDF
#                    .write
#                    .format("delta")
#                    .mode("overwrite")
#                    .saveAsTable("VehiculeCircuitInfos"))
        
        print("Done")
        return sQuery1 #, sQuery2
      
      except Exception as e:
        print("Failed to process stream: ", str(e))
        raise e

    def cleanTests(self):
        print(f"Starting Cleanup...", end='')
        spark.sql("drop table if exists VehiculesStreamBronze")
        dbutils.fs.rm("/user/hive/warehouse/VehiculesStreamBronze", True)

        dbutils.fs.rm(f"{self.base_data_dir}/chekpoint/VehiculesStreamBronze", True)

        dbutils.fs.rm(f"{self.base_data_dir}/chekpoint/VehRaw", True)

        dbutils.fs.mkdirs(f"{self.base_data_dir}/data/VehRaw")
        print("Done")


# COMMAND ----------

eh_name = "tjxrm"
connection_string = dbutils.secrets.get(scope="tjx", key="IoTCon")
bronzeTracking = WMSLiveTrackingBronze(eh_name)



# COMMAND ----------

class WMSLiveTrackingSilver:
    def __init__(self, connection_string):
        self.base_data_dir = "/FileStore/wcm_streaming"
        self.jdbcHostname = "srvdbwcmtanger.database.windows.net"
        self.jdbcDatabase = "WACOMOS_DATA_BACKUP"

    def ReadRefrWCM(self,TableView):
         return (spark.read
            .format("sqlserver")
            .option("host", self.jdbcHostname)
            .option("user", dbutils.secrets.get(scope = 'tjx', key = 'jdbcUsername') )
            .option("password", dbutils.secrets.get(scope = 'tjx', key = 'jdbcPwd'))
            .option("database", self.jdbcDatabase)
            .option("dbtable", TableView) 
            .option("readChangeFeed", "true")
            #.option("fetchsize", self.BatchSize)
            .load()          
         )
    def ReadRefrWCMBronze(self, VehiculeStreamDT):
        return spark.readStream.table(VehiculeStreamDT)

    def StageRawStream(self, VehRawDF):
        return (
            VehRawDF.select("*")
            .withColumnRenamed("ident", "DeviceID")
            .withColumnRenamed('timestamp','deviceTS')
            .withColumn(
                "Position",
                struct(
                    lit("Point").alias("type"),
                    array(
                        col("`position.longitude`"), col("`position.latitude`")
                    ).alias("coordinates"),
                ),
            )
            .withColumnRenamed("can.vehicle.speed", "VehicleSpeed")
            .withColumnRenamed("position.speed", "PositionSpeed")
            .withColumnRenamed("can.fuel.level", "FuelLevel")
            .withColumnRenamed("can.fuel.consumed", "FuelConsumed")
            .withColumnRenamed("can.vehicle.mileage", "VehicleMileage")
            .withColumnRenamed("can.engine.ignition.status", "IgnitionStatus")
            .withColumnRenamed("engine.ignition.on.duration", "IgnitionOnDuration")
            .withColumnRenamed("gsm.signal.level", "GsmSignalLevel")
            .withColumnRenamed("movement.status", "MovementStatus")
            .withColumnRenamed("din.2", "IsWashing")
            .withColumnRenamed("din.3", "IsSweeping")
            .drop("position.latitude")
            .drop("position.longitude")
        )
    
    def JoinRefStream (self, vStreamDF,RefDF):

        df3 = (
        vStreamDF.join(RefDF, vStreamDF.DeviceID == RefDF.DeviceID, "inner")
        .select(
            vStreamDF.DeviceID,
            vStreamDF.deviceTS,
            RefDF.VehiculeMotorise_DelegataireId,
            RefDF.VehiculeMotorise_Matricule,
            RefDF.VehiculeMotorise_TypePrestationId,
            RefDF.PlanningPassage_CircuitId,
        )
        .where(
            (vStreamDF.deviceTS >= RefDF.PlanningPassage_EpochStartTime)
            & (vStreamDF.deviceTS<= RefDF.PlanningPassage_EpochEndTime)
        )
        )
        return df3



    def appendSilverStream(self, stagedDF):
        return (
            stagedDF.writeStream.queryName("silver-processing")
            .format("delta")
            .option(
                "checkpointLocation",
                f"{self.base_data_dir}/chekpoint/VehiculesStreamSilver",
            )
            .outputMode("append")
            .toTable("VehiculesStreamSilver")
        )

#    def ReadRefrWCMBronze2(self, TableView):
#        return (
#            spark.read.format("delta")
#            .option("readChangeFeed", "true")
#            .table("VehiculeCircuitInfos")
            # .option("fetchsize", self.BatchSize)
#        )

    def process(self):
        print(f"\nStarting Silver Stream...", end="")
        RefrWCMBronzeDF = self.ReadRefrWCMBronze("VehiculesStreamBronze")

        SilverStreamDF = self.StageRawStream(RefrWCMBronzeDF)
        VehiculeCircuitInfosDF = self.ReadRefrWCM("dbo.Vw_VehiculeCircuitInfos")
        
        dfm= self.JoinRefStream (SilverStreamDF,VehiculeCircuitInfosDF)
        
        #sQuery = self.appendSilverStream(SilverStreamDF)
        #print("Done\n")
        return dfm

# COMMAND ----------



# COMMAND ----------

eh_name = "tjxrm"
connection_string = dbutils.secrets.get(scope="tjx", key="IoTCon")
bronzeTracking = WMSLiveTrackingBronze(eh_name)
silverTracking = WMSLiveTrackingSilver(connection_string)

bronzeTracking.cleanTests()
bronzeTracking.process()

#dfref = silverTracking.ReadRefrWCMBronze("VehiculesStreamBronze")

dlm = silverTracking.process()

dlm.display()

#dfref.display()

#df = livestream.ReadVehRawStream()
#dfref = silverTracking.ReadRefrWCMBronze("VehiculesStreamBronze")
#livestream.process()
#dfref.display()
#df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from VehiculesStreamSilver
