# Databricks notebook source
# MAGIC %sql
# MAGIC DESCRIBE DETAIL SilverDMA

# COMMAND ----------

spark.sql("drop table if exists DMASilver")
spark.sql("drop table if exists LavageContSilver")
spark.sql("drop table if exists BrossageSilver")
spark.sql("drop table if exists LavageMecSilver")
spark.sql("drop table if exists SilverLiveTracking")

dbutils.fs.rm("dbfs:/user/hive/warehouse/silverdma", True)
dbutils.fs.rm("/chekpoint/dmasilver", True)

dbutils.fs.rm("dbfs:/user/hive/warehouse/lavagecontsilver", True)
dbutils.fs.rm("/chekpoint/lavagecontsilver", True)

dbutils.fs.rm("dbfs:/user/hive/warehouse/brossagesilver", True)
dbutils.fs.rm("/chekpoint/brossagesilver", True)

dbutils.fs.rm("dbfs:/user/hive/warehouse/lavagemecsilver", True)
dbutils.fs.rm("/chekpoint/LavageMecSilver", True)

dbutils.fs.rm("dbfs:/user/hive/warehouse/silverlivetracking", True)
dbutils.fs.rm("/chekpoint/SilverLiveTracking", True)

# COMMAND ----------

import uuid
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
                .withColumnRenamed('timestamp','deviceTS')
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
    # def SinkIntoCosmos(self, df2sink, container):
    #     cosmosEndpoint = "https://wcmdev.documents.azure.com:443/"
    #     cosmosDatabaseName = "tracking-db"
        
    #     writeConfig = {
    #     "spark.cosmos.accountEndpoint" : cosmosEndpoint,
    #     "spark.cosmos.accountKey" : dbutils.secrets.get(scope = "tjx", key = "CosmosKey"),
    #     "spark.cosmos.database" : cosmosDatabaseName,
    #     "spark.cosmos.container" : container
    #   #  "spark.cosmos.write.upsertEnabled": "true"
    #     }
    #     df2sink2=df2sink.withColumn("id", lit(str(uuid.uuid1())))
    #     df2sink2\
    #     .writeStream\
    #     .outputMode("append")\
    #     .option("checkpointLocation", f"{self.base_data_dir}/chekpoint/CosmosDMA")\
    #     .format("cosmos.oltp")\
    #     .options(**writeConfig)\
    #     .start()
    def SinkIntoCosmos(self, df2sink, container):
        cosmosEndpoint = "https://wcmdev.documents.azure.com:443/"
        cosmosDatabaseName = "tracking-db"

        # Define connection options
        writeConfig = {
        "spark.cosmos.accountEndpoint": cosmosEndpoint,
        "spark.cosmos.accountKey": dbutils.secrets.get(scope="tjx", key="CosmosKey"),
        "spark.cosmos.database": cosmosDatabaseName,
        "spark.cosmos.container": container     
        }

        # Define a batch function that will be called for each batch of data
        def process_batch(df, batchId):
            df.withColumn("id", lit(str(uuid.uuid1()))) \
            .write \
            .format("cosmos.oltp") \
            .options(**writeConfig) \
            .mode("append") \
            .save()

    # Write the data to Cosmos DB in batches using foreachBatch
        df2sink.writeStream \
            .outputMode("append") \
            .option("checkpointLocation", f"{self.base_data_dir}/chekpoint/{container}") \
            .foreachBatch(process_batch) \
            .start()
    
    def SinkIntoDeltaSilver(self, silverdf2sink, PrestId, DeltaTable):
        silverdf2sink.filter(col("TypePrestationId") == PrestId).writeStream.format("delta")\
        .option("checkpointLocation", f"{livestream.base_data_dir}/checkpoints/{DeltaTable}")\
        .outputMode("append")\
        .table(DeltaTable)


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

import pyspark.sql.functions as F
from pyspark.sql.functions import col, to_json, struct
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType,DateType,LongType,IntegerType,TimestampType
from multiprocessing.pool import ThreadPool

#JDBC connect details for SQL Server database
jdbcHostname = "tjmr.database.windows.net"
jdbcDatabase = "WACOMOS2"
jdbcUsername = "Adminu"
sqlpwd       =  dbutils.secrets.get(scope="tjx", key="jdbcPwd2")
base_data_dir = "/FileStoreTest/orders"
eh_name = "tjxrm"
connection_string = dbutils.secrets.get(scope="tjx", key="IoTCon")

VehicDF = (spark.read
  .format("sqlserver")
  .option("host", jdbcHostname)
  .option("user", jdbcUsername)
  .option("password", sqlpwd)
  .option("database", jdbcDatabase)
  .option("dbtable", "dbo.Vw_VehiculeCircuitInfos") # (if schemaName not provided, default to "dbo")
  .option("readChangeFeed", "true")
  .load()
)

livestream = WMSLiveTracking(eh_name, connection_string)
df = livestream.ReadVehRawStream()
df2 = livestream.StageRawStream(df)
TrackingDF = (
    df2.join(VehicDF, df2.DeviceID == VehicDF.DeviceID, "inner")
    .select(
        df2.DeviceID.alias("GPSDeviceId"),
        df2.IgnitionStatus,
        #df2.IsMoving,
        df2.Position,
        df2.deviceTS,
        VehicDF.VehiculeMotorise_Id,
        df2.IsWashing,
        df2.IsSweeping,
        df2.VehicleSpeed,
        df2.PositionSpeed,
        df2.FuelLevel,
        df2.VehicleMileage,
        VehicDF.VehiculeMotorise_DelegataireId.alias("DelegataireId"),
        VehicDF.VehiculeMotorise_TypePrestationId.alias("TypePrestationId"),
        VehicDF.PlanningPassage_CircuitId.alias("planningId"),
        VehicDF.PlanningPassage_Id.alias("CircuitId"),
        VehicDF.VehiculeMotorise_Numero,
        VehicDF.VehiculeMotorise_Matricule.alias("Matricule"),
        VehicDF.VehiculeMotorise_Modele,
    )
    .where(
        (df2.deviceTS >= VehicDF.PlanningPassage_EpochStartTime)
        & (df2.deviceTS<= VehicDF.PlanningPassage_EpochEndTime)
    )
)

# Collecte des DMA
DMADF = TrackingDF.filter(col("TypePrestationId") == 2)
livestream.SinkIntoCosmos(DMADF, "DMATrackingPlanId")
livestream.SinkIntoDeltaSilver(DMADF, 2, "SilverDMA")


#Brossage Mécanique
BrossageDF = TrackingDF.filter(col("TypePrestationId") == 4)
livestream.SinkIntoCosmos(BrossageDF, "BrossageTrackingPlanId")
livestream.SinkIntoDeltaSilver(BrossageDF, 4, "BrossageSilver")

# Lavage Mécanique 
LavageMecDF = TrackingDF.filter(col("TypePrestationId") == 9)
livestream.SinkIntoCosmos(LavageMecDF, "LavageMecTrackingPlanId")
livestream.SinkIntoDeltaSilver(LavageMecDF, 9, "LavageMecSilver")

#Lavage de Conteneur
LavageBacDF = TrackingDF.filter(col("TypePrestationId") == 10)
livestream.SinkIntoCosmos(LavageBacDF, "LavageContTrackingPlanId")
livestream.SinkIntoDeltaSilver(LavageBacDF, 10, "LavageContSilver")

#Sink all Prestation to SilverLiveTracking Delta Table
TrackingDF.writeStream.format("delta")\
       .option("checkpointLocation", f"{livestream.base_data_dir}/checkpoints/SilverLiveTracking")\
       .outputMode("append")\
       .table("SilverLiveTracking")

#VehicDF.write.format("delta").mode("overwrite").saveAsTable("ProductsBronze")
#livestream.appendInvoices(df2)

# COMMAND ----------

df.display()

# COMMAND ----------

df2.display() #silver 1st level

# COMMAND ----------

TrackingDF.display()

# COMMAND ----------

DMADF.display()

# COMMAND ----------

BrossageDF.display()

# COMMAND ----------

LavageBacDF.display()

# COMMAND ----------

LavageMecDF.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from SilverDMA

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from LavageMecSilver
