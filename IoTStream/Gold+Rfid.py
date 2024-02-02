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

class WMSLiveTrackingBronze:
    def __init__(self, eh_name, connection_string):
        self.base_data_dir = "/FileStore/wcm_streaming"
        self.eh_conf = {
            'eventhubs.connectionString': sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string),
            'ehName': eh_name,
            'eventhubs.consumerGroup': 'delta'
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

  
    def appendRawDelta(self, stageRawDF, deltaTab):
        return (stageRawDF.writeStream
                    .format("delta")
                    .option("checkpointLocation", f"{self.base_data_dir}/chekpoint/{deltaTab}")
                    .outputMode("append")
                    .toTable(deltaTab)
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

class WMSLiveTrackingSilver:
    def __init__(self):
        self.base_data_dir = "/FileStore/wcm_streaming"
        self.jdbcHostname = "srvdbwcmtanger.database.windows.net"    #"tjmr.database.windows.net"
        self.jdbcDatabase = "WACOMOS_DATA_BACKUP"                    #"WACOMOS2" "spark.databricks01"

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
            .load())
    
    def ReadRefrWCMBronze(self, VehiculeStreamDT):
        return spark.readStream.table(VehiculeStreamDT)
    
    def Sink2Hub(self,df2sink):
        # The connection string to your Event Hubs Namespace
        connectionStringAlerting = dbutils.secrets.get(scope = 'tjx', key = 'wcmevhub')
        eh_name = "wbc"
        consumergroup = "sink2"

        ehWriteConf = {
            'eventhubs.connectionString': sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionStringAlerting),
            'ehName': eh_name,
            'eventhubs.consumerGroup': consumergroup
        }

        df2sink.withColumn('body', F.to_json(F.struct(*df2sink.columns), options={"ignoreNullFields": False})) \
                  .select('body')\
                  .writeStream \
                  .format("eventhubs") \
                  .options(**ehWriteConf) \
                  .option("checkpointLocation", "/streamingHub/checkpoints") \
                  .start() #.outputMode("update") \
    
    def JoinRefStream (self, vStreamDF,VehicDF):
        return (
            vStreamDF.join(VehicDF, vStreamDF.DeviceID == VehicDF.DeviceID, "inner")
            .select(
                vStreamDF.DeviceID.alias("GPSDeviceId"),
                vStreamDF.IgnitionStatus,
                #vStreamDF.IsMoving,
                vStreamDF.Position,
                vStreamDF.deviceTS,
                VehicDF.VehiculeMotorise_Id,
                vStreamDF.IsWashing,
                vStreamDF.IsSweeping,
                vStreamDF.VehicleSpeed,
                vStreamDF.PositionSpeed,
                vStreamDF.FuelLevel,
                vStreamDF.VehicleMileage,
                VehicDF.VehiculeMotorise_DelegataireId.alias("DelegataireId"),
                VehicDF.VehiculeMotorise_TypePrestationId.alias("TypePrestationId"),
                VehicDF.PlanningPassage_CircuitId.alias("planningId"),
                VehicDF.PlanningPassage_Id.alias("CircuitId"),
                VehicDF.VehiculeMotorise_Numero,
                VehicDF.VehiculeMotorise_Matricule.alias("Matricule"),
                VehicDF.VehiculeMotorise_Modele,
            )
            .where(
                (vStreamDF.deviceTS >= VehicDF.PlanningPassage_EpochStartTime)
                & (vStreamDF.deviceTS<= VehicDF.PlanningPassage_EpochEndTime)
            )
        )
    def SinkIntoDeltaSilver(self, silverdf2sink, PrestId, DeltaTable):
        silverdf2sink.filter(col("TypePrestationId") == PrestId).writeStream.format("delta")\
        .option("checkpointLocation", f"{self.base_data_dir}/checkpoints/{DeltaTable}")\
        .outputMode("append")\
        .table(DeltaTable)

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


# COMMAND ----------

from pyspark.sql.functions import sum, expr

class WMSLiveTrackingGold():
    def __init__(self):
        self.base_data_dir = "/FileStore/wcm_streaming"
        
    def readSilver(self, deltaTbl):
        return spark.readStream.table(deltaTbl)

    def getAggregates(self, invoices_df):
        
        return (invoices_df.groupBy("planningId")
                    .agg(sum("TotalAmount").alias("TotalAmount"),
                         sum(expr("TotalAmount*0.02")).alias("TotalPoints"))
        )

    def saveResults(self, results_df):
        print(f"\nStarting Silver Stream...", end='')
        return (results_df.writeStream
                    .queryName("gold-update")
                    .option("checkpointLocation", f"{self.base_data_dir}/chekpoint/customer_rewards")
                    .outputMode("complete")
                    .toTable("customer_rewards")
                )
        print("Done")

    def process(self):
        invoices_df = self.readBronze()
        aggregate_df = self.getAggregates(invoices_df)
        sQuery = self.saveResults(aggregate_df)
        return sQuery

# COMMAND ----------

eh_name = "tjxrm"
connection_string = dbutils.secrets.get(scope="tjx", key="IoTCon")
bronze = WMSLiveTrackingBronze(eh_name, connection_string)
silver = WMSLiveTrackingSilver()

###############################################################################

bronzedf = bronze.ReadVehRawStream()

bronzedf.display()

# COMMAND ----------

eh_name = "tjxrm"
connection_string = dbutils.secrets.get(scope="tjx", key="IoTCon")
bronze = WMSLiveTrackingBronze(eh_name, connection_string)
silver = WMSLiveTrackingSilver()

###############################################################################

df = bronze.ReadVehRawStream()
df2 = bronze.StageRawStream(df)
VehiculeCircuitViewDF = silver.ReadRefrWCM("dbo.Vw_VehiculeCircuitInfos")

TrackingDF = silver.JoinRefStream(df2, VehiculeCircuitViewDF)
silver.Sink2Hub(TrackingDF)
# Collecte des DMA
DMADF = TrackingDF.filter(col("TypePrestationId") == 2)
#silver.SinkIntoCosmos(DMADF, "DMATrackingPlanId")
silver.SinkIntoDeltaSilver(DMADF, 2, "SilverDMA")
#silver.Sink2Hub(DMADF)

#Brossage Mécanique
BrossageDF = TrackingDF.filter(col("TypePrestationId") == 4)
#silver.SinkIntoCosmos(BrossageDF, "BrossageTrackingPlanId")
silver.SinkIntoDeltaSilver(BrossageDF, 4, "BrossageSilver")

# Lavage Mécanique 
LavageMecDF = TrackingDF.filter(col("TypePrestationId") == 9)
# silver.SinkIntoCosmos(LavageMecDF, "LavageMecTrackingPlanId")
silver.SinkIntoDeltaSilver(LavageMecDF, 9, "LavageMecSilver")

#Lavage de Conteneur
LavageBacDF = TrackingDF.filter(col("TypePrestationId") == 10)
# silver.SinkIntoCosmos(LavageBacDF, "LavageContTrackingPlanId")
silver.SinkIntoDeltaSilver(LavageBacDF, 10, "LavageContSilver")
# silver.Sink2Hub(LavageBacDF)

#Sink all Prestation to SilverLiveTracking Delta Table
# TrackingDF.writeStream.format("delta")\
#        .option("checkpointLocation", f"{silver.base_data_dir}/checkpoints/SilverLiveTracking")\
#        .outputMode("append")\
#        .table("SilverLiveTracking")

#VehicDF.write.format("delta").mode("overwrite").saveAsTable("ProductsBronze")
#livestream.appendInvoices(df2)

# COMMAND ----------

df2.printSchema()

# COMMAND ----------

df.display()

# COMMAND ----------

df2.display() #silver 1st level

# COMMAND ----------

TrackingDF.printSchema()

# COMMAND ----------

TrackingDF.display()

# COMMAND ----------

DMADF.display()

# COMMAND ----------

BrossageDF.display()

# COMMAND ----------

LavageBacDF.display()

# COMMAND ----------

LavageBacDF.printSchema()

# COMMAND ----------

LavageMecDF.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from SilverDMA

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from LavageMecSilver
