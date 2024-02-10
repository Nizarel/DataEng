# Databricks notebook source
spark.sql("drop table if exists SilverDMA")
spark.sql("drop table if exists LavageContSilver")
spark.sql("drop table if exists BrossageSilver")
spark.sql("drop table if exists LavageMecSilver")
spark.sql("drop table if exists SilverLiveTracking")

dbutils.fs.rm("dbfs:/user/hive/warehouse/silverdma", True)
dbutils.fs.rm("/chekpoint/silverdma", True)

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
    def __init__(self, eh_name, connection_string, ConsmGrp):
        self.jdbcHostname = "srvdbwcmtanger.database.windows.net"    #"tjmr.database.windows.net"
        self.jdbcDatabase = "WACOMOS_DATA_BACKUP"                    #"WACOMOS2" "spark.databricks01"
        self.base_data_dir = "/FileStore/wcm_streaming"
        self.eh_conf = {
            'eventhubs.connectionString': sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string),
            'ehName': eh_name,
            'eventhubs.consumerGroup': ConsmGrp 
        }

    def ReadRefrDBWCM(self,TableView):
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

    def ReadVehRawStream(self):
         return (spark \
            .readStream \
            .format("eventhubs") \
            .options(**self.eh_conf) \
            .load() #\
            #.withColumn('reading', from_json(col('body').cast('string'), self.getSchema())) \
            #.select('reading.*')           
         )

    def LoadRefDelta(self, RawDF, deltaTab):
        return (RawDF.write
                    .format("delta")
                    .option("checkpointLocation", f"{self.base_data_dir}/chekpoint/{deltaTab}")
                    .mode("overwrite")
                    .saveAsTable(deltaTab)
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
from pyspark.sql.types import StringType,DateType,LongType,IntegerType,TimestampType, DoubleType
from multiprocessing.pool import ThreadPool

class WMSLiveTrackingSilver:
    def __init__(self):
        self.base_data_dir = "/FileStore/wcm_streaming"
   
    def VehiculeSchema(self):
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
            StructField("din.3", BooleanType()),
            StructField("counter.impulses.2", FloatType())
        ])
        return self.schema
    
    def RfidSchema(self):
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
            StructField("timestamp", StringType(), True)
        ])
        return self.schema
    
    def stageRfidStream(self, bronzeRfidDF):

        dlmr = bronzeRfidDF.selectExpr("CAST(body AS STRING)", "systemProperties").withColumn("string", col("body").cast("string")) \
            .withColumn("string", expr("replace(string, '[', '')")) \
            .withColumn("string", expr("replace(string, ']', '')")) \
            .withColumn("string", expr("replace(string, '{\"data\":', '')")) \
            .withColumn("string", expr("replace(string, '\"reads\":1},', '')"))

        decoded_df2 = dlmr.select(from_json(col("string"), self.RfidSchema()).alias("Payload"), col("systemProperties.iothub-connection-device-id").alias("deviceId"))

        rfidDF = decoded_df2.select('Payload.idHex','Payload.timestamp', 'deviceId')

        rfidDF2 = rfidDF.select("*") \
                        .withColumnRenamed('idHex','RfidTag') \
                        .withColumn('timestamp', col('timestamp').cast('timestamp'))  
                        #.withColumn('epochTimestamp', (col('timestamp').cast('double') * 1000).cast('long'))
        rfidDF2 = self.convert_to_epoch(rfidDF2, "timestamp")

        return rfidDF2
    
    def convert_to_epoch(self, df, input_col):
        # Cast the input column to timestamp
        df = df.withColumn(input_col, F.col(input_col).cast('timestamp'))
        # Convert the timestamp to Unix timestamp (in seconds)
        epoch_seconds = F.unix_timestamp(input_col)
        # Check if the input is in milliseconds (i.e., the epoch time is a large number)
        return df.withColumn('epochTimestamp', F.when(epoch_seconds > 1000000000000, F.floor(epoch_seconds / 1000)).otherwise(epoch_seconds))
    

    # def create_geojson_point(self, coordinates, position_col):
    #     latitude = position_col.split(',')[0]
    #     longitude = position_col.split(',')[1]
    #     return {
    #         "Position": {
    #             "type": "Point",
    #             "coordinates": [latitude, longitude]
    #         }
    #     }
    
    def StageRawStream(self, VehRawDF):
        return (VehRawDF.withColumn('reading', from_json(col('body').cast('string'), self.VehiculeSchema())).select('reading.*') #   select("*")
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
                .withColumnRenamed('counter.impulses.2','IsCollecting')
                .drop("position.latitude")
                .drop("position.longitude")
                )



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
                vStreamDF.IsCollecting,
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

    def JoinRFIDStream (self, RfidStreamDF,RfidDF):
        return (
            RfidStreamDF.join(RfidDF, RfidStreamDF.RfidTag == RfidDF.Tag_Numero, "inner")
            .select(
                RfidStreamDF.RfidTag.alias("Tag_Number"),
                RfidStreamDF.timestamp.alias("TagReadingTimeStamp"),
                RfidStreamDF.deviceId.alias("VehiculeRFIDReader"),
                RfidDF.BacDeployment_TagId.alias("tag_id"),
                RfidDF.BacDeployment_Id.alias("BacDeploymentId"),
                RfidDF.BacDeployment_CircuitId.alias("CircuitId"),

                RfidDF.BacDeployment_Position.alias("Position"),
                RfidDF.Circuit_SecteurId.alias("SecteurId"),
                RfidDF.Circuit_Numero.alias("CircuitNumero"),
                RfidDF.Circuit_NombreBac.alias("NombreBac"),

                RfidDF.PlanningPassage_Id.alias("PlanningPassageId"),
                RfidDF.PlanningPassage_DateHeureDebut.alias("StartPlanningTs"),
                RfidDF.PlanningPassage_DateHeureFin.alias("EndPlanningTs"),

             )
            .where(
                (RfidStreamDF.epochTimestamp >= RfidDF.PlanningPassage_EpochStartTime)
                & (RfidStreamDF.epochTimestamp <= RfidDF.PlanningPassage_EpochEndTime)  
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

from pyspark.sql.functions import sum, expr, count, round

class WMSLiveTrackingGold():
    def __init__(self):
        self.base_data_dir = "/FileStore/wcm_streaming"
        
    def readSilver(self, deltaTbl):
        return spark.readStream.table(deltaTbl)

    def CollectedBac(self, rfid_df):
        TotalBac = "NombreBac"
        #lmDF = spark.table("SilverRFIDF1")
        lmc = rfid_df.groupBy("PlanningPassageId", TotalBac).agg(count("Tag_Number").alias("CollectedTags"))
       
        return lmc.withColumn("Taux de Collecte", expr(f"round(100 * CollectedTags / {TotalBac}, 2)"))
    
    def LavageBac(self, rfid_df):
        TotalBac = "NombreBac"
        #lmDF = spark.table("SilverRFIDF1")
        lmc = rfid_df.groupBy("PlanningPassageId", TotalBac).agg(count("Tag_Number").alias("CollectedTags"))
       
        return lmc.withColumn("Taux de Collecte", expr(f"round(100 * CollectedTags / {TotalBac}, 2)"))

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
consumerGroup = "delta"

eh_namerfid = "iothubrfid"
connection_stringrfid = dbutils.secrets.get(scope="tjx", key="iotrfid")
consumerGrouprfid = "clientb"

bronzeVeh = WMSLiveTrackingBronze(eh_name, connection_string, consumerGroup)
bronzeRfid = WMSLiveTrackingBronze(eh_namerfid, connection_stringrfid, consumerGrouprfid)

silver = WMSLiveTrackingSilver()

###############################################################################
bronzeVehDf = bronzeVeh.ReadVehRawStream()
VehiculeCircuitViewDF = bronzeVeh.ReadRefrDBWCM("dbo.Vw_VehiculeCircuitInfos")

bronzeRfidDf = bronzeRfid.ReadVehRawStream()
BacTagInfosDF = bronzeRfid.ReadRefrDBWCM("dbo.Vw_BacTagInfos")

silverVehDf = silver.StageRawStream(bronzeVehDf)

silverRFIDDf = silver.stageRfidStream(bronzeRfidDf)

bronzeRfid.appendRawDelta(silverRFIDDf, "SilverRFID1")

TrackingVehDF = silver.JoinRefStream (silverVehDf,VehiculeCircuitViewDF)

TrackingRFIDDF = silver.JoinRFIDStream (silverRFIDDf,BacTagInfosDF)

bronzeRfid.appendRawDelta(TrackingRFIDDF, "SilverRFIDF1")

################################################################################





TrackingVehDF.display()

# COMMAND ----------

TrackingRFIDDF.display()

# COMMAND ----------

TrackingRFIDDF.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from SilverRFIDF1

# COMMAND ----------

TotalBac = "NombreBac"

lmDF = spark.table("SilverRFIDF1")
lmc = lmDF.groupBy("PlanningPassageId", TotalBac).agg(count("Tag_Number").alias("CollectedTags"))
lmc = lmc.withColumn("Taux de Collecte", expr(f"round(100 * CollectedTags / {TotalBac}, 2)"))
lmc.show()

# COMMAND ----------

DMADF = TrackingVehDF.filter(col("TypePrestationId") == 2)
#silver.SinkIntoCosmos(DMADF, "DMATrackingPlanId")
silver.SinkIntoDeltaSilver(DMADF, 2, "SilverDMA")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from SilverDMA

# COMMAND ----------

# eh_name = "tjxrm"
# connection_string = dbutils.secrets.get(scope="tjx", key="IoTCon")
# consumerGroup = "delta"
# bronze = WMSLiveTrackingBronze(eh_name, connection_string, consumerGroup)
# silver = WMSLiveTrackingSilver()

# ###############################################################################

# df = bronze.ReadVehRawStream()
# df2 = bronze.StageRawStream(df)
# VehiculeCircuitViewDF = silver.ReadRefrWCM("dbo.Vw_VehiculeCircuitInfos")

# TrackingDF = silver.JoinRefStream(df2, VehiculeCircuitViewDF)
# silver.Sink2Hub(TrackingDF)
# Collecte des DMA
DMADF = TrackingVehDF.filter(col("TypePrestationId") == 2)
#silver.SinkIntoCosmos(DMADF, "DMATrackingPlanId")
silver.SinkIntoDeltaSilver(DMADF, 2, "SilverDMA")
#silver.Sink2Hub(DMADF)

#Brossage Mécanique
BrossageDF = TrackingVehDF.filter(col("TypePrestationId") == 4)
#silver.SinkIntoCosmos(BrossageDF, "BrossageTrackingPlanId")
silver.SinkIntoDeltaSilver(BrossageDF, 4, "BrossageSilver")

# Lavage Mécanique 
LavageMecDF = TrackingVehDF.filter(col("TypePrestationId") == 9)
# silver.SinkIntoCosmos(LavageMecDF, "LavageMecTrackingPlanId")
silver.SinkIntoDeltaSilver(LavageMecDF, 9, "LavageMecSilver")

#Lavage de Conteneur
LavageBacDF = TrackingVehDF.filter(col("TypePrestationId") == 10)
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

DMADF.display()

# COMMAND ----------

BrossageDF.display()

# COMMAND ----------

LavageBacDF.display()

# COMMAND ----------

LavageBacDF.printSchema()

# COMMAND ----------

LavageMecDF.display()
