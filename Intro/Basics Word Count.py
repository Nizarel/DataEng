# Databricks notebook source
spark.conf.set("fs.azure.account.key.dlstjxmastorage02env01.dfs.core.windows.net", dbutils.secrets.get(scope="tjx", key="stg-accesskey2"))

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, trim, lower, count

class WordCounter:
    
    """
    A class that reads a CSV file of text data from Blob storage and counts the number of words that appear in the data.
    
    """
    
    def __init__(self, input_path):
        
        """
        Initialize WordCounter object with the location of the input CSV file in Blob storage
        :param input_path: location of input CSV file in Blob storage
        
        """
        
        self.input_path = input_path
        self.spark = SparkSession.builder.appName("Word Counter").getOrCreate()
    
    def _raw_words(self):
        
        """
        Private method that reads the CSV file stored in Blob storage and explodes the values by space. Returns a DataFrame with a column named 'word' and a row for each word found. 
        
        """
        df = self.spark.read.format("text").option("lineSep", ".").load(self.input_path)
        raw_words = df.select(explode(split(df.value, " ")).alias("word"))
        return raw_words
    
    def _quality_words(self, words):
        
        """
        Private method that takes a DataFrame of raw words and filters out rows where the 'word' column is null, empty or contains non-alphabetic characters. Returns a DataFrame of quality words with a column named 'word'.
        
        """
        
        quality_words = words.select(lower(trim(words.word)).alias("word")).where("word is not null and word != ''").where("word rlike '^[a-zA-Z]+$'")
        return quality_words
    
    def count_words(self):
        
        """
        A method that calls the _raw_words and _quality_words methods to count the number of times each quality word appears in the data. Returns a DataFrame with two columns: 'word' and 'count'.
        
        """
        
        raw_words = self._raw_words()
        quality_words = self._quality_words(raw_words)
        word_count = quality_words.groupBy("word").count().orderBy("count", ascending=False)
        return word_count
    
    def write_to_table(self, table_name):
        
        """
        A method that saves the output of the count_words method as a Delta table with the specified table_name.
        
        """
        
        word_count = self.count_words()
        word_count.write.format("delta").mode("overwrite").option("compression", "snappy").saveAsTable(table_name)


# COMMAND ----------

# check if the directory is already mounted, and unmount if it is
if any(mount.mountPoint == '/mnt/rawex' for mount in dbutils.fs.mounts()):
  dbutils.fs.unmount('/mnt/rawex')

# mount the blob storage
storage_account_name = 'dlstjxmastorage02env01'
container_name = 'rawex'
storage_account_access_key = dbutils.secrets.get(scope="tjx", key="stg-accesskey2")
conf_key = f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net"
conf_value = storage_account_access_key
dbutils.fs.mount(
  source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
  mount_point=f'/mnt/{container_name}',
  extra_configs={conf_key: conf_value}
)

# instantiate the WordCounter object
input_path = '/mnt/rawex/'
counter = WordCounter(input_path=input_path)

# call the method to count words and generate Delta table
table_name = "word_count_table"
counter.write_to_table(table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from word_count_table
