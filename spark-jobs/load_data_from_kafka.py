import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


spark = SparkSession \
    .builder \
    .appName("Load data from kafka topic") \
    .getOrCreate()

quiet_logs(spark)

HDFS_NAMENODE = os.environ["namenode"]
IN_PATH = os.environ["in_path"]
OUT_PATH = os.environ["out_path"]

schema = StructType() \
    .add("ID", StringType()) \
    .add("Case Number", StringType()) \
    .add("Date", StringType()) \
    .add("Block", StringType()) \
    .add("IUCR", StringType()) \
    .add("Primary Type", StringType()) \
    .add("Description", StringType()) \
    .add("Location Description", StringType()) \
    .add("Arrest", StringType()) \
    .add("Domestic", StringType()) \
    .add("Beat", StringType()) \
    .add("District", StringType()) \
    .add("Ward", StringType()) \
    .add("Community Area", StringType()) \
    .add("FBI Code", StringType()) \
    .add("X Coordinate", StringType()) \
    .add("Y Coordinate", StringType()) \
    .add("Year", StringType()) \
    .add("Updated On", StringType()) \
    .add("Latitude", StringType()) \
    .add("Longitude", StringType()) \
    .add("Location", StringType()) \

df = spark \
  .read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka://kafka:9092") \
  .option("subscribe", "crime-reporting") \
  .option("startingOffsets", "earliest") \
  .load()

df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
df = df.select( \
  col("key").cast("string"),
  from_json(col("value").cast("string"), schema).alias("value"))

df = df.select("key", "value.*").drop('key')
df.printSchema()

#df.show()
#df.write.format('com.databricks.spark.csv').save(HDFS_NAMENODE + OUT_PATH, header = 'true', mode="append")
