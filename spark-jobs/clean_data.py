import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

@udf
def get_month(date_string):
    return date_string.split(' ')[0].split('-')[1]

@udf
def get_day(date_string):
    return date_string.split(' ')[0].split('-')[2]

@udf
def get_hour(date_string):
    return date_string.split(' ')[1].split(':')[0]

spark = SparkSession \
    .builder \
    .appName("Clean Data") \
    .getOrCreate()

quiet_logs(spark)

HDFS_NAMENODE = os.environ["namenode"]
IN_PATH = os.environ["in_path"]
OUT_PATH = os.environ["out_path"]

df = spark.read.format("csv").option('header', 'true').load(HDFS_NAMENODE + IN_PATH)

result = df.dropDuplicates(['ID']).where(col("District").isNotNull() & col("Beat").isNotNull() & col("Latitude").isNotNull() & col("Longitude").isNotNull() & col("Community Area").isNotNull()) \
            .withColumn("District", df["District"].cast(IntegerType())) \
            .withColumn("Beat", df["Beat"].cast(IntegerType())) \
            .withColumn("Community Area", df["Community Area"].cast(IntegerType())) \
            .withColumn('Month', lit(get_month(df["Date"]))) \
            .withColumn('Day', lit(get_day(df["Date"]))) \
            .withColumn('Hour', lit(get_hour(df["Date"]))) \
            .withColumn("Latitude", df["Latitude"].cast(DoubleType())) \
            .withColumn("Longitude", df["Longitude"].cast(DoubleType()))

result.write.format('com.databricks.spark.csv').save(HDFS_NAMENODE + OUT_PATH, header = 'true', mode="overwrite")
