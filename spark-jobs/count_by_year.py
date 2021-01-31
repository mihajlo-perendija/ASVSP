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
    .appName("Count crimes by year") \
    .config("spark.mongodb.output.uri", "mongodb://asvsp:asvsp@mongo:27017/asvsp.count_by_year?authSource=admin") \
    .getOrCreate()

quiet_logs(spark)

HDFS_NAMENODE = os.environ["namenode"]
IN_PATH = os.environ["in_path"]

df = spark.read.format("csv").option('header', 'true').load(HDFS_NAMENODE + IN_PATH)

grouping_cols = ["Year"]
result = df.groupBy(grouping_cols) \
            .agg(
              count(col("*")).alias("crimes_count"),
            ) \
            .orderBy(grouping_cols)

result.show()
result.write.format("mongo").mode("overwrite").save()