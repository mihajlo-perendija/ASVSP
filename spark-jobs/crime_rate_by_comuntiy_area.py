import math as m
import os
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


@f.udf
def get_crime_rate(count, population):
    return (float(count) / float(population)) * 1000


spark = SparkSession \
    .builder \
    .appName("Count crimes by closest police station distance") \
    .config("spark.mongodb.output.uri", "mongodb://asvsp:asvsp@mongo:27017/asvsp.crime_rate_by_comuntiy_area_avg?authSource=admin") \
    .getOrCreate()

quiet_logs(spark)

HDFS_NAMENODE = os.environ["namenode"]
IN_PATH = os.environ["in_path"]

df = spark.read.format("csv").option('header', 'true').load(HDFS_NAMENODE + IN_PATH)
census = spark.read.format("csv").option('header', 'true').option("mode", "DROPMALFORMED").option("delimiter", " ").load(HDFS_NAMENODE + '/data/Census_Data_By_Community_Area.csv')


grouping_cols = ["Community Area", "Year"]
by_comunity_area = df.groupBy(grouping_cols).agg(f.count(f.col("*")).alias("crimes_count"))
by_comunity_area.show()
by_comunity_area = by_comunity_area.groupBy(grouping_cols).agg(f.mean(f.col("crimes_count")).alias("crimes_avg_year"))
by_comunity_area = by_comunity_area.groupBy(["Community Area"]).agg(f.mean(f.col("crimes_avg_year")).alias("crimes_avg"))

by_comunity_area.show()

joined = df.join(by_comunity_area, ["Community Area"]).dropDuplicates()

result = joined.join(census, joined["Community Area"] == census["Comunity_number"]) \
            .withColumn('Crime Rate', f.lit(get_crime_rate(f.col("crimes_avg"), f.col("Total population")))) \
            .select("Community Area", "Crime Rate", "Total population", "crimes_avg", "Hispanic", "Non-Hispanic Black", "Non-Hispanic White", "Non-Hispanic Asian").distinct()

result.show()
result.write.format("mongo").mode("overwrite").save()
