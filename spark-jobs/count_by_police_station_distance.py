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

spark = SparkSession \
    .builder \
    .appName("Count crimes by closest police station distance") \
    .config("spark.mongodb.output.uri", "mongodb://asvsp:asvsp@mongo:27017/asvsp.count_by_police_station_distance?authSource=admin") \
    .getOrCreate()

quiet_logs(spark)

HDFS_NAMENODE = os.environ["namenode"]
IN_PATH = os.environ["in_path"]

df = spark.read.format("csv").option('header', 'true').load(HDFS_NAMENODE + IN_PATH)
stations = spark.read.format("csv").option('header', 'true').option("mode", "DROPMALFORMED").load(HDFS_NAMENODE + '/data/Police_Stations.csv')
stations_list = stations.select('*').rdd.map(lambda row : row).collect()

# https://medium.com/@petehouston/calculate-distance-of-two-locations-on-earth-using-python-1501b1944d97
def haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(m.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = m.sin(dlat / 2) ** 2 + m.cos(lat1) * m.cos(lat2) * m.sin(dlon / 2) ** 2
    return 2 * 6371000 * m.asin(m.sqrt(a))

@f.udf
def get_distance(lat, lon):
    distances = []
    for station in stations_list:
        distances.append(haversine(float(lon), float(lat), float(station[13]), float(station[12])))
    min_distance = min(distances)
    if min_distance <= 100:
        return '100'
    elif 100 < min_distance <= 300:
        return '300'
    elif 300 < min_distance <= 600:
        return '600'
    elif 600 < min_distance <= 900:
        return '900'
    elif 900 < min_distance <= 1200:
        return '1200'
    elif 1200 < min_distance <= 1500:
        return '1500'
    else:
        return '>1500'

distance_udf = f.udf(get_distance, FloatType())

grouping_cols = ["Distance"]
result = df.withColumn('Distance', f.lit(get_distance(df["Latitude"], df["Longitude"]))) \
            .groupBy(grouping_cols) \
                .agg(
                    f.count(f.col("*")).alias("crimes_count"),
                ) \
                .orderBy(grouping_cols)

result.show()
result.write.format("mongo").mode("overwrite").save()