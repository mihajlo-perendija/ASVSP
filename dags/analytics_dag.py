import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator


default_args = {
    'owner': 'test',
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 10),
    'email': ['test@test.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='analytics_dag',
          default_args=default_args,
          catchup=False,
          schedule_interval="@once")

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
pyspark_app_home = "/usr/local/spark/app"


load_data_from_kafka = BashOperator(
    task_id='load_data_from_kafka',
    bash_command='spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --master spark://spark-master:7077 ' +
    f'{pyspark_app_home}/load_data_from_kafka.py',
    env={'namenode': HDFS_NAMENODE,
        'in_path': '/data/data_batch.csv',
        'out_path': '/data/data_batch.csv'},
    dag=dag)

clean_data = BashOperator(
    task_id='clean_data',
    bash_command='spark-submit --master spark://spark-master:7077 ' +
                f'{pyspark_app_home}/clean_data.py',
    env={'namenode': HDFS_NAMENODE,
        'in_path': '/data/data_batch.csv',
        'out_path': '/data/data_batch_cleaned.csv'
        },
    dag=dag)

count_by_year = BashOperator(
    task_id='count_by_year',
    bash_command='spark-submit --conf "spark.mongodb.input.uri=mongodb://asvsp:asvsp@mongo:27017/asvsp.count_by_year?authSource=admin" \
              --conf "spark.mongodb.output.uri=mongodb://asvsp:asvsp@mongo:27017/asvsp.count_by_year?authSource=admin&retryWrites=true" \
              --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.0 --master spark://spark-master:7077 ' +
    f'{pyspark_app_home}/count_by_year.py',
    env={'namenode': HDFS_NAMENODE,
        'in_path': '/data/data_batch_cleaned.csv'},
    dag=dag)

count_by_year_and_month = BashOperator(
    task_id='count_by_year_and_month',
    bash_command='spark-submit --conf "spark.mongodb.input.uri=mongodb://asvsp:asvsp@mongo:27017/asvsp.count_by_year_and_month?authSource=admin" \
              --conf "spark.mongodb.output.uri=mongodb://asvsp:asvsp@mongo:27017/asvsp.count_by_year_and_month?authSource=admin&retryWrites=true" \
              --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.0 --master spark://spark-master:7077 ' +
    f'{pyspark_app_home}/count_by_year_and_month.py',
    env={'namenode': HDFS_NAMENODE,
        'in_path': '/data/data_batch_cleaned.csv'},
    dag=dag)

count_by_district = BashOperator(
    task_id='count_by_district',
    bash_command='spark-submit --conf "spark.mongodb.input.uri=mongodb://asvsp:asvsp@mongo:27017/asvsp.count_by_district?authSource=admin" \
              --conf "spark.mongodb.output.uri=mongodb://asvsp:asvsp@mongo:27017/asvsp.count_by_district?authSource=admin&retryWrites=true" \
              --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.0 --master spark://spark-master:7077 ' +
    f'{pyspark_app_home}/count_by_district.py',
    env={'namenode': HDFS_NAMENODE,
        'in_path': '/data/data_batch_cleaned.csv'},
    dag=dag)


count_by_police_station_distance = BashOperator(
    task_id='count_by_police_station_distance',
    bash_command='spark-submit --conf "spark.mongodb.input.uri=mongodb://asvsp:asvsp@mongo:27017/asvsp.count_by_police_station_distance?authSource=admin" \
              --conf "spark.mongodb.output.uri=mongodb://asvsp:asvsp@mongo:27017/asvsp.count_by_police_station_distance?authSource=admin&retryWrites=true" \
              --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.0 --master spark://spark-master:7077 ' +
    f'{pyspark_app_home}/count_by_police_station_distance.py',
    env={'namenode': HDFS_NAMENODE,
        'in_path': '/data/data_batch_cleaned.csv'},
    dag=dag)

crime_rate_by_comuntiy_area = BashOperator(
    task_id='crime_rate_by_comuntiy_area',
    bash_command='spark-submit --conf "spark.mongodb.input.uri=mongodb://asvsp:asvsp@mongo:27017/asvsp.crime_rate_by_comuntiy_area_avg?authSource=admin" \
              --conf "spark.mongodb.output.uri=mongodb://asvsp:asvsp@mongo:27017/asvsp.crime_rate_by_comuntiy_area_avg?authSource=admin&retryWrites=true" \
              --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.0 --master spark://spark-master:7077 ' +
    f'{pyspark_app_home}/crime_rate_by_comuntiy_area.py',
    env={'namenode': HDFS_NAMENODE,
        'in_path': '/data/data_batch_cleaned.csv'},
    dag=dag)

load_data_from_kafka >> clean_data
load_data_from_kafka >> clean_data >> count_by_year
load_data_from_kafka >> clean_data >> count_by_year_and_month
load_data_from_kafka >> clean_data >> count_by_district
load_data_from_kafka >> clean_data >> count_by_police_station_distance
load_data_from_kafka >> clean_data >> crime_rate_by_comuntiy_area
