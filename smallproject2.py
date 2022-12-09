import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

warehouse_location = abspath('spark-warehouse')
scSpark = SparkSession.builder.appName("Spark Hive").config("spark.sql.warehouse.dir",
                                                            warehouse_location).enableHiveSupport().getOrCreate()


def task2():
    df_task2 = scSpark.read.load('USvideos.csv',
                                 format='com.databricks.spark.csv',
                                 header='true',
                                 inferSchema='true')

    print(df_task2.show())

    df_task2.withColumnRenamed('title', 'title_csv').write.mode('overwrite').option('header', True).csv(
        '/user/ubuntu/task2/')


def task3():
    readjson = scSpark.read.option("multiline", "true").json('US_category_id.json')

    reading_json = readjson.withColumn('items', explode(col('items'))).select('items.*')
    df_task3 = reading_json.select(['etag', 'id', 'kind', 'snippet.assignable', 'snippet.channelId', 'snippet.title'])
    print(df_task3.show())

    df_task3.withColumnRenamed('title', 'title_json').write.mode('overwrite').option('header', True).csv(
        '/user/ubuntu/task3/')


def task4():
    loadtask2 = scSpark.read.csv('/user/ubuntu/task2/', header='true')
    loadtask3 = scSpark.read.csv('/user/ubuntu/task3/', header='true')

    df_task4 = loadtask2.join(loadtask3)
    print(df_task4.show())

    df_task4.write.mode('overwrite').option('header', True).csv('/user/ubuntu/task4/')

    scSpark.stop()


default_args = {
    'owner': 'chelsea',
    'start_date': dt.datetime(2022, 10, 3),
    'concurrency': 2,
    'tags': 'sekolah big data',
    'retries': 0,
}

with DAG('smallproject2_chelsea', catchup=False, default_args=default_args, schedule_interval='*/15 * * * *') as dag:
    opr_1 = BashOperator(task_id='Operator_1', bash_command='echo "Operator 1"')
    opr_2 = PythonOperator(task_id='Task_2', python_callable=task2)
    opr_3 = PythonOperator(task_id='Task_3', python_callable=task3)
    opr_4 = PythonOperator(task_id='Task_4', python_callable=task4)
    opr_5 = BashOperator(task_id='Operator_5', bash_command='echo "Operator 5"')

opr_1 >> opr_2 >> opr_3 >> opr_4 >> opr_5
