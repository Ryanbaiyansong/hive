import datetime as dt

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2020, 4, 24, 10, 00, 00),
    'concurrency': 1,
    'retries': 0
}
with DAG('clickstream_dag',
         default_args=default_args,
         schedule_interval='*/10 * * * *',
         ) as dag:
    opr_generate_logs = BashOperator(task_id='generate_logs',
                             bash_command='python3 /home/ec2-user/generate_logs.py')
    opr_flume = BashOperator(task_id='start_flume',                                                                                        bash_command='/home/ec2-user/apache-flume-1.4.0-bin/bin/flume-ng agent --conf /home/ec2-user/apache-flume-1.4.0-bin/conf/ -f /home/ec2-user/apache-flume-1.4.0-bin/conf/flume.conf -Dflume.root.logger=DEBUG,console -n clickstreamagent')
    opr_flume.set_upstream(opr_generate_logs)
    opr_spark = BashOperator(task_id='spark_job',                                                                                        bash_command='/home/ec2-user/spark-2.3.4-bin-hadoop2.7/bin/spark-submit /home/ec2-user/Clickstream.py --master local --driver-memory 5g')
    opr_spark.set_upstream(opr_generate_logs)
opr_generate_logs >> [opr_flume,opr_spark]
