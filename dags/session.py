# coding: utf-8
import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime as datetime1, timedelta
import datetime as datetime2

dt = datetime1.now() - datetime2.timedelta(days=1)
airflow_home = '/root/airflow'
os.environ['airflow_home'] = str(airflow_home)

# default_args
default_args = {
    'owner': 'user1',
    'depends_on_past': False,
    'start_date': datetime1(dt.year, dt.month, dt.day, 10, 2, 0),
    'email': ['user1@xxx.com', 'user2@xxx.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(seconds=1)
}

# dag name
dag = DAG(
    dag_id='session',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

def task1(ds, **kwargs):
    ret = os.system("python $airflow_home/python_file/session.py")
    if ret != 0:
        os._exit(-1)
    print("Continued....")    
    
# task 1
t1 = PythonOperator(
    task_id='SelectSql',
    python_callable=task1,
    dag=dag,
    provide_context=True,
    retries=0,
)

# t1 >> t2
t1

