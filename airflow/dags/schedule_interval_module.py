# coding: utf-8
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from connectSqlModule import execsql

# get last time
# schedule_interval time
dt = datetime.now()-timedelta(minutes=2)

# default_args
default_args = {
    'owner': 'hope.gong',
    'depends_on_past': False,
    'start_date': datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute),
    'email': ['hope.gong@jingdigital.com'],  # list
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(seconds=1)
}


# dag name
dag = DAG(
    dag_id='schedule_interval_module',
    default_args=default_args,
    # schedule_interval time && the same as dt = datetime.now()-timedelta(minutes=2)
    schedule_interval=timedelta(minutes=2)
)

def task1():
    execsql.conect_execsqlfile('/tmp/connector_analy_pg/sqlfile', '0')

# task 1
t1 = PythonOperator(
    task_id='SelectSql',
    python_callable=task1,
    dag=dag,
    retries=0,
)

# t1 >> t2
t1


