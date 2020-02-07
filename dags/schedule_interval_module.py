# coding: utf-8
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime as datetime1, timedelta
import datetime as datetime2
from connectSqlModule import execsql

# get last time
# schedule_interval time
dt = datetime1.now() - datetime2.timedelta(minutes=2)

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
    dag_id='schedule_interval_module',
    default_args=default_args,
    # schedule_interval time && the same as dt = datetime.now()-timedelta(minutes=2)
    schedule_interval=timedelta(minutes=2)
)

def task1(ds, **kwargs):
    execsql.conect_execsqlfile('/tmp/connector_analy_pg/sqlfile', '0')

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


