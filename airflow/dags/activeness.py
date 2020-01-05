# coding: utf-8
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
from connectSqlModule import execsql

dt = datetime.now()-timedelta(days=1)

# default_args
default_args = {
    'owner': 'hope.gong',
    'depends_on_past': False,
    'start_date': datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute),
    'email': ['hope.gong@jingdigital.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(seconds=1)
}

# dag name
dag = DAG(
    dag_id='activeness',
    default_args=default_args,
    schedule_interval='20 19 * * * 0'
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

