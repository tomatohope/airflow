# coding: utf-8
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

dt = datetime.now()-timedelta(minutes=1)
airflow_home = '/root/airflow'
os.environ['airflow_home'] = str(airflow_home)

# default_args
default_args = {
    'owner': 'hope.gong',
    'depends_on_past': False,
    'start_date': datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute),
    'email': ['helper@test.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(seconds=5)
}

# define DAG
dag = DAG(
    dag_id='hello_world',  # display DAG name
    default_args=default_args,
    #schedule_interval="00, *, *, *, *"  # minutes, hour, day, month, year
    schedule_interval=timedelta(minutes=1),
    catchup=False #禁用回补 禁止执行过期任务
)

def hello_world_1():
    current_time = str(datetime.today())
    print("hello_world1")
    with open("/tmp/a","at") as f:
        f.write("hello----word")
    assert 1 == 1  # 可以在函数中使用assert断言来判断执行是否正常，也可以直接抛出异常

def hello_world_2():
    # ret = os.system("python3 $airflow_home/python_file/hello.py") 这里的 python 版本根据您的airflow 所安装的python 版本
    ret = os.system("python $airflow_home/python_file/hello.py")
    # 执行状态返回值判断
    if ret != 0:
        os._exit(-1)
    print("Continued....")
# task 1
t1 = PythonOperator(
    task_id='hello_world_1',
    python_callable=hello_world_1,  # 指定要执行的函数
    dag=dag,  # 指定归属的dag
    retries=2,  # 重写失败重试次数，如果不写，则默认使用dag类中指定的default_args中的设置
)
# task 2
t2 = PythonOperator(
    task_id='hello_world_2',
    python_callable=hello_world_2,
    dag=dag,
)

# task plan
#t2.set_upstream(t1)  # t2依赖于t1; 等价于 t1.set_downstream(t2);同时等价于 dag.set_dependency('hello_world_1', 'hello_world_2')
# 表示t2这个任务只有在t1这个任务执行成功时才执行
# t1        ##only t1
# t1 >> t2  ## t1 first success && t2
t1 >> t2

# airflow.note
# http://note.youdao.com/noteshare?id=bb4888b561b3468e732361de74c7794e&sub=FD605AE047F04575A92C1DF2BCF9E7EA
# exec time
###############################################
# start_date + schedule_interval
# https://www.jianshu.com/p/5aa4447f48ea
#
# # start_date
#
# if now time ago:
#     real
#     start
#     time: now
#     time + schedule_interval
#
# # schedule_interval
# if cron:
#     not now
#     time: now
#     time + schedule_interval
