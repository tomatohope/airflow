# coding: utf-8
import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime as datetime1, timedelta
import datetime as datetime2

# interval time:    与 schedule_interval=timedelta(days=1), 一致
dt = datetime1.now() - datetime2.timedelta(days=1)
airflow_home = '/root/airflow'
os.environ['airflow_home'] = str(airflow_home)

# default_args
default_args = {
    'owner': 'user1',
    'depends_on_past': False,
    # start time: year month day hour minutes seconds
    'start_date': datetime1(dt.year, dt.month, dt.day, 10, 2, 0),
    'email': ['user1@xxx.com', 'user2@xxx.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(seconds=5)
}

# define DAG
dag = DAG(
    # display DAG name
    dag_id='test',
    default_args=default_args,
    # interval time:    与 dt = datetime1.now() - datetime2.timedelta(days=1) 一致
    schedule_interval=timedelta(days=1),
    # 禁用回补 禁止执行过期任务
    catchup=False
)

def hello_world_1(ds, **kwargs):
    print("hello_world1")
    with open("/tmp/a", "at") as f:
        f.write("hello----word" + "\n")
    # 可以在函数中使用assert断言来判断执行是否正常，也可以直接抛出异常
    assert 1 == 1

def hello_world_2(ds, **kwargs):
    ret = os.system("python $airflow_home/python_file/print.py")
    # 执行状态返回值判断
    if ret != 0:
        os._exit(-1)
    print("Continued....")
# task 1
t1 = PythonOperator(
    task_id='hello_world_1',
    # 指定要执行的函数
    python_callable=hello_world_1,
    # 指定归属的dag
    provide_context=True,
    dag=dag,
    retries=0,
)
# task 2
t2 = PythonOperator(
    task_id='hello_world_2',
    python_callable=hello_world_2,
    provide_context=True,
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