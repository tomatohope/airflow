# -*- coding: utf-8 -*-
import os
import threading
from connectSqlModule import execsql, log

mid = execsql.conect_execsql('SELECT DISTINCT wid FROM dev_test_dw.user_action_record', '1')
log.log('info', "querymid - " + str(mid))
airflow_home = '/root/airflow'

#date = str(time.strftime("%Y%m%d")) ####insert  date
date = '20191023'

for i in range(len(mid)):
    Mid = str(mid[i][0])
    os.environ['mid'] = Mid
    log.log('info', "mid - " + Mid)

    table_name = 'ads_user_session_269_' + Mid
    log.log('info', "table_name - " + table_name)
    os.environ['table_name'] = str(table_name)

    os.environ['date'] = str(date)
    log.log('info', "date - " + str(date))

    checkTablesqlfile = airflow_home + "/Sqlfile/query_if_table_exist_sql"
    log.log('info', "checkTablesqlfile - " + checkTablesqlfile)

    os.environ['checkTablesqlfile'] = str(checkTablesqlfile)
    checkTablesql = os.popen("sed -e \"s/tablenametablename/$table_name/g\" $checkTablesqlfile").read()
    log.log('info', "checkTablesql - " + checkTablesql)

    insertSqlfile = airflow_home + "/Sqlfile/session"
    log.log('info', "insertSqlfile - " + insertSqlfile)
    os.environ['insertSqlfile'] = str(insertSqlfile)

    createTablesqlfile = airflow_home + "/Sqlfile/create_session_table_sql"
    log.log('info', "createTablesqlfile - " + createTablesqlfile)
    os.environ['createTablesqlfile'] = str(createTablesqlfile)

    ### check table exist
    checkTables = execsql.conect_execsql(checkTablesql, '1')
    log.log('info', "checkTables - " + str(checkTables))
    if '0' in str(checkTables[0]):
        ### create table sql
        sql = os.popen("sed -e \"s/tablenametablename/$table_name/g\" $createTablesqlfile").read()
        log.log('info', "createTablesql - " + sql)
        execsql.conect_execsql(sql, '0') #set search_path='dev_test_dw'; 在SQL语句中指定schema

    sql = os.popen("sed -e \"s/created_datecreated_date/$date/g\" $insertSqlfile | sed -e \"s/widwid/$mid/g\" | sed -e \"s/tablenametablename/$table_name/g\"").read()
    log.log('info', "insertSql - " + sql)

    task = threading.Thread(target=execsql.conect_execsql(sql, '0'), args=sql)
    task.start()
    log.log('info', table_name + " - task is started")
