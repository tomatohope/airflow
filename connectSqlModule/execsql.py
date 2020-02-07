# coding: utf-8
import psycopg2
from airflow.models import Variable


# execsql by sqlfile
def execsqlfile(database, user, password, host, port, sqlfile, result):
    with open(sqlfile, 'r') as f:
        sql = f.read()
    results = execsql(database, user, password, host, port, sql, result)
    return results


# execsql by sql
def execsql(database, user, password, host, port, sql, result):
    conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(sql)
    results = []
    if str(result) == '1':
        results = cursor.fetchall()
    conn.commit()
    conn.close()
    return results


# connect
def conect_execsql(sql, result):
    results = execsql(Variable.get("annlydb_pg_database"),
            Variable.get("annlydb_pg_user"),
            Variable.get("annlydb_pg_pass"),
            Variable.get("annlydb_pg_host"),
            Variable.get("annlydb_pg_port"),
            sql,
            result)
    return results


def conect_execsqlfile(sqlfile, result):
    results = execsqlfile(Variable.get("annlydb_pg_database"),
                Variable.get("annlydb_pg_user"),
                Variable.get("annlydb_pg_pass"),
                Variable.get("annlydb_pg_host"),
                Variable.get("annlydb_pg_port"),
                sqlfile,
                result)
    return results