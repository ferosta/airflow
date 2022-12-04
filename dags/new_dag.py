from datetime import datetime
import psycopg2
import pandas as pd
import random

from airflow.models import Variable
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.sensors.sql_sensor import SqlSensor
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models.xcom import XCom

# def get_conn_credentials(conn_id) -> BaseHook.get_connection:
#     conn_to_airflow = BaseHook.get_connection(conn_id)
#     return conn_to_airflow

def hello():
    print("Hello, Airflow!")


def read_csv(**kwargs):
    ti = kwargs['ti']
    print("Hello! Address csv reader")
    """    # 12. Поздравляем, вы успешно создали первый DAG в Airflow! Теперь попробуем усовершенствовать его:
    # a. Скачайте произвольный csv-файл(можно отсюда: https://people.sc.fsu.edu/~jburkardt/data/csv/csv.html) и 
    # напишите в python-операторе код, который подключается к скачанному csv-файлу и подсчитывает количество строк в файле.
    #  Попробуйте использовать технологию Variables и записать путь к файлу в список переменных Airflow, 
    # а кол-во строк в файле записать в XCom. 
    # Про XCom и Variables можно прочитать на оф. документации
    # (https://airflow.apache.org/docs/apache-airflow/stable/concepts/index.html) или смотреть примеры в Интернете."""
    
    # file = "./dags/csvs/addresses.csv"
    file = Variable.get("csv_path")
      
    # read the csv file
    results = pd.read_csv(file)
    
    # display dataset
    print(len(results))

    ti.xcom_push("return_value", len(results))


def two_random_numbers ():
    """a. Создайте еще один PythonOperator, который генерирует два произвольных числа и печатает их.
     Добавьте вызов нового оператора в конец вашего pipeline с помощью >>."""

    a = random.randint(0, 10)
    b = random.randint(0, 10)
    print(f"а. Два случайных числа: {a} и {b}")
	

# def connect_to_psql(**kwargs):
#     ti = kwargs['ti']

#     conn_id = Variable.get("conn_id")
#     conn_to_airflow = get_conn_credentials(conn_id)

#     pg_hostname, pg_port, pg_username, pg_pass, pg_db = conn_to_airflow.host, conn_to_airflow.port,\
#                                                              conn_to_airflow.login, conn_to_airflow.password,\
#                                                                  conn_to_airflow.schema
    
#     ti.xcom_push(value = [pg_hostname, pg_port, pg_username, pg_pass, pg_db], key='conn_to_airflow')
#     pg_conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)

#     # conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)
#     cursor = pg_conn.cursor()

#     cursor.execute("CREATE TABLE IF NOT EXISTS test_table (id serial PRIMARY KEY, num integer, data varchar);")
	
# 	#cursor.execute("""begin;select pg_advisory_xact_lock(12345);CREATE TABLE IF NOT EXISTS test_table (id serial PRIMARY KEY, num integer,data varchar);commit;""")
	
#     cursor.execute("INSERT INTO test_table (num, data) VALUES (%s, %s)",(100, "abc'def"))
    
#     #cursor.fetchall()
#     pg_conn.commit()

#     cursor.close()
#     pg_conn.close()

# def read_from_psql(**kwargs):
#     ti = kwargs['ti']
#     pg_hostname, pg_port, pg_username, pg_pass, pg_db = ti.xcom_pull(key='conn_to_airflow', task_ids='conn_to_psql')

#     # pg_hostname, pg_port, pg_username, pg_pass, pg_db = pg_conn.host, pg_conn.port,\
#     #                                                          pg_conn.login, pg_conn.password, pg_conn.schema
#     pg_conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)
#     cursor = pg_conn.cursor()

#     cursor.execute("SELECT * FROM test_table;")
#     print(cursor.fetchone())
    
#     cursor.close()
#     pg_conn.close()

# def python_branch(**kwargs):
#     ti = kwargs['ti']

#     creds = ti.xcom_pull(key='conn_to_airflow', task_ids='conn_to_psql')
#     if len(creds) == 5:
#         return "accurate"
#     else:
#         return "inaccurate"

# A DAG represents a workflow, a collection of tasks
with DAG(dag_id="new_dag", start_date=datetime(2022, 1, 1), schedule="*/1 * * * *") as dag:
    
    # accurate = DummyOperator(
    #     task_id = 'accurate'
    # )
    # inaccurate = DummyOperator(
    #     task_id = 'inaccurate'
    # )

    # Tasks are represented as operators
    bash_task = BashOperator(task_id="hello", bash_command="echo hello", do_xcom_push=False)
    python_task = PythonOperator(task_id="world", python_callable = hello, do_xcom_push=False)
    python_task1 = PythonOperator(task_id="read_csv", python_callable = read_csv, do_xcom_push=True)
    python_task2 = PythonOperator(task_id="two_random_numbers", python_callable = two_random_numbers, do_xcom_push=False)
    # conn_to_psql_tsk = PythonOperator(task_id="conn_to_psql", python_callable = connect_to_psql)
    # read_from_psql_tsk = PythonOperator(task_id="read_from_psql", python_callable = read_from_psql)

    # sql_sensor = SqlSensor(
    #         task_id='sql_sensor_check',
    #         poke_interval=60,
    #         timeout=180,
    #         soft_fail=False,
    #         retries=2,
    #         sql="select count(*) from test_table",
    #         conn_id=Variable.get("conn_id"),
    #     dag=dag)

    # bash_task2 = BashOperator(task_id="bye", bash_command="echo bye, baby, bye", do_xcom_push=False)
    
    # choose_best_model = BranchPythonOperator(
    #     task_id = 'branch_oper',
    #     python_callable = python_branch,
    #     do_xcom_push = False
    # )

    # Set dependencies between tasks
    # bash_task >> python_task >> python_task1 >> conn_to_psql_tsk >> read_from_psql_tsk >> sql_sensor \
    #     >> bash_task2 >> choose_best_model >> [accurate, inaccurate]

    bash_task >> python_task >> python_task1 >> python_task2
