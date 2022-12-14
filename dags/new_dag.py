from datetime import datetime
import psycopg2
import pandas as pd
import random
import os.path

from airflow.models import Variable
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.sensors.sql_sensor import SqlSensor
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models.xcom import XCom
from airflow.sensors.python import PythonSensor
from airflow.sensors.filesystem import FileSensor


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

# file = "./dags/file.txt"
file = Variable.get("txt_path")
file2 = r"./dags/file_12d.txt"
file3 = r"./dags/file_12e.txt"

def two_rnd_2file():
    """c. Если запуск произошел успешно, попробуйте изменить логику вашего Python-оператора 
    следующим образом – сгенерированные числа должны записываться в текстовый файл – через пробел.
     При этом должно соблюдаться условие, что каждые новые два числа должны записываться 
     с новой строки не затирая предыдущие."""

    debug = False

    # file = r"file.txt"
    a = random.randint(-100, 100)
    b = random.randint(-100, 100)
    if debug : print(a,b)

    # строка для добавления
    df_new = pd.DataFrame(data = [[a,b]], columns=[0,1])
    if debug : print("новый фрейм\n", df_new)

    # если файла не будет, то заменим его этой пустой таблицей
    df = pd.DataFrame(columns=[0, 1])
    if debug : print("пустрой фрейм\n", df)

    # открываем файл
    if os.path.isfile(file):
        if debug : print(f"Opening {file}")
        df = pd.read_csv(file, sep=" ",header=None, index_col=None  )
        # df.columns=["a","b"]
        if debug : print("Прочитали из файла: \n", df)
    else:
        print(f"нет файла {file}")

    # df.append({"a":a,"b":b}, ignore_index=True)
    df1=pd.concat([df, df_new])
    if debug : print ("сохраняем в файл вот это:\n",df1)
    df1.to_csv(file, sep=" ", index=False, header= False)
	

def two_rnd_2file_subtotal():
    """d. Создайте новый оператор, который подключается к файлу 
    и вычисляет сумму всех чисел из первой колонки, 
    затем сумму всех чисел из второй колонки 
    и рассчитывает разность полученных сумм. 
    Вычисленную разность необходимо записать в конец того же файла, не затирая его содержимого."""

    debug = False

    file = file2 #r"./dags/file_12d.txt"
    a = random.randint(-100, 100)
    b = random.randint(-100, 100)
    if debug : print(a,b)

    # строка для добавления
    df_new = pd.DataFrame(data = [[a,b]], columns=[0,1])
    if debug : print("новый фрейм\n", df_new)

    # если файла не будет, то заменим его этой пустой таблицей
    df = pd.DataFrame(columns=[0, 1])
    if debug : print("пустрой фрейм\n", df)

    # открываем файл
    if os.path.isfile(file):
        if debug : print(f"Opening {file}")
        df = pd.read_csv(file, sep=" ",header=None, index_col=None  )
        # df.columns=["a","b"]
        if debug : print("Прочитали из файла: \n", df)
        # # это, оказывается надо было для следующей задачи
        # #удалить последнюю строчку с разностью сумм
        # df = df.drop(index=df.index[-1])
        # if debug : print("Удалили последнюю строку: \n", df)
    else:
        print(f"нет файла {file}")

    # df.append({"a":a,"b":b}, ignore_index=True)
    df1=pd.concat([df, df_new])
    if debug : print ("Получилась такая таблица:\n",df1)

    # расчет сумм
    sum0 = df1[0].sum()
    sum1 = df1[1].sum()
    delt = sum0 - sum1
    if debug : print(f"Сумма0 {sum0},  Сумма1 {sum1} , разница {delt}")
    #вставляем в таблицу строку с разностью сумм 
    df_new = pd.DataFrame(data = [[delt,""]], columns=[0,1])
    df1=pd.concat([df1, df_new])

    if debug : print ("сохраняем в файл вот это:\n",df1)
    df1.to_csv(file, sep=" ", index=False, header= False)



def two_rnd_2file_subtotal_del():
    """e. Измените еще раз логику вашего оператора из пунктов 12.а – 12.с. 
    При каждой новой записи произвольных чисел в конец файла, 
    вычисленная сумма на шаге 12.d должна затираться."""

    debug = False

    file = file3 #r"./dags/file_12e.txt"
    a = random.randint(-100, 100)
    b = random.randint(-100, 100)
    if debug : print(a,b)

    # строка для добавления
    df_new = pd.DataFrame(data = [[a,b]], columns=[0,1])
    if debug : print("новый фрейм\n", df_new)

    # если файла не будет, то заменим его этой пустой таблицей
    df = pd.DataFrame(columns=[0, 1])
    if debug : print("пустрой фрейм\n", df)

    # открываем файл
    if os.path.isfile(file):
        if debug : print(f"Opening {file}")
        df = pd.read_csv(file, sep=" ",header=None, index_col=None  )
        # df.columns=["a","b"]
        if debug : print("Прочитали из файла: \n", df)
        #удалить последнюю строчку с разностью сумм
        df = df.drop(index=df.index[-1])
        if debug : print("Удалили последнюю строку: \n", df)
    else:
        print(f"нет файла {file}")

    # df.append({"a":a,"b":b}, ignore_index=True)
    df1=pd.concat([df, df_new])
    if debug : print ("Получилась такая таблица:\n",df1)

    # расчет сумм
    sum0 = df1[0].sum()
    sum1 = df1[1].sum()
    delt = sum0 - sum1
    if debug : print(f"Сумма0 {sum0},  Сумма1 {sum1} , разница {delt}")
    #вставляем в таблицу строку с разностью сумм 
    df_new = pd.DataFrame(data = [[delt,""]], columns=[0,1])
    df1=pd.concat([df1, df_new])

    if debug : print ("сохраняем в файл вот это:\n",df1)
    df1.to_csv(file, sep=" ", index=False, header= False)


def file_exists():
    """g.* ... условие: i.     Файл существует"""
    file = file3 #r"./dags/file_12e.txt"
    debug = False

    # открываем файл
    if os.path.isfile(file):
        if debug : print(f"File exists: {file}")
        return True
    else:
        if debug : print(f"File NOT exists: {file}")
        return False


def check_str_num(**context):
    """g.* ... условие: ii.     Количество строк в файле минус одна последняя - соответствует кол-ву запусков"""

    debug = True
    
    # https://stackoverflow.com/questions/70692446/how-do-i-check-if-all-my-tasks-in-an-airflow-dag-were-successful
    DagRun = context["dag_run"]
    TaskInstance = context["ti"]
    # https://stackoverflow.com/questions/58741649/how-to-get-dag-status-like-running-or-success-or-failure
    dag_id = TaskInstance.dag_id #task_id # 'new_dag'
    if debug : print(f"dag_id = {dag_id}")
    dag_runs = len(DagRun.find(dag_id=dag_id))
    if debug : print(f"Количетво Запусков dag_runs = {dag_runs}")


    file = file3 #r"./dags/file_12e.txt"

    # открываем файл
    if os.path.isfile(file):
        if debug : print(f"Opening {file}")
        df = pd.read_csv(file, sep=" ",header=None, index_col=None  )
        # df.columns=["a","b"]
        if debug : print("Прочитали из файла: \n", df.tail() )
        #удалить последнюю строчку с разностью сумм
        df = df.drop(index=df.index[-1])
        if debug : print("Удалили последнюю строку: \n", df.tail() )
        str_count = len(df)

        # проверяем целевое условие
        if str_count == dag_runs :
            if debug : print(f"str_count({str_count}) == dag_runs({dag_runs}): \n")
            return True
        else:
            if debug : print(f"str_count({str_count}) != dag_runs({dag_runs}): \n")
            return False
    else:
        print(f"нет такого файла {file}")
        return False

    



   




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
with DAG(dag_id="new_dag", start_date=datetime(2022, 12, 11, 17, 0), schedule="* * * * *", max_active_runs=5 ) as dag:
    
    # accurate = DummyOperator(
    #     task_id = 'accurate'
    # )
    # inaccurate = DummyOperator(s
    #     task_id = 'inaccurate'
    # )

    # Tasks are represented as operators
    bash_task = BashOperator(task_id="hello", bash_command="echo hello", do_xcom_push=False)
    python_task = PythonOperator(task_id="world", python_callable = hello, do_xcom_push=False)
    python_task1 = PythonOperator(task_id="read_csv", python_callable = read_csv, do_xcom_push=True)
    python_task2 = PythonOperator(task_id="two_random_numbers", python_callable = two_random_numbers, do_xcom_push=False)
    python_task3 = PythonOperator(task_id="two_rnd_2file", python_callable = two_rnd_2file, do_xcom_push=False)
    python_task4 = PythonOperator(task_id="two_rnd_2file_subtotal", python_callable = two_rnd_2file_subtotal, do_xcom_push=False)
    python_task5 = PythonOperator(task_id="two_rnd_2file_subtotal_del", python_callable = two_rnd_2file_subtotal_del, do_xcom_push=False)
    python_task6 = PythonOperator(task_id="count_runs", python_callable = check_str_num, do_xcom_push=False)
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

    # pySensor = PythonSensor()

    # file_Sensor = FileSensor(
    #     task_id=f'file_waiter',
    #     poke_interval=60,
    #     timeout=60 * 1,
    #     mode="reschedule",
    #     # on_failure_callback=_failure_callback,
    #     filepath= file3,
    #     fs_conn_id=f'conn_filesensor_1'
    #     )

    
    # bash_task2 = BashOperator(task_id="bye", bash_command="echo bye, baby, bye", do_xcom_push=False)
    
    # choose_best_model = BranchPythonOperator(
    #     task_id = 'branch_oper',
    #     python_callable = python_branch,
    #     do_xcom_push = False
    # )

    # Set dependencies between tasks
    # bash_task >> python_task >> python_task1 >> conn_to_psql_tsk >> read_from_psql_tsk >> sql_sensor \
    #     >> bash_task2 >> choose_best_model >> [accurate, inaccurate]

    python_task6 >> bash_task >> python_task >> python_task1 >> python_task2 >> python_task3 >> python_task4 >> python_task5
