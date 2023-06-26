"""
BranchingOperator - это оператор, который по некоторому условию определяет, в какое ответвление пойдет выполнение DAG. 
Один из способов определить это "некоторое условие" - это задать python функцию, которая будет возвращать task_id, куда надо перейти 

Создайте DAG, имеющий BranchPythonOperator. Логика ветвления должна быть следующая: если значение Variable is_startml равно "True", 
то перейти в таску с task_id="startml_desc", иначе перейти в таску с task_id="not_startml_desc". 
Затем объявите две задачи с task_id="startml_desc" и task_id="not_startml_desc".

NB: класс Variable возвращает строку!

В первой таске распечатайте "StartML is a starter course for ambitious people", во второй "Not a startML course, sorry".

Перед BranchPythonOperator можете поставить DummyOperator - он ничего не делает, но зато задает красивую "стартовую точку" на графе. 
Точно так же можете поставить DummyOperator в конце DAG.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator


def is_startml():
    from airflow.models import Variable
    is_startml = Variable.get("is_startml")
    if is_startml == "True":
        return 'startml_desc'
    else:
        return 'not_startml_desc'

def print_str(s):
    print(s)

with DAG(
        '12_omorozova',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='12_omorozova',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['12_omorozova'],
) as dag:
    date = "{{ ds }}"

    dummy_step_1 = DummyOperator(
        task_id="step_1",
        trigger_rule="all_success",
    )
    branch = BranchPythonOperator(
        task_id='branch',
        python_callable=is_startml,
        dag=dag)

    t1 = PythonOperator(
        task_id='startml_desc',
        python_callable=print_str,
        op_kwargs={'s':  "StartML is a starter course for ambitious people"},
    )

    t2 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=print_str,
        op_kwargs={'s':  "Not a startML course, sorry"},
    )

    dummy_step_last = DummyOperator(
        task_id="step_last",
        trigger_rule="all_success",
    )

    dummy_step_1 >> branch >> t1 >> dummy_step_last
    branch >> t2 >> dummy_step_last