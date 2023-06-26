"""
Создайте новый DAG, содержащий два PythonOperator. Первый оператор должен вызвать функцию, возвращающую строку 
"Airflow tracks everything".

Второй оператор должен получить эту строку через XCom.
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from airflow.operators.bash import BashOperator

with DAG(
    '9_omorozova',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='9_omorozova',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['9_omorozova'],
) as dag:
        date = "{{ ds }}"
        def save_com(ti):
                s = "Airflow tracks everything"
                return s

        def print_com(ti):
                res = ti.xcom_pull(
                        key='return_value',
                        task_ids='save_com'
                )
                print(res)

        t1 = PythonOperator(
                task_id='save_com',
                python_callable=save_com,
        )


        t2 = PythonOperator(
                task_id='print_com',
                python_callable=print_com,
        )


        t1 >> t2