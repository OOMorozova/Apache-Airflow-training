"""
Напишите DAG, состоящий из одного PythonOperator. Этот оператор должен печатать значение Variable с названием is_startml
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def print_is_startml():
    from airflow.models import Variable
    is_startml = Variable.get("is_startml")
    print(is_startml)
    return is_startml

with DAG(
        '11_omorozova',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='11_omorozova',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['11_omorozova'],
) as dag:
    date = "{{ ds }}"

    t1 = PythonOperator(
        task_id='print_is_startml',
        python_callable=print_is_startml,
    )
