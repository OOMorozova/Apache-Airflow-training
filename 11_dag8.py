"""
Сделайте новый DAG, содержащий два Python оператора. Первый PythonOperator должен класть в XCom значение "xcom test" по ключу 
"sample_xcom_key".

Второй PythonOperator должен доставать это значение и печатать его. Настройте правильно последовательность операторов.
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from airflow.operators.bash import BashOperator

with DAG(
    '8_omorozova',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='8_omorozova',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['8_omorozova'],
) as dag:
        date = "{{ ds }}"
        def save_com(ti):
                ti.xcom_push(
                        key='sample_xcom_key',
                        value='xcom test'
                )

        def print_com(ti):
                res = ti.xcom_pull(
                        key='sample_xcom_key',
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