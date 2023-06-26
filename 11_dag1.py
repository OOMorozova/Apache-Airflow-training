"""
Напишите DAG, который будет содержать BashOperator и PythonOperator. В функции PythonOperator примите аргумент ds и распечатайте его. Можете распечатать дополнительно любое другое сообщение.

В BashOperator выполните команду pwd, которая выведет директорию, где выполняется ваш код Airflow. Результат может оказаться неожиданным, не пугайтесь - Airflow может запускать ваши задачи на разных машинах или контейнерах с разными настройками и путями по умолчанию.

Сделайте так, чтобы сначала выполнялся BashOperator, потом PythonOperator.
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from airflow.operators.bash import BashOperator

with DAG(
    'HW1',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='HW1 DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw1'],
) as dag:
        date = "{{ ds }}"
        t1 = BashOperator(
                task_id="pwd",
                bash_command="pwd",
                dag=dag,  # говорим, что таска принадлежит дагу из переменной dag
                env={"DATA_INTERVAL_START": date},
        )
        t1.doc_md = dedent(
                """
            #### Task Documentation
            # You can document your task using the attributes `doc_md` (markdown),
            `doc` (_plain text_), `doc_rst`, `doc_json`, `doc_yaml` which gets
            **rendered in the UI's**
            *Task Instance Details page.*
            """)

        def print_date(ds):
                print(ds)

        t2 = PythonOperator(
                task_id='print_date',
                python_callable=print_date,
                op_kwargs={'ds': date},
        )


        t1 >> t2