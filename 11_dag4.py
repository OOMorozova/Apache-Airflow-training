"""
Создайте новый DAG, состоящий из одного BashOperator. Этот оператор должен  использовать шаблонизированную команду следующего вида: 
"Для каждого i в диапазоне от 0 до 5 не включительно распечатать значение ts и затем распечатать значение run_id".
Здесь ts и run_id - это шаблонные переменные (вспомните, как в лекции подставляли шаблонные переменные).
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from airflow.operators.bash import BashOperator

with DAG(
    'HW4',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='HW4 DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw1'],
) as dag:
        date = "{{ ds }}"
        templated_command = dedent(
                """
            {% for i in range(5) %}
                echo "{{ ts }}"
                echo "{{ run_id }}"
            {% endfor %}
            """
        )
        t1 = BashOperator(
                task_id="templated_command",
                bash_command=templated_command,
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

