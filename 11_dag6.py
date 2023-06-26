"""
Добавьте в PythonOperator из второго задания (где создавали 30 операторов в цикле) kwargs и передайте в этот kwargs task_number
 со значением переменной цикла. Также добавьте прием аргумента ts и run_id в функции, указанной в PythonOperator, 
 и распечатайте эти значения.
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from airflow.operators.bash import BashOperator

with DAG(
    'o-morozova_6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='o-morozova_6',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['o-morozova_6'],
) as dag:
        date = "{{ ds }}"
        for i in range(10):
                t_bash = BashOperator(
                        task_id="echo_" + str(i),
                        bash_command='f"echo ' + str(i) + '"',
                        dag=dag,  # говорим, что таска принадлежит дагу из переменной dag
                        env={"DATA_INTERVAL_START": date},
                )
                if i == 0:
                        t = t_bash
                else:
                        t >> t_bash
                        t = t_bash

        dag.doc_md = __doc__

        def print_str(task_number, ts, run_id):
                print("task number is:", task_number)
                print(ts, run_id)

        for j in range(10, 30):
                t_pth = PythonOperator(
                        task_id='print_str_' + str(j),
                        python_callable=print_str,
                        op_kwargs={'task_number': j, 'ts': "{{ ts }}", 'run_id': "{{ run_id }}"},
                )
                if j == 0:
                        t = t_bash
                else:
                        t >> t_pth
                        t = t_pth