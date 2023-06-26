"""
Возьмите BashOperator из второго задания (где создавали task через цикл) и подбросьте туда переменную окружения NUMBER, 
чье значение будет равно i из цикла. Распечатайте это значение в команде, указанной в операторе (для этого используйте 
bash_command="echo $NUMBER"). `code 2`
 `Monospace`
 # bold text 2
 **bold text 2**
 ## italicized text 2
 *italicized text 2*
 _italicized text 2_
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from airflow.operators.bash import BashOperator

with DAG(
    'HW5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='HW2 DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw5'],
) as dag:
        date = "{{ ds }}"
        for i in range(10):
                t_bash = BashOperator(
                        task_id="echo_" + str(i),
                        bash_command="echo $NUMBER",
                        dag=dag,  # говорим, что таска принадлежит дагу из переменной dag
                        env={"NUMBER": i},
                )
                if i == 0:
                        t = t_bash
                else:
                        t >> t_bash
                        t = t_bash

        dag.doc_md = __doc__



