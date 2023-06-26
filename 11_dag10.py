"""
Напишите DAG, состоящий из одного PythonOperator. Этот оператор должен, используя подключение с conn_id="startml_feed", найти пользователя,
 который поставил больше всего лайков, и вернуть словарь {'user_id': <идентификатор>, 'count': <количество лайков>}. Эти значения, кстати,
   сохранятся в XCom.
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from airflow.providers.postgres.operators.postgres import PostgresHook
import psycopg2
from psycopg2.extras import RealDictCursor

with DAG(
        '10_omorozova',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='10_omorozova',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['10_omorozova'],
) as dag:
    date = "{{ ds }}"


    def user_max_likes():
        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """SELECT user_id, count(action) as count
                                             FROM "feed_action" 
                                            WHERE action = 'like'
                                            GROUP BY user_id
                                            ORDER BY 2 DESC
                                            LIMIT 1
                                        """
                )
                res = cursor.fetchone()
        return res


    t1 = PythonOperator(
        task_id='user_max_likes',
        python_callable=user_max_likes,
    )
