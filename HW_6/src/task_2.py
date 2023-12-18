from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import random


def generate_and_square():
    number = random.randint(1, 10)
    squared_number = number**2
    print(f'Случайное число: {number}')
    print(f'Число в квадрате: {squared_number}')


default_args = {
    'start_date': datetime(2023, 1, 1)
}

dag = DAG('random_square', default_args=default_args, schedule_interval='@daily')

t1 = PythonOperator(
    task_id='generate_and_print',
    python_callable=generate_and_square,
    dag=dag
)
