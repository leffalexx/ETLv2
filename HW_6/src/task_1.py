from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
import random

default_args = {
    'start_date': datetime(2023, 1, 1)
}

dag = DAG('random_number', default_args=default_args, schedule_interval='@daily')

generate_number = BashOperator(
    task_id='generate_number',
    bash_command='echo $((RANDOM % 100))',
    dag=dag
)

print_number = BashOperator(
    task_id='print_number',
    bash_command='echo "Случайное число: ${NUMBER}"',
    env={'NUMBER': '{{ ti.xcom_pull(task_ids="generate_number") }}'},
    dag=dag
)

generate_number >> print_number
