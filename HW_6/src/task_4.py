from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2023, 1, 1)
}

dag = DAG('my_dag', default_args=default_args, schedule_interval=None)

t1 = BashOperator(
    task_id='print_number',
    bash_command='echo "Случайное число: $((RANDOM%10))"',
    dag=dag
)

t2 = PythonOperator(
    task_id='square_number',
    python_callable=generate_and_square,
    dag=dag
)

t3 = PythonOperator(
    task_id='get_weather',
    python_callable=get_weather,
    dag=dag
)

t1 >> t2 >> t3