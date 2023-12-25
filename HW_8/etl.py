import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 24)
}

dag = DAG(
    'process_data',
    default_args=default_args,
    schedule_interval='@daily')


def load_csv():
    booking_df = pd.read_csv('booking.csv')
    client_df = pd.read_csv('client.csv')
    hotel_df = pd.read_csv('hotel.csv')

    return {'booking': booking_df, 'client': client_df, 'hotel': hotel_df}


def transform(**context):
    booking_df = context['task_instance'].xcom_pull(task_ids='load_csv', key='booking')
    client_df = context['task_instance'].xcom_pull(task_ids='load_csv', key='client')
    hotel_df = context['task_instance'].xcom_pull(task_ids='load_csv', key='hotel')

    return merged_df


def load_to_db(**context):
    df = context['task_instance'].xcom_pull(task_ids='transform', key='return_value')


load_csv = PythonOperator(
    task_id='load_csv',
    python_callable=load_csv,
    dag=dag)

transform = PythonOperator(
    task_id='transform',
    python_callable=transform,
    provide_context=True,
    dag=dag)

load_to_db = PostgresOperator(
    task_id='load_to_db',
    postgres_conn_id='postgres_default',
    sql='...',
    dag=dag)

load_csv >> transform >> load_to_db
