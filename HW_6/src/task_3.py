import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


def get_weather():
    location = "Yerevan"
    api_url = f"https://goweather.herokuapp.com/weather/{location}"

    response = requests.get(api_url)
    weather = response.json()

    print(f"Погода в городе {location}")
    print(weather)


default_args = {
    'start_date': datetime(2023, 1, 1)
}

dag = DAG('weather', default_args=default_args, schedule_interval="@daily")

get_weather_report = PythonOperator(
    task_id='get_weather_report',
    python_callable=get_weather,
    dag=dag
)
