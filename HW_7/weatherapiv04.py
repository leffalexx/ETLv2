import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.operators.python import get_current_context
from airflow.utils.dates import days_ago

API_KEY = 'bf8dfb3427c7ddafde6070979125c39d'
CITY = 'Yerevan'


def get_weather():
    url = f'http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}'
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        temp = data['main']['temp'] - 273.15
        print(f'Temperature in {CITY} is {temp:.2f}°C')
        return temp
    else:
        print(f'Error getting weather data. Status code: {response.status_code}')
        return None


def hot_branch(**kwargs):
    ti = get_current_context()['ti']
    temp = ti.xcom_pull(task_ids='branching')
    if temp is not None and temp > 15:
        print(f'Temperature is {temp:.2f}°C - Hot branch')
    else:
        print('Skipping hot branch due to temperature condition.')


def cold_branch(**kwargs):
    ti = get_current_context()['ti']
    temp = ti.xcom_pull(task_ids='branching')
    if temp is not None and temp <= 15:
        print(f'Temperature is {temp:.2f}°C - Cold branch')
    else:
        print('Skipping cold branch due to temperature condition.')

def branching(**kwargs):
    ti = get_current_context()['ti']
    temp = ti.xcom_pull(task_ids='get_temp')
    if temp is not None:
        print(f'Temperature is {temp:.2f}°C')
    else:
        print('Temperature is not available.')
    return True


with DAG(dag_id='weather_etl_V04', schedule_interval='@daily', start_date=days_ago(1)) as dag:
    get_temp = PythonOperator(task_id='get_temp', python_callable=get_weather)

    branching = ShortCircuitOperator(
        task_id='branching',
        python_callable=branching,
        provide_context=True,
    )

    hot_branch = PythonOperator(
        task_id='hot_branch',
        python_callable=hot_branch,
        provide_context=True,
    )

    cold_branch = PythonOperator(
        task_id='cold_branch',
        python_callable=cold_branch,
        provide_context=True,
    )

    get_temp >> branching >> [hot_branch, cold_branch]
