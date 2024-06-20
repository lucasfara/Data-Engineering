from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import extract_api
import transform_data
import load_data
import send_alert

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 1),
    'retries': 1,
}

dag = DAG('data_pipeline', default_args=default_args, schedule_interval='@daily')

def transform_task(**kwargs):
    df_api = kwargs['ti'].xcom_pull(task_ids='extract_api_data')
    df_transformed = transform_data.transform_data(df_api)
    return df_transformed

def load_task(**kwargs):
    df_transformed = kwargs['ti'].xcom_pull(task_ids='transform_data')
    load_data.load_data(df_transformed)

t1 = PythonOperator(
    task_id='extract_api_data', 
    python_callable=extract_api.extract_api_data, 
    dag=dag
)

t2 = PythonOperator(
    task_id='transform_data', 
    python_callable=transform_task, 
    provide_context=True,
    dag=dag
)

t3 = PythonOperator(
    task_id='load_data', 
    python_callable=load_task, 
    provide_context=True,
    dag=dag
)

t4 = PythonOperator(
    task_id='send_alert', 
    python_callable=lambda: send_alert.send_alert("Alert message"), 
    dag=dag
)

t1 >> t2 >> t3 >> t4
