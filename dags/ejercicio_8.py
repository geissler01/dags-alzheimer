from airflow import DAG
from datetime import datetime

with DAG(
    dag_id = 'ejercicio_8',
    start_date = datetime(2026,4,11),
    schedule = None,
    catchup = False
) as dag:
    pass