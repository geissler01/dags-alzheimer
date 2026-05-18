from airflow.sdk import dag, task, TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging
import os

# Importamos las tareas modulares
from tasks.analytics_pipeline.extract_tasks import extract_from_api
from tasks.analytics_pipeline.kaggle_tasks import extract_kaggle_data
from tasks.analytics_pipeline.transform_tasks import transform_analytics_data
from tasks.analytics_pipeline.report_tasks import generate_and_upload_reports

default_args = {
    'owner': 'draco',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

@dag(
    dag_id='analytics_pipeline_v8',
    default_args=default_args,
    description='ETL Completo con Branching y Reportes en S3',
    schedule=None,
    start_date=datetime(2026, 5, 11),
    catchup=False,
    tags=['ejercicio 8', 'aws', 'branching', 's3']
)
def analytics_pipeline():

    # ---- 1. SETUP ----
    @task
    def init_db_schema():
        dag_dir = os.path.dirname(os.path.abspath(__file__)) # Me lleva a la carpera raiz
        sql_path = os.path.join(dag_dir, 'sql', 'crear_tabla_ejercicio_8.sql') # construeye la ruta hasta el schema o hacia donde queramos
        with open(sql_path, 'r') as f: # abre el archivo sql
            sql = f.read() # convierte a texto todo lo que hay dentro del schema
        hook = PostgresHook(postgres_conn_id='postgres_aws_ejercicio_8') # crea la conexion con la base de datos
        hook.run(sql) # corre el comando sql para crear la tabla
        return True

    # ---- 2. EXTRACCIÓN ----
    with TaskGroup(group_id='extraction_layer') as extraction_group:
        @task
        def extract_users_a(): return extract_from_api(results=500)
        @task
        def extract_users_b(): return extract_from_api(results=500)
        @task
        def extract_population(): return extract_kaggle_data()
        [extract_users_a(), extract_users_b(), extract_population()]

    # ---- 3. TRANSFORMACIÓN ----
    @task
    def process_data(users_a, users_b, population):
        return transform_analytics_data([users_a, users_b], population)

    # ---- 4. CARGA ----
    @task
    def load_data(enriched_data):
        hook = PostgresHook(postgres_conn_id='postgres_aws_ejercicio_8')
        rows = [(d['first_name'], d['last_name'], d['gender'], d['email'], 
                 d['city'], d['country'], d['age'], d['population_count'], d['population_year']) 
                for d in enriched_data]
        target_fields = ['first_name', 'last_name', 'gender', 'email', 'city', 'country', 'age', 'population_count', 'population_year']
        hook.insert_rows(table='enriched_users_poblacion', rows=rows, target_fields=target_fields)
        return enriched_data

    # ---- 5. BRANCHING (Lógica de decisión) ----
    @task.branch
    def decide_reporting_path(data):
        """
        Si tenemos más de 500 registros exitosos, generamos todos los reportes.
        Si no, solo el JSON básico.
        """
        if len(data) > 500:
            return 'generate_full_reports'
        else:
            return 'generate_simple_report'

    # ---- 6. REPORTES ----
    @task
    def generate_full_reports(data):
        return generate_and_upload_reports(data, formats=['csv', 'json', 'txt'])

    @task
    def generate_simple_report(data):
        return generate_and_upload_reports(data, formats=['json'])

    # ---- FLUJO ----
    setup = init_db_schema()
    u_a = extract_users_a()
    u_b = extract_users_b()
    pop = extract_population()
    
    setup >> [u_a, u_b, pop]
    
    results = process_data(u_a, u_b, pop)
    enriched_data = load_data(results)
    
    # El branching recibe los datos enriquecidos
    path = decide_reporting_path(enriched_data)
    
    # Definimos los caminos posibles
    path >> [generate_full_reports(enriched_data), generate_simple_report(enriched_data)]

analytics_pipeline()
