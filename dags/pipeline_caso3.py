from airflow.sdk import dag, task, TaskGroup
from datetime import datetime, timedelta

# Entorno virtual centralizado y PYTHONPATH para simplificar tareas del DAG
VENV_PYTHON = '/opt/airflow/.venv/bin/python'
ENV_CONFIG = {"PYTHONPATH": "/opt/airflow/dags"}

# Configuraciones basicas por defecto del DAG
default_args = {
    'owner': 'Draco',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

@dag(
    dag_id='elt_pipeline_caso_3',
    default_args=default_args,
    description='Pipeline Modular ELT Caso 3 - Ingestas y Transformacion DBT',
    schedule=None,
    start_date=datetime(2026, 5, 18),
    catchup=False,
    tags=['caso 3', 'dbt', 'postgres', 's3', 'kaggle']
)
def elt_pipeline_caso_3():

    # ---- 1. CAPA DE VALIDACIONES DISTRIBUIDAS ----
    # Agrupa las tareas de diagnostico en paralelo
    with TaskGroup(group_id='validation_layer') as validation_group:

        # Validacion de Postgres en entorno virtual (.venv)
        @task.external_python(python=VENV_PYTHON, env_vars=ENV_CONFIG)
        def task_validate_postgres():
            from caso_3.tasks.test_conextions.validate_connections import validate_postgres
            validate_postgres()

        # Validacion de AWS S3 en entorno virtual (.venv)
        @task.external_python(python=VENV_PYTHON, env_vars=ENV_CONFIG)
        def task_validate_s3():
            from caso_3.tasks.test_conextions.validate_connections import validate_s3
            validate_s3()

        # Validacion de Kaggle en entorno virtual (.venv)
        @task.external_python(python=VENV_PYTHON, env_vars=ENV_CONFIG)
        def task_validate_kaggle():
            from caso_3.tasks.test_conextions.validate_connections import validate_kaggle
            validate_kaggle()

        # Al colocarlas en una lista paralela, Celery las distribuye a diferentes workers de AWS
        [task_validate_postgres(), task_validate_s3(), task_validate_kaggle()]

# Instancia el grafo
elt_pipeline_caso_3()
