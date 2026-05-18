from airflow.sdk import dag, task, TaskGroup
from datetime import datetime, timedelta

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
        @task.external_python(python='/opt/airflow/.venv/bin/python')
        def task_validate_postgres():
            import sys
            from pathlib import Path
            
            # Agrega la ruta de dags al sys.path del worker
            dag_dir = Path('/opt/airflow/dags').resolve()
            if str(dag_dir) not in sys.path:
                sys.path.append(str(dag_dir))
                
            from caso_3.tasks.test_conextions.validate_connections import validate_postgres
            validate_postgres()

        # Validacion de AWS S3 en entorno virtual (.venv)
        @task.external_python(python='/opt/airflow/.venv/bin/python')
        def task_validate_s3():
            import sys
            from pathlib import Path
            
            dag_dir = Path('/opt/airflow/dags').resolve()
            if str(dag_dir) not in sys.path:
                sys.path.append(str(dag_dir))
                
            from caso_3.tasks.test_conextions.validate_connections import validate_s3
            validate_s3()

        # Validacion de Kaggle en entorno virtual (.venv)
        @task.external_python(python='/opt/airflow/.venv/bin/python')
        def task_validate_kaggle():
            import sys
            from pathlib import Path
            
            dag_dir = Path('/opt/airflow/dags').resolve()
            if str(dag_dir) not in sys.path:
                sys.path.append(str(dag_dir))
                
            from caso_3.tasks.test_conextions.validate_connections import validate_kaggle
            validate_kaggle()

        # Al colocarlas en una lista paralela, Celery las distribuye a diferentes workers de AWS
        [task_validate_postgres(), task_validate_s3(), task_validate_kaggle()]

    # ---- 2. CAPA DE PREPARACIÓN E INGESTAS ----
    # Tarea para crear/verificar los esquemas analíticos en la base de datos Postgres
    @task.external_python(python='/opt/airflow/.venv/bin/python')
    def task_create_schemas():
        import sys
        from pathlib import Path
        
        dag_dir = Path('/opt/airflow/dags').resolve()
        if str(dag_dir) not in sys.path:
            sys.path.append(str(dag_dir))
            
        from caso_3.tasks.extraction_load.create_schema import create_analytical_schemas
        create_analytical_schemas()

    # Tarea para descargar usuarios de la API y subirlos a S3 (con validación de existencia)
    @task.external_python(python='/opt/airflow/.venv/bin/python')
    def task_ingest_random_users():
        import sys
        from pathlib import Path
        
        dag_dir = Path('/opt/airflow/dags').resolve()
        if str(dag_dir) not in sys.path:
            sys.path.append(str(dag_dir))
            
        from caso_3.tasks.extraction_load.ingestion_user import users_from_api_to_s3
        users_from_api_to_s3()

    # Tarea para descargar datasets de Kaggle (Netflix, Spotify) y cargarlos a Postgres
    @task.external_python(python='/opt/airflow/.venv/bin/python')
    def task_ingest_kaggle():
        import sys
        from pathlib import Path
        
        dag_dir = Path('/opt/airflow/dags').resolve()
        if str(dag_dir) not in sys.path:
            sys.path.append(str(dag_dir))
            
        from caso_3.tasks.extraction_load.ingestion_from_kaggle import ingest_kaggle_to_postgres
        ingest_kaggle_to_postgres()

    # Tarea para descargar dataset de MovieLens 25M y cargarlo a Postgres en chunks
    @task.external_python(python='/opt/airflow/.venv/bin/python')
    def task_ingest_movielens():
        import sys
        from pathlib import Path
        
        dag_dir = Path('/opt/airflow/dags').resolve()
        if str(dag_dir) not in sys.path:
            sys.path.append(str(dag_dir))
            
        from caso_3.tasks.extraction_load.ingestion_from_movielens import ingest_movielens_to_postgres
        ingest_movielens_to_postgres()

    # Tarea para descargar usuarios desde Amazon S3 y cargarlos a Postgres
    @task.external_python(python='/opt/airflow/.venv/bin/python')
    def task_load_users_from_s3():
        import sys
        from pathlib import Path
        
        dag_dir = Path('/opt/airflow/dags').resolve()
        if str(dag_dir) not in sys.path:
            sys.path.append(str(dag_dir))
            
        from caso_3.tasks.extraction_load.ingestion_from_s3 import ingest_s3_to_postgres
        ingest_s3_to_postgres()

    # ---- Definición de dependencias ----
    # Las validaciones deben pasar primero para asegurar que el entorno este listo
    validation_group >> [task_create_schemas(), task_ingest_random_users()]
    
    # La ingesta de base de datos requiere que primero se creen los esquemas analiticos
    task_create_schemas() >> [task_ingest_kaggle(), task_ingest_movielens()]
    
    # La carga de S3 a Postgres requiere que el archivo este en S3 y que el esquema Postgres este creado
    [task_ingest_random_users(), task_create_schemas()] >> task_load_users_from_s3()

# Instancia el grafo
elt_pipeline_caso_3()
