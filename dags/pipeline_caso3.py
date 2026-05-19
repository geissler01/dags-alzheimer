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

    # Tarea para ejecutar transformaciones dbt de forma aislada y segura
    # Tarea normal de Airflow (quitamos el aislamiento de external_python)
    @task
    def task_run_dbt():
        import os
        import subprocess
        import logging
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        # 1. Validación de existencia de vistas en la base de datos
        hook = PostgresHook(postgres_conn_id='Db_caso_3_janner')
        
        check_sql = """
        SELECT count(*) 
        FROM information_schema.views 
        WHERE table_schema = 'public_staging_layer'
        """
        records = hook.get_first(check_sql)
        if records and records[0] > 0:
            logging.info("[INFO] --> Las vistas de dbt ya existen en la capa 'public_staging_layer'. Omitiendo ejecución de dbt run.")
            return
            
        # 2. Recuperamos las credenciales
        conn = hook.get_connection('Db_caso_3_janner')
        
        env = os.environ.copy()
        env['DBT_HOST'] = str(conn.host)
        env['DBT_PORT'] = str(conn.port if conn.port else 5432)
        env['DBT_USER'] = str(conn.login)
        env['DBT_PASSWORD'] = str(conn.password)
        env['DBT_DBNAME'] = str(conn.schema)
        
        # Redirigimos las carpetas de escritura de dbt a /tmp para evitar 
        # que se estrelle por culpa del volumen de Solo Lectura (:ro)
        env['DBT_LOG_PATH'] = '/tmp/dbt_logs'
        env['DBT_TARGET_PATH'] = '/tmp/dbt_target'
        
        project_dir = '/opt/airflow/dags/caso_3/dbt_caso3'
        profiles_dir = '/opt/airflow/dags/caso_3/dbt_caso3'
        
        # 3. Ejecutamos dbt pasándolo como módulo para evitar los shebangs rotos del binario
        # Usamos explícitamente el Python del .venv, ya que la tarea ya no usa external_python
        cmd = [
            '/opt/airflow/.venv/bin/python', '-m', 'dbt.cli.main', 'run',
            '--select', 'staging',
            '--project-dir', project_dir,
            '--profiles-dir', profiles_dir
        ]
        
        logging.info("[INFO] --> Ejecutando modelos de dbt en la capa staging...")
        
        # 4. Ejecutamos capturando el output
        result = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            error_msg = f"La ejecución de dbt falló con código de salida: {result.returncode}\n\n"
            error_msg += f"--- DBT STDOUT ---\n{result.stdout}\n\n"
            error_msg += f"--- DBT STDERR ---\n{result.stderr}\n"
            raise Exception(error_msg)
        else:
            logging.info("[SUCCESS] --> Transformaciones dbt aplicadas correctamente en la capa 'public_staging_layer'.")

    # ---- Instanciación Única de Tareas ----
    task_create_schemas = task_create_schemas()
    task_ingest_random_users = task_ingest_random_users()
    task_ingest_kaggle = task_ingest_kaggle()
    task_ingest_movielens = task_ingest_movielens()
    task_load_users_from_s3 = task_load_users_from_s3()
    task_run_dbt = task_run_dbt()

    # ---- Definición de dependencias ----
    # 1. Las validaciones de entorno corren primero y habilitan la creación de esquemas y la ingesta de usuarios
    validation_group >> [task_create_schemas, task_ingest_random_users]
    
    # 2. Las ingestas de base de datos Postgres (Kaggle, MovieLens y la carga de S3) dependen de que los esquemas estén creados
    task_create_schemas >> [task_ingest_kaggle, task_ingest_movielens, task_load_users_from_s3]
    
    # 3. La carga de S3 a Postgres requiere que la subida del archivo CSV a S3 haya finalizado
    task_ingest_random_users >> task_load_users_from_s3

    # 4. Ejecutar dbt secuencialmente una vez que TODAS las ingestas a raw_layer hayan terminado exitosamente
    [task_ingest_kaggle, task_ingest_movielens, task_load_users_from_s3] >> task_run_dbt

# Instancia el grafo
elt_pipeline_caso_3()
