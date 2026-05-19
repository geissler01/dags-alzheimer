from airflow.sdk import dag, task, TaskGroup
from datetime import datetime, timedelta

# =========================================================================
# CONFIGURACIONES GLOBALES DEL DAG
# =========================================================================
# Definimos los parámetros base para asegurar que las tareas se reintenten 
# en caso de fallos temporales de red o indisponibilidad de la base de datos.
default_args = {
    'owner': 'Draco',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

@dag(
    dag_id='elt_pipeline_caso_3',
    default_args=default_args,
    description='Pipeline Modular ELT Caso 3 - Ingestas Crudas y Transformación dbt',
    schedule=None,  # Configurado para ejecución manual por el momento
    start_date=datetime(2026, 5, 18),
    catchup=False,
    tags=['caso 3', 'dbt', 'postgres', 's3', 'kaggle']
)
def elt_pipeline_caso_3():

    # =========================================================================
    # 1. CAPA DE VALIDACIONES DISTRIBUIDAS (PRE-REQUISITOS)
    # =========================================================================
    # Agrupamos las tareas de diagnóstico. Estas validan que las credenciales
    # y conexiones a Postgres, S3 y Kaggle funcionen ANTES de iniciar descargas pesadas.
    with TaskGroup(group_id='validation_layer') as validation_group:

        # Validamos Postgres usando el Python de nuestro entorno virtual (.venv)
        @task.external_python(python='/opt/airflow/.venv/bin/python')
        def task_validate_postgres():
            import sys
            from pathlib import Path
            
            # Aseguramos que el worker de Celery/Airflow encuentre nuestra carpeta local de dags
            dag_dir = Path('/opt/airflow/dags').resolve()
            if str(dag_dir) not in sys.path:
                sys.path.append(str(dag_dir))
                
            from caso_3.tasks.test_conextions.validate_connections import validate_postgres
            validate_postgres()

        # Validamos credenciales de AWS S3
        @task.external_python(python='/opt/airflow/.venv/bin/python')
        def task_validate_s3():
            import sys
            from pathlib import Path
            
            dag_dir = Path('/opt/airflow/dags').resolve()
            if str(dag_dir) not in sys.path:
                sys.path.append(str(dag_dir))
                
            from caso_3.tasks.test_conextions.validate_connections import validate_s3
            validate_s3()

        # Validamos la API Key de Kaggle
        @task.external_python(python='/opt/airflow/.venv/bin/python')
        def task_validate_kaggle():
            import sys
            from pathlib import Path
            
            dag_dir = Path('/opt/airflow/dags').resolve()
            if str(dag_dir) not in sys.path:
                sys.path.append(str(dag_dir))
                
            from caso_3.tasks.test_conextions.validate_connections import validate_kaggle
            validate_kaggle()

        # Las colocamos en una lista para que Airflow las ejecute en paralelo
        [task_validate_postgres(), task_validate_s3(), task_validate_kaggle()]

    # =========================================================================
    # 2. CAPA DE INGESTAS (EXTRACTION & LOAD - EL)
    # =========================================================================
    
    # Creamos los esquemas analíticos (staging, intermediate, marts) en Postgres si no existen
    @task.external_python(python='/opt/airflow/.venv/bin/python')
    def task_create_schemas():
        import sys
        from pathlib import Path
        
        dag_dir = Path('/opt/airflow/dags').resolve()
        if str(dag_dir) not in sys.path:
            sys.path.append(str(dag_dir))
            
        from caso_3.tasks.extraction_load.create_schema import create_analytical_schemas
        create_analytical_schemas()

    # Extraemos usuarios aleatorios de una API pública y los resguardamos en Amazon S3
    @task.external_python(python='/opt/airflow/.venv/bin/python')
    def task_ingest_random_users():
        import sys
        from pathlib import Path
        
        dag_dir = Path('/opt/airflow/dags').resolve()
        if str(dag_dir) not in sys.path:
            sys.path.append(str(dag_dir))
            
        from caso_3.tasks.extraction_load.ingestion_user import users_from_api_to_s3
        users_from_api_to_s3()

    # Descargamos los datasets de Netflix y Spotify desde Kaggle y los cargamos a Postgres
    @task.external_python(python='/opt/airflow/.venv/bin/python')
    def task_ingest_kaggle():
        import sys
        from pathlib import Path
        
        dag_dir = Path('/opt/airflow/dags').resolve()
        if str(dag_dir) not in sys.path:
            sys.path.append(str(dag_dir))
            
        from caso_3.tasks.extraction_load.ingestion_from_kaggle import ingest_kaggle_to_postgres
        ingest_kaggle_to_postgres()

    # Ingesta pesada: Descargamos y cargamos el dataset masivo de MovieLens 25M por chunks
    @task.external_python(python='/opt/airflow/.venv/bin/python')
    def task_ingest_movielens():
        import sys
        from pathlib import Path
        
        dag_dir = Path('/opt/airflow/dags').resolve()
        if str(dag_dir) not in sys.path:
            sys.path.append(str(dag_dir))
            
        from caso_3.tasks.extraction_load.ingestion_from_movielens import ingest_movielens_to_postgres
        ingest_movielens_to_postgres()

    # Leemos los usuarios guardados en S3 y los cargamos en la base de datos Postgres
    @task.external_python(python='/opt/airflow/.venv/bin/python')
    def task_load_users_from_s3():
        import sys
        from pathlib import Path
        
        dag_dir = Path('/opt/airflow/dags').resolve()
        if str(dag_dir) not in sys.path:
            sys.path.append(str(dag_dir))
            
        from caso_3.tasks.extraction_load.ingestion_from_s3 import ingest_s3_to_postgres
        ingest_s3_to_postgres()

    # =========================================================================
    # 3. CAPA DE TRANSFORMACIÓN (TRANSFORM - T) -> dbt
    # =========================================================================
    # Ejecutamos dbt pasándolo como módulo nativo de Python para evitar errores de shebang
    @task
    def task_run_dbt():
        import os
        import subprocess
        import logging
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        # 3.1 Extraemos credenciales seguras de Airflow y las inyectamos en el entorno
        hook = PostgresHook(postgres_conn_id='Db_caso_3_janner')
        conn = hook.get_connection('Db_caso_3_janner')
        
        env = os.environ.copy()
        env['DBT_HOST'] = str(conn.host)
        env['DBT_PORT'] = str(conn.port if conn.port else 5432)
        env['DBT_USER'] = str(conn.login)
        env['DBT_PASSWORD'] = str(conn.password)
        env['DBT_DBNAME'] = str(conn.schema)
        
        # IMPORTANTE: Redirigimos los logs y la compilación de dbt hacia /tmp.
        # Esto soluciona los problemas de volúmenes de Solo Lectura (:ro) en AWS/Docker.
        env['DBT_LOG_PATH'] = '/tmp/dbt_logs'
        env['DBT_TARGET_PATH'] = '/tmp/dbt_target'
        
        project_dir = '/opt/airflow/dags/caso_3/dbt_caso3'
        profiles_dir = '/opt/airflow/dags/caso_3/dbt_caso3'
        
        # 3.2 Comando de ejecución de dbt usando el entorno virtual
        cmd = [
            '/opt/airflow/.venv/bin/python', '-m', 'dbt.cli.main', 'run',
            '--project-dir', project_dir,
            '--profiles-dir', profiles_dir
        ]
        
        logging.info("[INFO] --> Ejecutando pipeline completo de dbt (Staging -> Intermediate -> Marts)...")
        
        # 3.3 Ejecutamos el subproceso y capturamos los logs
        result = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True
        )
        
        # 3.4 Manejo de errores
        if result.returncode != 0:
            error_msg = f"La compilación de dbt falló (Código {result.returncode})\n\n"
            error_msg += f"--- LOGS DE ERROR ---\n{result.stdout}\n\n"
            error_msg += f"{result.stderr}\n"
            raise Exception(error_msg)
        else:
            logging.info("[EXITO] --> ¡Modelo Kimball (start() construido perfectamente! Transformaciones completadas.")

    # =========================================================================
    # ORQUESTACIÓN DEL FLUJO (DEPENDENCIAS Y GRAFO)
    # =========================================================================
    # Instanciamos las tareas
    task_create_schemas = task_create_schemas()
    task_ingest_random_users = task_ingest_random_users()
    task_ingest_kaggle = task_ingest_kaggle()
    task_ingest_movielens = task_ingest_movielens()
    task_load_users_from_s3 = task_load_users_from_s3()
    task_run_dbt = task_run_dbt()

    # Regla 1: Las validaciones de entorno (S3, Postgres, Kaggle) deben pasar 
    # antes de crear esquemas y extraer usuarios de la API.
    validation_group >> [task_create_schemas, task_ingest_random_users]
    
    # Regla 2: Los esquemas en la base de datos deben existir antes de hacer cualquier COPY o INSERT.
    task_create_schemas >> [task_ingest_kaggle, task_ingest_movielens, task_load_users_from_s3]
    
    # Regla 3: No podemos cargar usuarios a Postgres si aún no los hemos extraído y subido a S3.
    task_ingest_random_users >> task_load_users_from_s3

    # Regla 4: La Transformación analítica (dbt) espera rigurosamente a que TODAS las fuentes crudas terminen de cargar.
    [task_ingest_kaggle, task_ingest_movielens, task_load_users_from_s3] >> task_run_dbt

# Arrancamos el motor
elt_pipeline_caso_3()
