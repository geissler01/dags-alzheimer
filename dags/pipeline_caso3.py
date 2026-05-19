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
    @task.external_python(python='/opt/airflow/.venv/bin/python')
    def task_run_dbt():
        import os
        import subprocess
        import sys
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        # 1. Recuperamos las credenciales seguras de Postgres desde la conexión centralizada en la UI de Airflow
        hook = PostgresHook(postgres_conn_id='Db_caso_3_janner')
        conn = hook.get_connection('Db_caso_3_janner')
        
        # 2. Inyectamos las credenciales en variables de entorno del subproceso en RAM (nunca se escriben en disco)
        env = os.environ.copy()
        env['DBT_HOST'] = str(conn.host)
        env['DBT_PORT'] = str(conn.port if conn.port else 5432)
        env['DBT_USER'] = str(conn.login)
        env['DBT_PASSWORD'] = str(conn.password)
        env['DBT_DBNAME'] = str(conn.schema)
        
        # 3. Definimos las rutas del proyecto en el contenedor del worker
        project_dir = '/opt/airflow/dags/caso_3/dbt_caso3'
        profiles_dir = '/opt/airflow/dags/caso_3/dbt_caso3'
        
        # 4. Comando de dbt
        # Utilizamos la API oficial de dbtRunner para ejecutar dbt de forma programática.
        # Esto esquiva los binarios rotos de uv, los shebangs incompatibles y los fallos de runpy.
        dbt_script = """
import sys
from dbt.cli.main import dbtRunner
res = dbtRunner().invoke(sys.argv[1:])
if not res.success:
    sys.exit(2)
"""
        cmd = [
            sys.executable, '-c', dbt_script, 'run',
            '--select', 'staging',
            '--project-dir', project_dir,
            '--profiles-dir', profiles_dir
        ]
        import logging
        logger = logging.getLogger("airflow.task")
        
        logger.info(f"Iniciando subproceso dbt: {' '.join(cmd)}")
        
        # 5. Ejecutamos dbt de forma síncrona capturando todo el output
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
