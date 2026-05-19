import os
import subprocess
import sys
from airflow.providers.postgres.hooks.postgres import PostgresHook

def run_debug():
    try:
        print("Obteniendo credenciales de Db_caso_3_janner...")
        hook = PostgresHook(postgres_conn_id='Db_caso_3_janner')
        conn = hook.get_connection('Db_caso_3_janner')
        
        env = os.environ.copy()
        env['DBT_HOST'] = str(conn.host)
        env['DBT_PORT'] = str(conn.port if conn.port else 5432)
        env['DBT_USER'] = str(conn.login)
        env['DBT_PASSWORD'] = str(conn.password)
        env['DBT_DBNAME'] = str(conn.schema)
        
        project_dir = 'c:/Users/ASUS/Desktop/RIWI/ruta-avanzada/m5/m5-analitica/clusters/alzheimer/dags-alzheimer/dags/caso_3/dbt_caso3'
        
        cmd = [
            sys.executable, '-m', 'dbt.cli.main', 'parse',
            '--project-dir', project_dir,
            '--profiles-dir', project_dir
        ]
        
        print(f"Ejecutando dbt: {' '.join(cmd)}")
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        
        print("\n--- STDOUT ---")
        print(result.stdout)
        print("\n--- STDERR ---")
        print(result.stderr)
        print(f"\nExit Code: {result.returncode}")
        
    except Exception as e:
        print(f"Error interno: {e}")

if __name__ == '__main__':
    run_debug()
