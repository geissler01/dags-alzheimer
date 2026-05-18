from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine

def get_db_engine():
    # Obtiene la URI de la DB configurada en la UI de Airflow como 'Db_caso_3_janner'
    hook = PostgresHook(postgres_conn_id='Db_caso_3_janner')
    return create_engine(hook.get_uri())
