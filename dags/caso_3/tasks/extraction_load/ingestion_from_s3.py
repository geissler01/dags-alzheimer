import os
import sys
from pathlib import Path
from io import StringIO
import csv
import logging

# Agrega la carpeta dags al sys.path para permitir importacion absoluta
dag_dir = Path(__file__).resolve().parents[2]
if str(dag_dir) not in sys.path:
    sys.path.append(str(dag_dir))

# AHORA importamos las demas librerias
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from caso_3.tasks.services.db_conection import get_db_engine

def psql_insert_copy(table, conn, keys, data_iter):
    # Obtiene la conexion nativa del driver
    db_api_conn = conn.connection
    with db_api_conn.cursor() as cur:
        # Crea un buffer en memoria RAM
        s_buf = StringIO()
        writer = csv.writer(s_buf) 
        writer.writerows(data_iter)
        s_buf.seek(0)

        # Extrae el nombre del schema y la tabla
        column = ', '.join(f'"{k}"' for k in keys)
        if table.schema:
            table_name = f'"{table.schema}"."{table.name}"'
        else:
            table_name = f'"{table.name}"'

        # Construye el comando COPY
        sql = f'COPY {table_name}({column}) FROM STDIN WITH CSV'
        
        # Resiliencia hibrida psycopg2/psycopg3
        if hasattr(cur, 'copy'):
            # psycopg3 (Nativo en Airflow 3)
            with cur.copy(sql) as copy_op:
                copy_op.write(s_buf.getvalue())
        else:
            # psycopg2 (Local / Fallback)
            cur.copy_expert(sql=sql, file=s_buf)

def ingest_s3_to_postgres():
    logging.info("[INFO] Iniciando ingesta de usuarios desde Amazon S3 a Postgres")
    
    bucket_name = "draco-caso-3-users"
    s3_key = "raw/users_random.csv"
    aws_conn_id = "my_s3_conn"
    table_name = "users_generated"
    
    engine = get_db_engine()
    
    # 1. Validar si la tabla ya existe y tiene datos para evitar ingestas repetidas
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            check_exists = conn.execute(
                text("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'raw_layer' AND table_name = :t)"),
                {"t": table_name}
            ).scalar()
            
            if check_exists:
                count = conn.execute(text(f"SELECT COUNT(1) FROM raw_layer.{table_name}")).scalar()
                if count > 0:
                    logging.info(f"[INFO] --> La tabla 'raw_layer.{table_name}' ya existe y tiene datos en Postgres. Omitiendo descarga desde S3.")
                    return
    except Exception as e:
        logging.warning(f"[WARNING] No se pudo verificar existencia de la tabla {table_name}: {e}")

    # 2. Descargar de S3 usando S3Hook de Airflow
    logging.info(f"[INFO] --> Conectando a S3 (conn_id: {aws_conn_id}) para descargar s3://{bucket_name}/{s3_key}")
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    
    if not s3_hook.check_for_key(s3_key, bucket_name):
        logging.error(f"[ERROR] El archivo s3://{bucket_name}/{s3_key} no existe. Asegurate de que la tarea de ingesta de la API haya finalizado exitosamente.")
        raise FileNotFoundError(f"Archivo s3://{bucket_name}/{s3_key} no encontrado.")
        
    csv_data = s3_hook.read_key(s3_key, bucket_name)
    logging.info(f"[SUCCESS] --> Archivo S3 leido con exito. Cargando a la tabla raw_layer.{table_name}...")
    
    # 3. Cargar en chunks usando StringIO y COPY
    try:
        first_chunk = True
        # Leemos el buffer en chunks de 50,000 registros para optimizar el uso de RAM
        for chunk_df in pd.read_csv(StringIO(csv_data), dtype=str, chunksize=50000):
            chunk_df.to_sql(
                name=table_name,
                con=engine,
                schema="raw_layer",
                if_exists="replace" if first_chunk else "append",
                index=False,
                method=psql_insert_copy
            )
            first_chunk = False
            
        logging.info(f"[SUCCESS] >> Tabla raw_layer.{table_name} cargada exitosamente desde S3.")
    except Exception as e:
        logging.error(f"[ERROR] Fallo al cargar los datos de S3 a Postgres: {e}")
        raise e

# if __name__ == "__main__":
#     ingest_s3_to_postgres()
