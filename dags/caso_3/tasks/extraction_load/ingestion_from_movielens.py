import os
import sys
import tempfile
import shutil
import requests
import zipfile
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

def ingest_movielens_to_postgres():
    logging.info("[INFO] Iniciando ingesta unificada desde MovieLens a Postgres")
    
    url = "https://files.grouplens.org/datasets/movielens/ml-25m.zip"
    
    # Crear carpeta temporal segura en el HOME del worker (evitando dags de solo lectura y RAM tmpfs)
    temp_dir = Path.home() / "temp_movielens_data"
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    zip_path = temp_dir / "ml-25m.zip"
    
    engine = get_db_engine()
    
    # 1. Validar si las tablas ya existen y tienen datos para evitar descargas e ingestas repetidas
    files_to_load = {
        "genome-scores.csv": "movielens_genome_scores",
        "genome-tags.csv": "movielens_genome_tags",
        "links.csv": "movielens_links",
        "movies.csv": "movielens_movies",
        "ratings.csv": "movielens_ratings",
        "tags.csv": "movielens_tags"
    }
    
    all_loaded = True
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            for table_name in files_to_load.values():
                # Verificar si existe la tabla en raw_layer
                check_exists = conn.execute(
                    text("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'raw_layer' AND table_name = :t)"),
                    {"t": table_name}
                ).scalar()
                
                if not check_exists:
                    all_loaded = False
                    break
                    
                # Verificar si tiene al menos una fila
                count = conn.execute(text(f"SELECT COUNT(1) FROM raw_layer.{table_name}")).scalar()
                if count == 0:
                    all_loaded = False
                    break
    except Exception:
        # En caso de error (por ejemplo, si el schema raw_layer aun no esta creado), procedemos con la ingesta
        all_loaded = False
        
    if all_loaded:
        logging.info("[INFO] --> Las tablas de MovieLens ya existen y tienen datos en la capa 'raw_layer'. Omitiendo descarga e ingesta.")
        return

    try:
        # 2. Descarga controlada mediante Streaming (5MB chunks para maxima velocidad teorica)
        logging.info(f"[INFO] --> Iniciando descarga de MovieLens 25M desde: {url}")
        with requests.get(url, stream=True) as response:
            response.raise_for_status()
            with open(zip_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=5 * 1024 * 1024):
                    if chunk:
                        f.write(chunk)
                        
        logging.info(f"[SUCCESS] --> ZIP de MovieLens descargado en: {zip_path}")
        
        # 3. Descompresión del archivo ZIP
        logging.info("[INFO] --> Descomprimiendo archivos...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
            
        # Buscar recursivamente la carpeta que contiene los CSVs extraidos
        csv_dir = None
        for p in temp_dir.rglob("movies.csv"):
            csv_dir = p.parent
            break
            
        if not csv_dir or not csv_dir.exists():
            logging.error("[ERROR] No se encontraron los archivos CSV extraidos de MovieLens.")
            return
            
        # 4. Carga en base de datos con optimización de memoria (Pandas chunking + COPY)
        for file_name, table_name in files_to_load.items():
            full_path = csv_dir / file_name
            if not full_path.exists():
                logging.warning(f"[WARNING] El archivo {file_name} no existe en la carpeta extraida.")
                continue
                
            logging.info(f"[INFO] --> Cargando {file_name} a la tabla raw_layer.{table_name} en chunks...")
            first_chunk = True
            
            # Leemos en chunks de 500,000 filas para acelerar la carga en la DB (apoyado en la Swap de 4GB)
            for chunk_df in pd.read_csv(full_path, dtype=str, chunksize=500000):
                chunk_df.to_sql(
                    name=table_name,
                    con=engine,
                    schema="raw_layer",
                    if_exists="replace" if first_chunk else "append",
                    index=False,
                    method=psql_insert_copy
                )
                first_chunk = False
                
            logging.info(f"[SUCCESS] --> Tabla raw_layer.{table_name} cargada exitosamente.")
            
        logging.info("[SUCCESS] >> Ingesta y carga completa de MovieLens finalizada exitosamente.")
        
    finally:
        # Limpieza de archivos temporales
        try:
            shutil.rmtree(temp_dir)
            logging.info("[INFO] Archivos temporales de MovieLens eliminados para liberar espacio.")
        except Exception as e:
            logging.warning(f"[WARNING] No se pudo limpiar la carpeta temporal: {e}")

# if __name__ == "__main__":
#     ingest_movielens_to_postgres()
