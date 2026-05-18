# librerias
from unicodedata import name
from pathlib import Path
import sys
import pandas as pd
from sqlalchemy import text

# librerias necesarias para usar copy
import csv
from io import StringIO

# construccion de rutas
root_dir = Path(__file__).parents[2]
sys.path.append(str(root_dir))

# importacion de modulos
# pyrefly: ignore [missing-import]
from src.utils.db import get_conn

# transversales
engine = get_conn()

def load_netflix_tables():
    # ruta del archivo
    path_netflix = root_dir/"src/data/kaggle/netflix/netflix_titles.csv"

    print(f"\n[INFO] --> Cargando dataset de netflix")
    df = pd.read_csv(path_netflix, dtype=str) # esto sube como texto todas las columnas
    name_table = "netflix_titles"

    df.to_sql(
        name=name_table,
        con=engine,
        schema="raw_layer",
        if_exists="replace",
        index=False,
        chunksize=1000, # el dataset tiene pocos datos
        method="multi"
    )

    print(f"[OK] --> Tabla raw_layer.{name_table} cargada ({len(df)} filas)")

def load_usuers_generated():
    path_users = root_dir/"src/data/supabase"
    path_file = "usuarios.csv"
    full_path_file = path_users/path_file
    table_name = "users_generated"

    print(f"\n[INFO] Procesando archivo '{path_file}'...")
    df = pd.read_csv(full_path_file, dtype=str)

    # cargando usuarios 
    try:
        df.to_sql(
            name=table_name,
            con=engine,
            schema="raw_layer",
            if_exists="replace",
            index=False,
            chunksize=1000,
            method="multi"
        )
        print(f"[OK] --> Tabla {table_name} creada con exito")
    except Exception as e:
        print(f"[ERROR] --> Error >> {e}")


def psql_insert_copy(table, conn, keys, data_iter): # funcion para usar copy
    # obtener la conexion a la db
    db_api_conn = conn.connection # habla directo con el driver de postgres
    with db_api_conn.cursor() as cur: # cursor es como abrir una ventana de terminal directa a la db
        # Crea un buffer (un archivo temporal en la memoria RAM)
        s_buf = StringIO() # crea un archivo virtual que solo existe en la memoria RAM (no en el disco duro).
        #  toma los datos de Pandas y los escribe en ese archivo fantasma usando formato CSV.
        writer = csv.writer(s_buf) 
        writer.writerows(data_iter)
        s_buf.seek(0) # después de escribir, el "lector" queda al final del archivo. Esto lo regresa a la línea 1 para empezar a leerlo.

        # Extrae el nombre del schema y la tabla
        column = ', '.join(f'"{k}"' for k in keys)
        if table.schema:
            table_name = f'"{table.schema}"."{table.name}"'
        else:
            table_name = f'"{table.name}"'

        # Construye y ejecuta el comando COPY
        sql = f'COPY {table_name}({column}) FROM STDIN WITH CSV'
        # Sintaxis moderna y correcta para psycopg3
        with cur.copy(sql) as copy_op:
            copy_op.write(s_buf.getvalue())

def load_spotify_tables():
    path_spotify = root_dir/"src/data/kaggle/spotify"
    
    files_to_load = {
        "artists.csv": "spotify_artists",
        "dict_artists.json": "spotify_dict_artists",
        "tracks.csv": "spotify_tracks"
    }

    for file_name, table_name in files_to_load.items():
        full_path_files_spotify = path_spotify/file_name

        if file_name.lower().endswith(".csv"): # averigua si el archivo es csv o no
            df = pd.read_csv(full_path_files_spotify, dtype=str)
        else:
            try:
                # Intenta leerlo como una tabla normal
                df = pd.read_json(full_path_files_spotify)
            except ValueError:
                # Si es un diccionario puro, lo lee como Serie y lo aplana a Tabla
                df = pd.read_json(full_path_files_spotify, typ='series').reset_index()
                
                # Nombres descriptivos para armar las columnas
                df.columns = ['artist_id', 'related_artists']
                
                # Convertimos todo a texto para evitar problemas al subir a Postgres
                df = df.astype(str)
        
        print(f"\n[INFO] --> Procesando archivo {file_name}...")
        df.to_sql(
            name=table_name,
            con=engine,
            schema="raw_layer",
            if_exists="replace",
            index=False,
            # chunksize=10000,
            method=psql_insert_copy
        )
        print(f"[OK] --> Tabla raw_layer.{table_name} cargada ({len(df)} filas)")

def load_movielens_tables():
    path_movielens = root_dir/"src/data/ml-25m/ml-25m"

    files_to_load = {
        "genome-scores.csv": "movielens_genome_scores",
        "genome-tags.csv": "movielens_genome_tags",
        "links.csv": "movielens_links",
        "movies.csv": "movielens_movies",
        "ratings.csv": "movielens_ratings",
        "tags.csv": "movielens_tags"
    }

    for file_name, table_name in files_to_load.items():
        # construccion dinamica de rutas
        full_path_file_name = path_movielens/file_name

        try:
            df = pd.read_csv(full_path_file_name, dtype=str)
            print(f"\n[INFO] --> Procesando archivo '{file_name}'...")
            df.to_sql(
                name=table_name,
                con=engine,
                schema="raw_layer",
                if_exists="replace",
                index=False,
                chunksize=1000000,
                method=psql_insert_copy
            )
            print(f"[OK] --> Table '{table_name}' creada con exito.")
        except Exception as e:
            print(f"[ERROR] --> Error >> {e}")

    print(f"\n[OK] --> Tablas 'movielens' creadas exitosamente\n")

def load_all_files():
    load_movielens_tables()
    load_netflix_tables()
    load_spotify_tables()
    load_usuers_generated()

if __name__ == "__main__":
    # load_netflix_tables()
    load_usuers_generated()

