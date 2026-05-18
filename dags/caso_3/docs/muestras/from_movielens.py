import requests
import zipfile
import os
from pathlib import Path

# Configuración de rutas
# Path(__file__) es la ubicación de este script (src/ingestion/extract_csv.py)
# .parents[2] nos sube tres niveles hasta la raíz del proyecto
path_root = Path(__file__).parents[2]

def extract_csv_from_movielens():
    """
    Descarga el dataset MovieLens 25M, lo extrae en src/data y limpia el archivo temporal.
    Implementa streaming para no saturar la memoria RAM.
    """
    url = "https://files.grouplens.org/datasets/movielens/ml-25m.zip"
    
    # Definimos la carpeta 'data' como destino principal
    data_dir = path_root / 'src' / 'data'
    zip_path = data_dir / "ml-25m.zip"
    extracted_folder = data_dir / "ml-25m"

    # 1. Crear 'data' si no existe
    data_dir.mkdir(parents=True, exist_ok=True)

    # 2. Verificar si ya existe la carpeta descomprimida para no repetir el proceso
    if extracted_folder.exists():
        print(f"\n[INFO] --> Los datos de 'movielens' ya existen en: {extracted_folder}")
        return

    print(f"\n[INFO] --> Iniciando proceso de descarga para MovieLens 25M...")
    
    try:
        # 3. Descarga controlada mediante Streaming
        # stream=True mantiene la conexión abierta sin descargar todo a la RAM de golpe
        with requests.get(url, stream=True) as response:
            response.raise_for_status() # Lanza error si la descarga falla (ej. 404)
            
            # El archivo se crea físicamente en el disco en esta línea
            with open(zip_path, "wb") as f:
                # Bajamos el archivo en pedazos (chunks) de 8KB
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        
        print(f"[SUCCESS] --> Archivo descargado exitosamente en: {zip_path}")

        # 4. Descompresión del archivo ZIP
        print(f"[INFO] --> Descomprimiendo archivos en: {data_dir}...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Extrae todo directamente en la carpeta data/
            # El ZIP contiene internamente una carpeta llamada 'ml-25m'
            zip_ref.extractall(extracted_folder)
        
        print(f"[SUCCESS] --> Archivo descomprimido en: {extracted_folder}")

        # 5. Limpieza de archivos temporales
        if zip_path.exists():
            os.remove(zip_path)
            print(f"[INFO] --> Archivo ZIP temporal eliminado para ahorrar espacio.")

    except Exception as e:
        print(f"[ERROR] --> Ocurrió un problema durante el proceso: {e}")
        # Si falló la descarga, intentamos borrar el archivo ZIP incompleto/corrupto
        if zip_path.exists():
            os.remove(zip_path)
            print(f"[CLEANUP] --> Se eliminó el archivo ZIP incompleto.")

if __name__ == "__main__":
    extract_csv_from_movielens()
