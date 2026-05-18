# librerias
from dotenv import load_dotenv
from pathlib import Path
# carga de variables de entorno
load_dotenv()
# se debe cargar antes las variables de entorno para que kaggle funcione con las variables de entorno
import kaggle
# ruta root
root_path = Path(__file__).parents[2]
# ruta del directorio general de kaggle
path_base = root_path/"src"/"data"/"kaggle"
# Autentiticacion en Kaggle
kaggle.api.authenticate()

def extract_from_kaggle_netflix():
    """ Ruta al dataser de Netflix """
    dataset = "shivamb/netflix-shows"

    """" Construyendo ruta de descarga """
    file_destine = "netflix"
    path_full = path_base/file_destine
    # creando carpeta base "data"
    path_base.mkdir(parents=True, exist_ok=True)

    """ Probando si la ruta ya existe """
    if path_full.exists():
        print(f"\n[INFO] --> Los datos de 'Netflix' ya estan descargados en '{path_full}'")
        return

    print(f"\n[OK] >> Iniciando descarga del dataset 'Netflix'")
    
    try:
        # descargando los datos desde kaggle
        kaggle.api.dataset_download_files(dataset, path=path_full, unzip=True)
        print(f"[OK] Archivos descargados en: {path_full}")
        return
    except Exception as e:
        print(f"[ERROR] --> Error: {e}")
        return

def extract_from_kaggle_spotify():
    # usuario y dataset
    dataset = "yamaerenay/spotify-dataset-19212020-600k-tracks"
    file_destine = "spotify"
    path_destine = path_base/file_destine

    if path_destine.exists():
        print(f"\n[INFO]  --> Los datos de 'spotify' ya estan descargados en '{path_destine}'")
        return
    
    print(f"\n[INFO] --> Iniciando descarga del dataser 'spotify")

    try:
        kaggle.api.dataset_download_files(dataset, path=path_destine, unzip=True)
        print(f"[OK] --> Dataset descargado en '{path_destine}")
        return
    except Exception as e:
        print(f"[ERROR] >> Error: {e}")
        return

def extract_from_kaggle(): # funcion para llamar las otras dos al mismo tiempo
    extract_from_kaggle_netflix()
    extract_from_kaggle_spotify()

if __name__ == "__main__":
    extract_from_kaggle()