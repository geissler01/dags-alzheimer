import pandas as pd
import logging

def extract_kaggle_data():
    """
    Extrae datos de población mundial desde un CSV público (Dataset de Kaggle).
    Filtra por el año más reciente disponible.
    """
    url = "https://raw.githubusercontent.com/datasets/population/master/data/population.csv"
    logging.info(f"Descargando dataset de población: {url}")

    try:
        # Leemos el CSV directamente desde la URL
        df = pd.read_csv(url)

        # Filtramos por el año más reciente disponible para el análisis

        logging.info(f"Datos de población cargados correctamente.")
        
        # Convertimos a lista de diccionarios para que Airflow pueda serializarlo
        return df.to_dict(orient='records')
    
    except Exception as e:
        logging.error(f"Error al procesar el CSV de Kaggle: {e}")
        raise
