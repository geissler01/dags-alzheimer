import requests
import logging

def extract_from_api(results=500):
    """
    Extrae un número variable de usuarios. 
    Permite paralelizar la carga dividiendo las llamadas.
    """
    url = f"https://randomuser.me/api/?results={results}"
    logging.info(f"Consultando API para {results} usuarios...")

    response = requests.get(url)
    if response.status_code == 200:
        usuarios = response.json()['results']
        logging.info(f"Extracción exitosa de {len(usuarios)} usuarios.")
        return usuarios
    else:
        raise Exception(f"Fallo en API: {response.status_code}")