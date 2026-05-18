import requests
import logging

def extract_from_api(results=500):
    """
    Extrae un número variable de usuarios. 
    Permite paralelizar la carga dividiendo las llamadas.
    """
    url = f"https://randomuser.me/api/?results={results}"
    logging.info(f"Consultando API para {results} usuarios...")

    response = requests.get(url) # metodo para traer la api
    if response.status_code == 200:
        usuarios = response.json()['results'] # conviente a json el texto plano de la api
        logging.info(f"Extracción exitosa de {len(usuarios)} usuarios.")
        return usuarios
    else:
        raise Exception(f"Fallo en API: {response.status_code}")