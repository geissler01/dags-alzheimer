import pandas as pd
import logging

def transform_analytics_data(users_batches, population_list):
    """
    Realiza un join completo entre usuarios de la API y datos de población,
    devolviendo un dataset enriquecido con todos los campos.
    """
    logging.info("Iniciando transformación profunda y cruce de datos...")
    
    # 1. Unimos lotes de usuarios
    all_users = users_batches[0] + users_batches[1]
    df_users = pd.json_normalize(all_users) 
    
    # 2. Selección y limpieza de columnas de usuarios
    cols_to_keep = {
        'name.first': 'first_name',
        'name.last': 'last_name',
        'gender': 'gender',
        'email': 'email',
        'location.city': 'city',
        'location.country': 'country',
        'dob.age': 'age'
    } # cambiamos los nombres originales por unos mas claros
    
    # Solo tomamos las columnas que existen
    df_users_clean = df_users[list(cols_to_keep.keys())].rename(columns=cols_to_keep) # Aplica el cambio de nombres
    
    # 3. Cargamos población
    df_pop = pd.DataFrame(population_list) # population_list ya es un csv porque asi entra en esta funcion
    
    # 4. Join por país
    df_merged = pd.merge(
        df_users_clean, 
        df_pop, 
        left_on='country', 
        right_on='Country Name', 
        how='inner'
    )
    
    # 5. Formateo final
    df_final = df_merged[[
        'first_name', 'last_name', 'gender', 'email', 
        'city', 'country', 'age', 'Value', 'Year'
    ]].rename(columns={'Value': 'population_count', 'Year': 'population_year'})
    
    logging.info(f"Cruce completado exitosamente. Filas: {len(df_final)}")
    return df_final.to_dict(orient='records')
