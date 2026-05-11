from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import json
import logging
import os

BUCKET_NAME = "logs-s3-alzheimer-g"

def generate_and_upload_reports(data, formats=['csv', 'json', 'txt']):
    """
    Genera reportes en múltiples formatos y los sube a S3.
    """
    hook = S3Hook(aws_conn_id='my_s3_conn') # Usará tu IAM Role por defecto
    df = pd.DataFrame(data)
    
    # Creamos una carpeta temporal local para los archivos
    tmp_dir = "/tmp/reports"
    os.makedirs(tmp_dir, exist_ok=True)
    
    prefix = f"reports/ejercicio_8/{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}"

    for fmt in formats:
        file_path = f"{tmp_dir}/report.{fmt}"
        s3_key = f"{prefix}/report.{fmt}"
        
        logging.info(f"Generando reporte {fmt}...")
        
        if fmt == 'csv':
            df.to_csv(file_path, index=False)
        elif fmt == 'json':
            df.to_json(file_path, orient='records')
        elif fmt == 'txt':
            # Un reporte TXT simple con un resumen
            with open(file_path, 'w') as f:
                f.write(f"Resumen de Ejecución Ejercicio 8\n")
                f.write(f"Total registros: {len(df)}\n")
                f.write(f"Fecha: {pd.Timestamp.now()}\n")
        
        # Subir a S3
        logging.info(f"Subiendo a S3: s3://{BUCKET_NAME}/{s3_key}")
        hook.load_file(
            filename=file_path,
            key=s3_key,
            bucket_name=BUCKET_NAME,
            replace=True
        )
        
        # Limpieza local
        os.remove(file_path)

    return f"s3://{BUCKET_NAME}/{prefix}/"
