import os
import logging
import requests
import geopandas as gpd
from io import BytesIO
from zipfile import ZipFile
from google.cloud import bigquery
import boto3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configurações AWS e BigQuery - ajuste conforme seu ambiente
AWS_S3_BUCKET = 'meu-bucket'
BIGQUERY_PROJECT = 'meu-projeto'
BIGQUERY_DATASET = 'regularizacao_fundiaria'
BIGQUERY_TABLE = 'paranagua_pontal'

# URLs base ONR (exemplo, ajustar conforme URL oficial do ONR para shapefiles)
ONR_BASE_URL = 'https://mapa.onr.org.br/dados/regularizacao/'

# Nomes dos arquivos para os municípios Paranaguá e Pontal do Paraná
SHAPEFILES = {
    'paranagua': 'paranagua_regularizacao.zip',
    'pontal': 'pontal_regularizacao.zip',
}


def baixar_shapefile_onr(municipio):
    """Baixa e extrai shapefile do ONR para o município."""
    url = ONR_BASE_URL + SHAPEFILES.get(municipio)
    logger.info(f"Baixando dados ONR para {municipio} de {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()
        with ZipFile(BytesIO(response.content)) as zip_ref:
            zip_ref.extractall(f'data/raw/{municipio}')
        logger.info(f"Dados extraídos para data/raw/{municipio}")
        return f'data/raw/{municipio}'
    except Exception as e:
        logger.error(f"Erro ao baixar shapefile para {municipio}: {e}")
        return None


def processar_shapefile(municipio):
    """Lê shapefile, faz limpeza e retorna GeoDataFrame."""
    path = baixar_shapefile_onr(municipio)
    if not path:
        logger.error(f"Falha ao obter dados para {municipio}")
        return None
    # Encontrar arquivo .shp dentro da pasta extraída
    shp_files = [f for f in os.listdir(path) if f.endswith('.shp')]
    if not shp_files:
        logger.error(f"Nenhum arquivo .shp encontrado em {path}")
        return None
    shp_path = os.path.join(path, shp_files[0])
    logger.info(f"Lendo shapefile {shp_path}")
    gdf = gpd.read_file(shp_path)

    # Limpeza básica - manter colunas relevantes
    cols_relevantes = ['processo', 'situacao', 'area_ha', 'geometry']
    cols_existentes = [col for col in cols_relevantes if col in gdf.columns]
    gdf = gdf[cols_existentes]

    # Filtrar somente dados relevantes, por exemplo: situações ativas ou concluídas
    gdf = gdf[gdf['situacao'].isin(['Concluído', 'Ativo'])]

    # Salvar CSV para análise posterior
    output_csv = f'data/processed/regularizacao_{municipio}.csv'
    gdf.to_csv(output_csv, index=False)
    logger.info(f"Arquivo processado salvo em {output_csv}")
    return gdf


def upload_aws_s3(file_path, s3_key):
    """Faz upload do arquivo para AWS S3."""
    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_path, AWS_S3_BUCKET, s3_key)
        logger.info(f"Upload para S3 concluído: s3://{AWS_S3_BUCKET}/{s3_key}")
    except Exception as e:
        logger.error(f"Erro no upload para S3: {e}")


def upload_bigquery(file_path, table_id):
    """Faz upload do CSV para BigQuery."""
    client = bigquery.Client(project=BIGQUERY_PROJECT)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    try:
        with open(file_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_id, job_config=job_config)
        job.result()  # Espera terminar o upload
        logger.info(f"Upload para BigQuery concluído: {table_id}")
    except Exception as e:
        logger.error(f"Erro no upload para BigQuery: {e}")


def run_etl():
    logger.info("Iniciando ETL de regularização fundiária ONR para Paranaguá e Pontal")
    for municipio in ['paranagua', 'pontal']:
        gdf = processar_shapefile(municipio)
        if gdf is not None:
            csv_path = f'data/processed/regularizacao_{municipio}.csv'
            s3_key = f'regularizacao/{municipio}/regularizacao.csv'
            table_id = f'{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{municipio}'
            upload_aws_s3(csv_path, s3_key)
            upload_bigquery(csv_path, table_id)
    logger.info("ETL completo.")


if __name__ == "__main__":
    run_etl()
