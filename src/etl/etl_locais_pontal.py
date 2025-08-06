import os
import logging
import requests
from ftplib import FTP
import boto3
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format='%(asctime)s — %(levelname)s — %(message)s')

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'raw', 'ibge_shapefiles')
os.makedirs(DATA_DIR, exist_ok=True)

IBGE_FTP_HOST = 'geoftp.ibge.gov.br'
MALHAS_BASE_DIR = '/organizacao_do_territorio/malhas_territoriais/malhas_municipais'

# Config Google BigQuery
BQ_PROJECT = 'seu-projeto-gcp'
BQ_DATASET = 'valorimob_ibge'

# Config AWS S3
S3_BUCKET = 'seu-bucket-s3'
S3_FOLDER = 'ibge_shapefiles'

# Código do município de Pontal - alterar conforme necessário
CODIGO_MUNICIPIO_PONTAL = '4118500'  # Exemplo: Pontal do Paraná

def listar_anos_disponiveis():
    ftp = FTP(IBGE_FTP_HOST)
    ftp.login()
    ftp.cwd(MALHAS_BASE_DIR)
    anos = ftp.nlst()
    ftp.quit()
    anos_validos = [ano for ano in anos if ano.isdigit() and 2015 <= int(ano) <= 2025]
    logging.info(f"Anos disponíveis para download: {anos_validos}")
    return sorted(anos_validos)

def baixar_shapefile_ano(ano):
    ftp = FTP(IBGE_FTP_HOST)
    ftp.login()
    pasta_ano = f'{MALHAS_BASE_DIR}/{ano}/BR'
    ftp.cwd(pasta_ano)
    arquivos = ftp.nlst()
    arquivo_zip = None
    for arq in arquivos:
        if arq.startswith('BR_Municipios') and arq.endswith('.zip'):
            arquivo_zip = arq
            break
    if not arquivo_zip:
        logging.warning(f"Arquivo shapefile não encontrado para ano {ano}")
        ftp.quit()
        return None

    caminho_local = os.path.join(DATA_DIR, arquivo_zip)
    if os.path.exists(caminho_local):
        logging.info(f"Arquivo {arquivo_zip} já existe localmente.")
        ftp.quit()
        return caminho_local

    logging.info(f"Baixando {arquivo_zip} para {caminho_local}")
    with open(caminho_local, 'wb') as f:
        ftp.retrbinary(f'RETR {arquivo_zip}', f.write)

    ftp.quit()
    return caminho_local

def upload_s3(caminho_arquivo):
    s3 = boto3.client('s3')
    nome_arquivo = os.path.basename(caminho_arquivo)
    s3_key = f"{S3_FOLDER}/{nome_arquivo}"
    logging.info(f"Fazendo upload para S3: s3://{S3_BUCKET}/{s3_key}")
    s3.upload_file(caminho_arquivo, S3_BUCKET, s3_key)

def carregar_bigquery(caminho_arquivo, ano):
    client = bigquery.Client(project=BQ_PROJECT)
    tabela_id = f"{BQ_PROJECT}.{BQ_DATASET}.municipios_ibge_{ano}_pontal"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.SHAPEFILE,  # SHP pode ser SHAPEFILE dependendo do SDK
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    logging.info(f"Carregando shapefile no BigQuery: {tabela_id}")
    with open(caminho_arquivo, "rb") as source_file:
        job = client.load_table_from_file(source_file, tabela_id, job_config=job_config)

    job.result()
    logging.info(f"Upload no BigQuery concluído para o ano {ano}")

def run_etl():
    anos = listar_anos_disponiveis()
    for ano in anos:
        caminho = baixar_shapefile_ano(ano)
        if caminho:
            upload_s3(caminho)
            carregar_bigquery(caminho, ano)

if __name__ == "__main__":
    run_etl()
