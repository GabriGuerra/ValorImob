import os
import logging
import pandas as pd
from google.cloud import bigquery
import boto3

logging.basicConfig(level=logging.INFO, format='%(asctime)s — %(levelname)s — %(message)s')

PROCESSED_DIR = os.path.join('data', 'processed')
AWS_S3_BUCKET = 'seu-bucket-aqui'
AWS_S3_PREFIX = 'valorimob/variaveis/'

BQ_PROJECT = 'seu-projeto-gcp'
BQ_DATASET = 'valorimob'
BQ_TABLE = 'variaveis_paranagua'

def carregar_dados():
    df_pontal = pd.read_csv(os.path.join(PROCESSED_DIR, 'imoveis_pontal.csv'))
    df_paranagua = pd.read_csv(os.path.join(PROCESSED_DIR, 'imoveis_paranagua.csv'))
    df_hist = pd.read_csv(os.path.join(PROCESSED_DIR, 'dados_historicos.csv'))

    logging.info('Dados carregados com sucesso')

    df = pd.concat([df_pontal, df_paranagua], ignore_index=True)


    df['preco_por_m2'] = df['preco'] / df['area_m2']
    df['eh_novo'] = df['ano_construcao'].apply(lambda x: 1 if x >= 2015 else 0 if pd.notnull(x) else None)
    df['bairro'] = df['bairro'].str.strip().str.lower()

    logging.info(f'Variáveis geradas — {df.shape[0]} registros')

    return df

def salvar_csv(df, nome_arquivo):
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    path = os.path.join(PROCESSED_DIR, nome_arquivo)
    df.to_csv(path, index=False)
    logging.info(f'Arquivo salvo: {path}')
    return path

def upload_s3(file_path, bucket=AWS_S3_BUCKET, prefix=AWS_S3_PREFIX):
    s3 = boto3.client('s3')
    key = prefix + os.path.basename(file_path)
    s3.upload_file(file_path, bucket, key)
    logging.info(f'Upload para S3: s3://{bucket}/{key}')

def upload_bigquery(csv_path, project=BQ_PROJECT, dataset=BQ_DATASET, table=BQ_TABLE):
    client = bigquery.Client(project=project)
    dataset_ref = client.dataset(dataset)
    table_ref = dataset_ref.table(table)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition='WRITE_TRUNCATE'
    )

    with open(csv_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
    job.result()
    logging.info(f'Upload para BigQuery concluído: {project}.{dataset}.{table}')

def run():
    logging.info('Iniciando geração de variáveis')
    df = carregar_dados()
    path = salvar_csv(df, 'variaveis_paranagua.csv')
    upload_s3(path)
    upload_bigquery(path)
    logging.info('Geração de variáveis finalizada')

if __name__ == '__main__':
    run()
