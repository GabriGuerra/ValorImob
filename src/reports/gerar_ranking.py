import os
import logging
import pandas as pd
import boto3
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format='%(asctime)s — %(levelname)s — %(message)s')

PROCESSED_DIR = os.path.join('data', 'processed')
AWS_S3_BUCKET = 'seu-bucket-aqui'
AWS_S3_PREFIX = 'valorimob/rankings/'
BQ_PROJECT = 'seu-projeto-gcp'
BQ_DATASET = 'valorimob'
BQ_TABLE = 'ranking_bairros'

def carregar_predicoes():
    path = os.path.join(PROCESSED_DIR, 'predicoes_valorizacao.csv')
    df = pd.read_csv(path)
    logging.info(f'Previsões carregadas: {df.shape[0]} registros')
    return df

def gerar_ranking(df):
    if 'bairro' not in df.columns:
        logging.warning('Coluna "bairro" não encontrada. Ranking será inválido.')
        df['bairro'] = 'Desconhecido'

    df['valorizacao_absoluta'] = df['preco_previsto'] - df['preco_real']
    df['valorizacao_percentual'] = ((df['preco_previsto'] / df['preco_real']) - 1) * 100

    ranking = df.groupby('bairro').agg({
        'valorizacao_absoluta': 'mean',
        'valorizacao_percentual': 'mean',
        'preco_real': 'count'
    }).rename(columns={'preco_real': 'qtde_imoveis'}).reset_index()

    ranking = ranking.sort_values(by='valorizacao_percentual', ascending=False)
    logging.info(f'🏆 Ranking gerado: {ranking.shape[0]} bairros')
    return ranking

def salvar_ranking(ranking):
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    path = os.path.join(PROCESSED_DIR, 'ranking_bairros.csv')
    ranking.to_csv(path, index=False)
    logging.info(f'Ranking salvo em: {path}')
    return path

def upload_s3(path, bucket=AWS_S3_BUCKET, prefix=AWS_S3_PREFIX):
    s3 = boto3.client('s3')
    key = prefix + os.path.basename(path)
    s3.upload_file(path, bucket, key)
    logging.info(f'Upload do ranking para S3: s3://{bucket}/{key}')

def upload_bigquery(ranking, project=BQ_PROJECT, dataset=BQ_DATASET, table=BQ_TABLE):
    client = bigquery.Client(project=project)
    table_ref = client.dataset(dataset).table(table)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE'
    )

    tmp_csv = os.path.join(PROCESSED_DIR, 'ranking_bairros.csv')
    with open(tmp_csv, "rb") as f:
        job = client.load_table_from_file(f, table_ref, job_config=job_config)
    job.result()

    logging.info(f'📡 Ranking enviado ao BigQuery: {project}.{dataset}.{table}')

def run():
    logging.info('Gerando ranking de valorização dos bairros')
    df_pred = carregar_predicoes()
    ranking = gerar_ranking(df_pred)
    ranking_path = salvar_ranking(ranking)
    upload_s3(ranking_path)
    upload_bigquery(ranking)

    logging.info('Ranking de valorização finalizado e publicado')

if __name__ == '__main__':
    run()
