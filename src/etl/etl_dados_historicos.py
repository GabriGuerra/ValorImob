import requests
import pandas as pd
import logging
import os
from datetime import datetime
import boto3
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")

# Parâmetros gerais
AWS_S3_BUCKET = "seu-bucket-s3"
AWS_S3_PREFIX = "valorimob/historicos_ibge/"
BIGQUERY_PROJECT = "seu-projeto-gcp"
BIGQUERY_DATASET = "valorimob"
BIGQUERY_TABLE = "dados_historicos_ibge"

# Código IBGE dos municípios
MUNICIPIOS = {
    "Paranagua": 4117700,
    "Pontal_do_Parana": 4118203,
}

# Período de interesse
ANOS = list(range(2015, 2026))

# Tabela SIDRA e variável a consultar (exemplo: tabela 6562 — domicílios particulares permanentes)
TABELA_SIDRA = 6562
VARIAVEL_CODIGO = "7694"  # Exemplo variável dentro da tabela

def buscar_dados_sidra(municipio_code: int, ano: int) -> pd.DataFrame:
    url = (
        f"https://servicodados.ibge.gov.br/api/v1/sidra/values/{TABELA_SIDRA}/n1/all/"
        f"n2/{municipio_code}/v/all/p/{ano}/c11255/{VARIAVEL_CODIGO}/d/v{VARIAVEL_CODIGO}%202"
    )
    logging.info(f"Buscando dados SIDRA para município {municipio_code} e ano {ano}")
    r = requests.get(url)
    r.raise_for_status()
    data = r.json()

    # Conversão para DataFrame
    df = pd.DataFrame(data[1:], columns=data[0])
    return df

def rodar_etl():
    dfs = []
    for municipio, codigo in MUNICIPIOS.items():
        for ano in ANOS:
            try:
                df = buscar_dados_sidra(codigo, ano)
                df["municipio"] = municipio
                df["ano"] = ano
                dfs.append(df)
            except Exception as e:
                logging.error(f"Erro ao baixar dados para {municipio} ano {ano}: {e}")

    df_final = pd.concat(dfs, ignore_index=True)
    csv_path = "data/processed/dados_historicos_ibge.csv"
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    df_final.to_csv(csv_path, index=False)
    logging.info(f"Dados consolidados salvos em {csv_path}")

    # Enviar para S3
    enviar_s3(csv_path)

    # Enviar para BigQuery
    enviar_bigquery(csv_path)

def enviar_s3(file_path: str):
    logging.info("Enviando arquivo para AWS S3")
    s3_client = boto3.client("s3")
    key = AWS_S3_PREFIX + os.path.basename(file_path)
    try:
        s3_client.upload_file(file_path, AWS_S3_BUCKET, key)
        logging.info(f"Arquivo enviado para s3://{AWS_S3_BUCKET}/{key}")
    except Exception as e:
        logging.error(f"Erro ao enviar arquivo para S3: {e}")

def enviar_bigquery(file_path: str):
    logging.info("Enviando dados para BigQuery")
    client = bigquery.Client(project=BIGQUERY_PROJECT)

    dataset_ref = client.dataset(BIGQUERY_DATASET)
    table_ref = dataset_ref.table(BIGQUERY_TABLE)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    with open(file_path, "rb") as source_file:
        load_job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
    load_job.result()
    logging.info(f"Dados carregados na tabela {BIGQUERY_DATASET}.{BIGQUERY_TABLE}")

if __name__ == "__main__":
    rodar_etl()
