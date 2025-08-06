import os
import logging
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score
from joblib import dump
import boto3
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format='%(asctime)s — %(levelname)s — %(message)s')

PROCESSED_DIR = os.path.join('data', 'processed')
MODEL_DIR = os.path.join('models')
AWS_S3_BUCKET = 'seu-bucket-aqui'
AWS_S3_PREFIX = 'valorimob/modelos/'
BQ_PROJECT = 'seu-projeto-gcp'
BQ_DATASET = 'valorimob'
BQ_TABLE = 'predicoes_valorizacao'

def carregar_dados():
    path = os.path.join(PROCESSED_DIR, 'variaveis_paranagua.csv')
    df = pd.read_csv(path)
    logging.info(f'Dados carregados: {df.shape[0]} registros')
    return df

def treinar_modelo(df):
    df = df.dropna(subset=['preco', 'area_m2', 'preco_por_m2'])
    X = df[['area_m2', 'preco_por_m2', 'eh_novo']].fillna(0)
    y = df['preco']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    modelo = RandomForestRegressor(n_estimators=100, random_state=42)
    modelo.fit(X_train, y_train)

    y_pred = modelo.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    logging.info(f'Métricas — MSE: {mse:.2f} | R²: {r2:.2f}')

    return modelo, X_test, y_test, y_pred

def salvar_modelo(modelo):
    os.makedirs(MODEL_DIR, exist_ok=True)
    model_path = os.path.join(MODEL_DIR, 'modelo_valorizacao.joblib')
    dump(modelo, model_path)
    logging.info(f'Modelo salvo: {model_path}')
    return model_path

def upload_s3(path, bucket=AWS_S3_BUCKET, prefix=AWS_S3_PREFIX):
    s3 = boto3.client('s3')
    key = prefix + os.path.basename(path)
    s3.upload_file(path, bucket, key)
    logging.info(f'Upload para S3: s3://{bucket}/{key}')

def upload_bigquery(df_pred, project=BQ_PROJECT, dataset=BQ_DATASET, table=BQ_TABLE):
    client = bigquery.Client(project=project)
    table_ref = client.dataset(dataset).table(table)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE'
    )

    tmp_csv = os.path.join(PROCESSED_DIR, 'predicoes_valorizacao.csv')
    df_pred.to_csv(tmp_csv, index=False)

    with open(tmp_csv, "rb") as f:
        job = client.load_table_from_file(f, table_ref, job_config=job_config)
    job.result()

    logging.info(f'Previsões enviadas para BigQuery: {project}.{dataset}.{table}')

def run():
    logging.info('Treinamento do modelo de valorização iniciado')
    df = carregar_dados()
    modelo, X_test, y_test, y_pred = treinar_modelo(df)
    model_path = salvar_modelo(modelo)
    upload_s3(model_path)

    df_pred = X_test.copy()
    df_pred['preco_real'] = y_test.values
    df_pred['preco_previsto'] = y_pred
    upload_bigquery(df_pred)

    logging.info('Modelo treinado e resultados enviados com sucesso')

if __name__ == '__main__':
    run()
