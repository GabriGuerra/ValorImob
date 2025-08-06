import os
import streamlit as st
import pandas as pd
from google.cloud import bigquery
import plotly.express as px

# Configurações do BigQuery
BQ_PROJECT = 'seu-projeto-gcp'
BQ_DATASET = 'valorimob'
BQ_TABLE_RANKING = 'ranking_bairros'

st.set_page_config(page_title='Valorização Imobiliária', layout='wide')

@st.cache_data(ttl=3600)
def carregar_dados_bigquery():
    client = bigquery.Client(project=BQ_PROJECT)
    query = f"""
        SELECT * 
        FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE_RANKING}`
        ORDER BY valorizacao_percentual DESC
    """
    df = client.query(query).to_dataframe()
    return df

def main():
    st.title('📈 Dashboard de Valorização Imobiliária - Litoral PR')
    st.markdown('**Fonte de dados:** IBGE, GeoData, Modelo preditivo')

    df = carregar_dados_bigquery()

    # Filtros
    bairros = df['bairro'].dropna().unique()
    bairro_selecionado = st.selectbox("Selecione o bairro", options=['Todos'] + list(bairros))

    if bairro_selecionado != 'Todos':
        df = df[df['bairro'] == bairro_selecionado]

    col1, col2, col3 = st.columns(3)
    col1.metric("📊 Bairros analisados", len(df))
    col2.metric("📈 Valorização média (%)", f"{df['valorizacao_percentual'].mean():.2f}")
    col3.metric("🏠 Imóveis processados", df['qtde_imoveis'].sum())

    # Gráfico de barras
    fig = px.bar(
        df.sort_values('valorizacao_percentual', ascending=False).head(10),
        x='bairro',
        y='valorizacao_percentual',
        color='valorizacao_percentual',
        labels={'valorizacao_percentual': 'Valorização (%)'},
        title='Top 10 Bairros com Maior Valorização'
    )
    st.plotly_chart(fig, use_container_width=True)

    # Tabela
    st.subheader('📋 Tabela de Ranking')
    st.dataframe(df, use_container_width=True)

if __name__ == '__main__':
    main()
