# ValorImob
>_**English**. Scroll down for the Portuguese version._
## Overview
Python data pipeline built to analyze real estate trends in Paranaguá-PR and Pontal do Paraná-PR, Brazil.
The project was built specifically to avoid web scraping, relying instead on historical and geopolitical data to generate meaningful insights.

- BigQuery for cloud data storage
- dbt for data transformation
- Streamlit for dashboard visualization

Pontal do Paraná was included due to its strategic influence on Paranaguá’s development.




## Data Sources
The data used in this project comes from public and reliable sources:
- IBGE (Brazilian Institute of Geography and Statistics) – demographic and territorial data
- GeoData – geospatial information and location mapping
>_Due to licensing restrictions and file size, raw data files are not included in the repository._


## Features
- Extraction of historical and geospatial real estate data
- Feature engineering from raw datasets
- Predictive modeling to estimate property appreciation
- Generation of ranked reports by neighborhood
- Interactive dashboard visualization via Streamlit

## Project Structure
```plaintext
valorimob/
├── run_project.py                    # Main pipeline script
├── requirements.txt                  # Python dependencies
├── .env                              # Environment variables (not included)
├── app/
│   └── dashboard_valorizacao.py      # Streamlit dashboard
├── src/
│   ├── etl/
│   │   ├── etl_dados_historicos.py   # Historical data extraction
│   │   ├── etl_geospatial.py         # Geospatial data extraction
│   │   ├── etl_locais_paranagua.py   # Paranaguá location data
│   │   ├── etl_locais_pontal.py      # Pontal do Paraná location data
│   ├── features/
│   │   └── gerar_variaveis.py        # Feature engineering
│   ├── models/
│   │   └── modelo_valorizacao.py     # Predictive modeling
│   ├── reports/
│   │   └── gerar_ranking.py          # Ranking report generation
```
## Requirements
Install dependencies:
```plaintext
pip install -r requirements.txt
```

Run the full pipeline:
```plaintext
python run_project.py
```

Launch the dashboard locally:
```plaintext
streamlit run app/dashboard_valorizacao.py
```

_Environment variables (e.g., Google BigQuery credentials) must be configured in a .env file_



# ValorImob

## Visão Geral

Pipeline de dados desenvolvido em Python para analisar a dinâmica do mercado imobiliário em Paranaguá-PR.
O projeto foi criado com foco em qualidade de dados e insights de longo prazo com o objetivo de não utilizar scraping. Em vez disso, utiliza dados históricos e geopolíticos para gerar insights sobre valorização urbana e tendências reais.
A arquitetura inclui:
- Google BigQuery para armazenamento e consulta escalável na nuvem
- dbt para transformação de dados modular e versionada
- Streamlit para visualização interativa em dashboard

Pontal do Paraná foi incluída pela sua influência estratégica no desenvolvimento de Paranaguá.



## Fontes de Dados

Os dados utilizados neste projeto foram obtidos de fontes públicas e confiáveis:

- **IBGE (Instituto Brasileiro de Geografia e Estatística)** – dados demográficos e territoriais
- **GeoData** – informações geoespaciais e mapeamento de localização

>_Devido a restrições de licenciamento e ao tamanho dos arquivos, os dados brutos **não foram incluídos** no repositório._

## Funcionalidades

- Extração de dados históricos e geográficos do mercado imobiliário
- Engenharia de variáveis a partir dos dados brutos
- Treinamento de modelo preditivo para estimar valorização de imóveis
- Geração de relatórios ranqueados por bairro
- Visualização interativa via dashboard

```plaintext
valorimob/
├── run_project.py                    # Script principal do pipeline
├── requirements.txt                  # Dependências Python
├── .env                              # Variáveis de ambiente
├── app/
│   └── dashboard_valorizacao.py      # Streamlit dashboard
├── src/
│   ├── etl/
│   │   ├── etl_dados_historicos.py   # Extração de dados históricos
│   │   ├── etl_geospatial.py         # Extração de dados geográficos
│   │   ├── etl_locais_paranagua.py   # Dados de locais em Paranaguá
│   │   ├── etl_locais_pontal.py      # Dados de locais em Pontal
│   ├── features/
│   │   └── gerar_variaveis.py        # Engenharia de variáveis
│   ├── models/
│   │   └── modelo_valorizacao.py     # Modelagem preditiva
│   ├── reports/
│   │   └── gerar_ranking.py          # Geração de relatórios
│  
```
## Requisitos

Instalar as dependências:
```plaintext
pip install -r requirements.txt
```

Executar o pipeline completo:
```plaintext
python run_project.py
```

Executar o dashboard localmente:
```plaintext
streamlit run app/dashboard_valorizacao.py
```

_As variáveis de ambiente (como credenciais do Google BigQuery) devem estar configuradas em um arquivo .env._











