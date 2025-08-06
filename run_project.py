import logging
from src.etl import etl_dados_historicos, etl_locais_paranagua, etl_locais_pontal
from src.features import gerar_variaveis
from src.models import modelo_valorizacao
from src.reports import gerar_ranking

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)

def main():
    logging.info("Iniciando pipeline completo do ValorImob")

    # 1. ETL dos dados de imóveis
    etl_locais_paranagua.run_etl()
    etl_locais_pontal.run_etl()

    # 2. ETL histórico
    etl_dados_historicos.run_etl()

    # 3. Feature engineering
    gerar_variaveis.gerar()

    # 4. Treinamento e predição de modelo
    modelo_valorizacao.treinar_modelo()

    # 5. Gerar ranking final
    gerar_ranking.gerar()

    logging.info("Pipeline completo executado com sucesso.")

if __name__ == "__main__":
    main()
