
import os
import requests
import pandas as pd
from pandas_gbq import to_gbq
import logging

# Configuração de logging para melhor visibilidade no Cloud Run/Cloud Build
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def run_pipeline():
    """Função principal que orquestra a extração e o carregamento dos dados."""
    
    # --- Pega as configurações do ambiente ---
    # É uma boa prática pegar configurações de variáveis de ambiente
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_id = os.getenv("BQ_DATASET_ID")
    table_id = os.getenv("BQ_TABLE_ID")

    if not all([project_id, dataset_id, table_id]):
        logging.error("Variáveis de ambiente (GCP_PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID) não foram configuradas.")
        return

    # --- ETAPA DE EXTRAÇÃO (Extract) ---
    logging.info("Iniciando extração de dados da API REST Countries...")
    try:
        url = "https://restcountries.com/v3.1/all"
        response = requests.get(url)
        response.raise_for_status()  # Garante que a requisição foi um sucesso
        dados = response.json()
        logging.info("Dados extraídos com sucesso.")
        
        # Normaliza o JSON para um formato de tabela (achata a estrutura)
        df = pd.json_normalize(dados, sep='_')
        logging.info(f"Dados normalizados. DataFrame criado com {df.shape[0]} linhas.")
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao extrair dados da API: {e}")
        return

    # --- ETAPA DE TRANSFORMAÇÃO BÁSICA (Transform) ---
    # Limpa nomes de colunas para serem compatíveis com o BigQuery
    df.columns = df.columns.str.replace('[^0-9a-zA-Z_]', '_', regex=True).str.lower()
    
    # Converte tipos de dados que podem ser problemáticos (listas/objetos para string)
    for col in df.columns:
        if df[col].apply(type).isin([list, dict]).any():
            df[col] = df[col].astype(str)
            
    logging.info("Nomes de colunas limpos e tipos de dados ajustados.")

    # --- ETAPA DE CARREGAMENTO (Load) ---
    tabela_destino = f"{dataset_id}.{table_id}"
    logging.info(f"Iniciando carregamento para a tabela do BigQuery: {tabela_destino}")
    try:
        to_gbq(
            df,
            destination_table=tabela_destino,
            project_id=project_id,
            if_exists='replace'  # Substitui a tabela a cada execução
        )
        logging.info("Dados carregados com sucesso no BigQuery!")
    except Exception as e:
        logging.error(f"Falha ao carregar dados no BigQuery: {e}")


if __name__ == "__main__":
    run_pipeline()