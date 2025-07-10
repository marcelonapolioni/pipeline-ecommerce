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

def fetch_and_load(url: str, table_name: str, project_id: str, dataset_id: str):
    """
    Função reutilizável para buscar dados de uma URL e carregar em uma tabela do BigQuery.
    
    Args:
        url (str): A URL da API para buscar os dados.
        table_name (str): O nome da tabela de destino no BigQuery.
        project_id (str): O ID do projeto no Google Cloud.
        dataset_id (str): O ID do conjunto de dados no BigQuery.
    """
    try:
        logging.info(f"Iniciando extração de dados da API: {url}")
        response = requests.get(url)
        response.raise_for_status()  # Garante que a requisição foi um sucesso
        data = response.json()
        logging.info(f"Dados de '{table_name}' extraídos com sucesso.")
        
        if not data:
            logging.warning(f"Nenhum dado retornado da API para '{table_name}'. Pulando o carregamento.")
            return

        df = pd.DataFrame(data)
        
        # --- ETAPA DE TRANSFORMAÇÃO BÁSICA (Pré-carregamento) ---
        # Limpa nomes de colunas para serem compatíveis com o BigQuery
        df.columns = df.columns.str.replace('[^0-9a-zA-Z_]', '_', regex=True).str.lower()
        
        # Converte colunas complexas (listas/dicionários) para string para carregar no BQ
        # A transformação real será feita com SQL no BigQuery
        for col in df.columns:
            if df[col].apply(type).isin([list, dict]).any():
                df[col] = df[col].astype(str)
                
        logging.info(f"Dados de '{table_name}' normalizados. DataFrame criado com {df.shape[0]} linhas.")

        # --- ETAPA DE CARREGAMENTO (Load) ---
        destination_table = f"{dataset_id}.{table_name}"
        logging.info(f"Iniciando carregamento para a tabela do BigQuery: {destination_table}")
        
        to_gbq(
            df,
            destination_table=destination_table,
            project_id=project_id,
            if_exists='replace'  # Substitui a tabela a cada execução
        )
        logging.info(f"Dados de '{table_name}' carregados com sucesso no BigQuery!")

    except requests.exceptions.RequestException as e:
        logging.error(f"Erro de rede ao extrair dados para '{table_name}': {e}")
    except Exception as e:
        logging.error(f"Falha no processamento de '{table_name}': {e}")


def run_pipeline():
    """Função principal que orquestra a extração e o carregamento dos dados de e-commerce."""
    
    # --- Pega as configurações do ambiente ---
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_id = os.getenv("BQ_DATASET_ID")

    if not all([project_id, dataset_id]):
        logging.error("Variáveis de ambiente (GCP_PROJECT_ID, BQ_DATASET_ID) não foram configuradas.")
        return

    logging.info("--- Iniciando pipeline de dados da Fake Store API ---")
    
    # Extrai e carrega os dados de produtos
    fetch_and_load(
        url="https://fakestoreapi.com/products",
        table_name="raw_products",
        project_id=project_id,
        dataset_id=dataset_id
    )
    
    # Extrai e carrega os dados de carrinhos (vendas)
    fetch_and_load(
        url="https://fakestoreapi.com/carts",
        table_name="raw_carts",
        project_id=project_id,
        dataset_id=dataset_id
    )
    
    logging.info("--- Pipeline de dados da Fake Store API finalizado. ---")


if __name__ == "__main__":
    run_pipeline()