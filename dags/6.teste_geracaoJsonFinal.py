from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

# Definição dos argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 8, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Configurações de diretórios base
BASE_DATA_DIR = Path("/opt/airflow/data")
FINAL_DIR = BASE_DATA_DIR / "FINAL"
PROCESSED_DIR = BASE_DATA_DIR / "processed" / "calculo_emissoes"


def processar_csv_para_json(csv_path: str, json_output_path: str):
    """
    Lê o CSV da data de execução, renomeia as colunas para o padrão
    esperado no JSON final e exporta em formato lista de objetos JSON.
    """
    csv_file = Path(csv_path)
    json_file = Path(json_output_path)

    # Garante que o diretório de destino existe
    json_file.parent.mkdir(parents=True, exist_ok=True)

    if not csv_file.exists():
        raise FileNotFoundError(f"Arquivo CSV não encontrado: {csv_file}")

    # 1. Leitura do arquivo CSV do dia
    df = pd.read_csv(csv_file)

    # 2. Mapeamento de colunas
    colunas_map = {
        'bus_id': 'codigo_onibus',
        'distancia_km': 'distancia_percorrida',
        'Tecnologia': 'tecnologia',
        'emissao_co2(t)': 'emissao_co2',
        'emissao_mp': 'emissao_mp',
        'emissao_nox': 'emissao_nox'
    }
    
    df = df.rename(columns=colunas_map)

    # 3. Filtrar apenas colunas necessárias presentes no DataFrame
    colunas_finais = [
        'codigo_onibus', 
        'distancia_percorrida', 
        'tecnologia', 
        'emissao_co2', 
        'emissao_mp', 
        'emissao_nox'
    ]
    df_saida = df[[col for col in colunas_finais if col in df.columns]]

    # 4. Exportação para JSON
    df_saida.to_json(
        json_file, 
        orient='records', 
        force_ascii=False,
        indent=2
    )
    
    print(f"Arquivo JSON gerado com sucesso em: {json_file}")


# Declaração da DAG diária
with DAG(
    dag_id = 'dag_converte_emissoes_csv_para_json',
    default_args = default_args,
    description = 'Converte tabela de emissões diária CSV para arquivo JSON',
    schedule = '0 7 * * *',
    catchup = True,  # Processa retroativamente os dias desde 2026-08-27
    tags = ['emissoes', 'etl', 'json', 'final']
) as dag:

    tarefa_converter_json = PythonOperator(
        task_id='task_csv_para_json',
        python_callable=processar_csv_para_json,
        op_kwargs={
            'csv_path': str(PROCESSED_DIR / "tabela_emissoes_{{ ds_nodash }}.csv"),
            'json_output_path': str(FINAL_DIR / "tabela_emissoes_{{ ds_nodash }}.json")
        }
    )

    tarefa_converter_json