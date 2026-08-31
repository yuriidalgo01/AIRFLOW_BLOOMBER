from datetime import datetime, timedelta, date
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

    json_file.parent.mkdir(parents=True, exist_ok=True)

    if not csv_file.exists():
        print(f"[AVISO] Arquivo CSV não encontrado: {csv_file}")
        return False

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
    
    print(f"[SUCESSO] Arquivo JSON gerado em: {json_file}")
    return True


# --- Função de varredura de dias pendentes ---
def processar_dias_pendentes(data_inicio_str: str = "2026-08-27"):
    """
    Percorre o intervalo de datas da data_inicio_str até hoje.
    Verifica se o JSON já existe; se não existir, processa a conversão.
    """
    dt_inicio = datetime.strptime(data_inicio_str, "%Y-%m-%d").date()
    dt_fim = date.today()

    total_dias = (dt_fim - dt_inicio).days + 1
    print(f"--- Verificando emissões pendentes de {dt_inicio} até {dt_fim} ({total_dias} dia(s)) ---")

    for i in range(total_dias):
        dia_atual = dt_inicio + timedelta(days=i)
        ds_nodash = dia_atual.strftime("%Y%m%d")

        csv_path = PROCESSED_DIR / f"tabela_emissoes_{ds_nodash}.csv"
        json_output_path = FINAL_DIR / f"tabela_emissoes_{ds_nodash}.json"

        # Verifica se o arquivo final já existe
        if json_output_path.exists():
            print(f"[PULANDO] Data {ds_nodash} já possui JSON processado.")
            continue

        print(f"[PROCESSANDO] Gerando JSON para a data: {ds_nodash}")
        processar_csv_para_json(str(csv_path), str(json_output_path))


# Declaração da DAG diária
with DAG(
    dag_id='dag_converte_emissoes_csv_para_json',
    default_args=default_args,
    description='Converte tabela de emissões diária CSV para arquivo JSON',
    schedule='0 7 * * *',
    catchup=True,
    tags=['emissoes', 'etl', 'json', 'final']
) as dag:

    tarefa_converter_json = PythonOperator(
        task_id='task_csv_para_json',
        python_callable=processar_csv_para_json,
        op_kwargs={
            'csv_path': str(PROCESSED_DIR / "tabela_emissoes_{{ ds_nodash }}.csv"),
            'json_output_path': str(FINAL_DIR / "tabela_emissoes_{{ ds_nodash }}.json")
        }
    )


# Execução manual pelo terminal (CMD / Docker)
if __name__ == "__main__":
    processar_dias_pendentes("2026-08-27")