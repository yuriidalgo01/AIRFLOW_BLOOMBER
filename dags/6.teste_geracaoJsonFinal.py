from datetime import datetime, timedelta, date
from pathlib import Path
import pandas as pd
import pendulum

from airflow import DAG
# Import atualizado conforme aviso no log
try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 8, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

BASE_DATA_DIR = Path("/opt/airflow/data")
FINAL_DIR = BASE_DATA_DIR / "FINAL"
PROCESSED_DIR = BASE_DATA_DIR / "processed" / "calculo_emissoes"


def processar_csv_para_json(csv_path: str, json_output_path: str):
    csv_file = Path(csv_path)
    json_file = Path(json_output_path)

    json_file.parent.mkdir(parents=True, exist_ok=True)

    if not csv_file.exists():
        print(f"[AVISO] Arquivo CSV não encontrado: {csv_file}")
        return False

    df = pd.read_csv(csv_file)

    colunas_map = {
        'bus_id': 'codigo_onibus',
        'line_id' : 'codigo_linha',
        'distancia_km': 'distancia_percorrida',
        'Tecnologia': 'modelo',
        'emissao_co2(t)': 'emissao_co2',
        'emissao_mp(kg)': 'emissao_mp',
        'emissao_nox(kg)': 'emissao_nox'
    }
    
    df = df.rename(columns=colunas_map)

    colunas_finais = [
        'codigo_onibus', 
        'modelo',
        'codigo_linha',
        'distancia_percorrida', 
        'emissao_co2', 
        'emissao_mp', 
        'emissao_nox'
    ]
    df_saida = df[[col for col in colunas_finais if col in df.columns]]

    df_saida.to_json(
        json_file, 
        orient='records', 
        force_ascii=False,
        indent=2
    )
    
    print(f"[SUCESSO] Arquivo JSON gerado em: {json_file}")
    return True


def processar_dias_pendentes(data_inicio_str: str = "2026-08-27"):
    dt_inicio = datetime.strptime(data_inicio_str, "%Y-%m-%d").date()
    dt_fim = date.today() - timedelta(days=1)

    if dt_inicio > dt_fim:
        print(f"[AVISO] Data de início ({dt_inicio}) é posterior a ontem ({dt_fim}). Nada a processar.")
        return

    total_dias = (dt_fim - dt_inicio).days + 1
    print(f"--- Verificando emissões pendentes de {dt_inicio} até {dt_fim} ({total_dias} dia(s)) ---")

    for i in range(total_dias):
        dia_atual = dt_inicio + timedelta(days=i)
        ds_nodash = dia_atual.strftime("%Y%m%d")

        csv_path = PROCESSED_DIR / f"tabela_emissoes_{ds_nodash}.csv"
        json_output_path = FINAL_DIR / f"tabela_emissoes_{ds_nodash}.json"

        if json_output_path.exists():
            print(f"[PULANDO] Data {ds_nodash} já possui JSON processado.")
            continue

        print(f"[PROCESSANDO] Gerando JSON para a data: {ds_nodash}")
        processar_csv_para_json(str(csv_path), str(json_output_path))


with DAG(
    dag_id='dag_converte_emissoes_csv_para_json',
    default_args=default_args,
    description='Converte tabela de emissões diária CSV para arquivo JSON',
    start_date=pendulum.datetime(2026, 8, 27, tz="America/Sao_Paulo"),
    schedule='0 7 * * *',
    catchup=True,
    tags=['emissoes', 'etl', 'json', 'final']
) as dag:

    tarefa_converter_json = PythonOperator(
        task_id='task_csv_para_json',
        python_callable=processar_csv_para_json,
        op_kwargs={
            'csv_path': str(PROCESSED_DIR / "tabela_emissoes_{{ data_interval_start.strftime('%Y%m%d') }}.csv"),
            'json_output_path': str(FINAL_DIR / "tabela_emissoes_{{ data_interval_start.strftime('%Y%m%d') }}.json")
        }
    )


if __name__ == "__main__":
    processar_dias_pendentes("2026-08-27")