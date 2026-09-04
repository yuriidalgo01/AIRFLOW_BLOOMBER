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


def processar_csv_para_json_e_csv(csv_path: str, json_output_path: str, csv_output_path: str = None):
    csv_file = Path(csv_path)
    json_file = Path(json_output_path)
    
    # Se o caminho CSV final não for passado explicitamente, deduz trocando a extensão do JSON
    if csv_output_path is None:
        csv_out_file = json_file.with_suffix(".csv")
    else:
        csv_out_file = Path(csv_output_path)

    json_file.parent.mkdir(parents=True, exist_ok=True)
    csv_out_file.parent.mkdir(parents=True, exist_ok=True)

    if not csv_file.exists():
        print(f"[AVISO] Arquivo CSV não encontrado: {csv_file}")
        return False

    df = pd.read_csv(csv_file)

    colunas_map = {
        'bus_id'                 : 'codigo_onibus',
        'distancia_km'           : 'distancia_percorrida',
        'segundos_deslocamento'  : 'segundos_deslocamento',
        'Tecnologia'             : 'tecnologia',
        'com_ar_l_km'            : 'fator_consumo_l',
        'com_ar_kg_km'           : 'fator_consumo_kg',
        'consumo_l'              : 'consumo_l',
        'consumo_kg'             : 'consumo_kg',
        'emissao_co2(t)'         : 'emissao_co2',
        'emissao_mp(kg)'         : 'emissao_mp',
        'emissao_nox(kg)'        : 'emissao_nox',
        'line_id'                : 'random_linhas',
        'geometry'               : 'geometry',
        'random_pop_afetada_mp'  : 'random_pop_afetada_mp',
        'random_pop_afetada_nox' : 'random_pop_afetada_nox'
    }
    
    df = df.rename(columns=colunas_map)

    colunas_finais = [
        'codigo_onibus',
        'distancia_percorrida',
        'segundos_deslocamento',
        'tecnologia',
        'fator_consumo_l',
        'fator_consumo_kg',
        'consumo_l',
        'consumo_kg',
        'emissao_co2',
        'emissao_mp',
        'emissao_nox',
        'random_linhas',
        'geometry',
        'random_pop_afetada_mp',
        'random_pop_afetada_nox'
    ]

    # Preenche colunas ausentes com 0
    colunas_faltantes = [col for col in colunas_finais if col not in df.columns]
    if colunas_faltantes:
        df[colunas_faltantes] = 0

    # Garante ordenação padronizada das colunas
    df_saida = df[colunas_finais]

    # 1. Salva em formato JSON
    df_saida.to_json(
        json_file, 
        orient='records', 
        force_ascii=False,
        indent=2
    )
    print(f"[SUCESSO] Arquivo JSON gerado em: {json_file}")

    # 2. Salva em formato CSV
    df_saida.to_csv(
        csv_out_file,
        index=False,
        encoding='utf-8'
    )
    print(f"[SUCESSO] Arquivo CSV gerado em: {csv_out_file}")

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
        csv_output_path = FINAL_DIR / f"tabela_emissoes_{ds_nodash}.csv"

        if json_output_path.exists() and csv_output_path.exists():
            print(f"[PULANDO] Data {ds_nodash} já possui JSON e CSV processados.")
            continue

        print(f"[PROCESSANDO] Gerando JSON e CSV para a data: {ds_nodash}")
        processar_csv_para_json_e_csv(str(csv_path), str(json_output_path), str(csv_output_path))


with DAG(
    dag_id='dag_converte_emissoes_csv_para_json_e_csv',
    default_args=default_args,
    description='Converte tabela de emissões diária CSV para arquivos JSON e CSV finais',
    start_date=pendulum.datetime(2026, 8, 27, tz="America/Sao_Paulo"),
    schedule='0 7 * * *',
    catchup=True,
    tags=['emissoes', 'etl', 'json', 'csv', 'final']
) as dag:

    tarefa_converter_arquivos = PythonOperator(
        task_id='task_csv_para_json_e_csv',
        python_callable=processar_csv_para_json_e_csv,
        op_kwargs={
            'csv_path': str(PROCESSED_DIR / "tabela_emissoes_{{ data_interval_start.strftime('%Y%m%d') }}.csv"),
            'json_output_path': str(FINAL_DIR / "tabela_emissoes_{{ data_interval_start.strftime('%Y%m%d') }}.json"),
            'csv_output_path': str(FINAL_DIR / "tabela_emissoes_{{ data_interval_start.strftime('%Y%m%d') }}.csv")
        }
    )


if __name__ == "__main__":
    processar_dias_pendentes("2026-08-27")