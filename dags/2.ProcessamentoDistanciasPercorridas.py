from datetime import datetime, timedelta
from pathlib import Path
import glob
import numpy as np
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator

# --- Função Haversine vetorizada ---
def haversine_np(lat1, lon1, lat2, lon2):
    R = 6371000.0  # Raio médio da Terra em metros
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)

    a = np.sin(dphi / 2.0)**2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2.0)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return R * c

# --- Função de processamento executada pela Task ---
def processar_distancia_dia_anterior(data_alvo: str, **kwargs):
    """
    data_alvo: data no formato 'YYYYMMDD' (ex: '20260827')
    """
    # Use os caminhos internos do Linux:
    base_dir = "/opt/airflow/data/row/POSICAO"
    output_dir = "/opt/airflow/data/processed"
    
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Filtra arquivos pela data
    pattern = f"{base_dir}/posicao_{data_alvo}_*.json"
    arquivos = glob.glob(pattern)

    if not arquivos:
        print(f"Nenhum arquivo encontrado para a data {data_alvo} no caminho {pattern}.")
        return

    print(f"Processando {len(arquivos)} arquivos da data {data_alvo}...")
    
    # Leitura em lote
    df_raw = pd.concat([pd.read_json(f) for f in arquivos], ignore_index=True)

    # Desaninhamento
    records = []
    for item in df_raw["l"].dropna():
        linha = item.get("c")
        for v in item.get("vs", []):
            records.append({
                "linha": linha,
                "p": v.get("p"),
                "ta": v.get("ta"),
                "lat": v.get("py"),
                "lon": v.get("px"),
            })

    if not records:
        print(f"Nenhum registro de veículo encontrado para a data {data_alvo}.")
        return

    df_veiculos = pd.DataFrame(records)
    df_veiculos["ta"] = pd.to_datetime(df_veiculos["ta"])
    df_veiculos = df_veiculos.sort_values(by=["p", "ta"]).reset_index(drop=True)

    # Posição anterior por veículo
    df_veiculos["lat_prev"] = df_veiculos.groupby("p")["lat"].shift(1)
    df_veiculos["lon_prev"] = df_veiculos.groupby("p")["lon"].shift(1)

    # Cálculo da distância
    df_veiculos["distancia_m"] = haversine_np(
        df_veiculos["lat_prev"],
        df_veiculos["lon_prev"],
        df_veiculos["lat"],
        df_veiculos["lon"],
    ).fillna(0.0)

    # Agregação diária por veículo e linha
    df_resumo = df_veiculos.groupby(["linha", "p"])["distancia_m"].sum().reset_index()
    df_resumo["distancia_km"] = (df_resumo["distancia_m"] / 1000).round(3)
    df_resumo["data"] = pd.to_datetime(data_alvo, format="%Y%m%d").date()

    df_resumo.columns = [
        "line_id",
        "bus_id",
        "distancia_m",
        "distancia_km",
        "data",
    ]

    # Salva o Parquet na pasta processed
    caminho_saida = f"{output_dir}/distancia_veiculos_{data_alvo}.parquet"
    df_resumo.to_parquet(caminho_saida, index=False)
    print(f"Processamento concluído com sucesso! Salvo em: {caminho_saida}")


# --- Definição da DAG ---
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="processamento_diario_posicao_onibus_teste",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,              # 'None' para permitir apenas disparos manuais no teste
    catchup=False,
    tags=["processamento_distancia", "distancia_km", "distancia_m"],
    max_active_runs=1,
) as dag:

    task_processar = PythonOperator(
        task_id="calcular_distancia_dia_anterior",
        python_callable=processar_distancia_dia_anterior,
        # Para testar os dados de hoje diretamente:
        op_kwargs={"data_alvo": "20260827"},
    )

# Permite executar o script puro no terminal / VS Code para testar sem o Airflow
if __name__ == "__main__":
    processar_distancia_dia_anterior("20260827")