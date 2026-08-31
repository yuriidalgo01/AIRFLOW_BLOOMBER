from datetime import datetime, timedelta, date
from pathlib import Path
import glob
import numpy as np
import pandas as pd
import json

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
    data_formatada = datetime.strptime(data_alvo, "%Y%m%d").strftime("%Y-%m-%d")
    print(f"Iniciando: {data_formatada}")

    base_dir = Path(f"/opt/airflow/data/raw/POSICAO/data={data_formatada}")
    output_dir = Path(f"/opt/airflow/data/processed/data={data_formatada}")

    dir_parquet = output_dir / "files_parquet"
    dir_csv = output_dir / "files_csv"

    dir_parquet.mkdir(parents=True, exist_ok=True)
    dir_csv.mkdir(parents=True, exist_ok=True)

    pattern = str(base_dir / f"posicao_{data_alvo}_*.json")
    arquivos = glob.glob(pattern)

    if not arquivos:
        print(f"Nenhum arquivo encontrado para a data {data_alvo} no caminho: {pattern}")
        return

    print(f"Processando {len(arquivos)} arquivos da data {data_alvo}...")

    records = []
    for f in arquivos:
        try:
            with open(f, "r", encoding="utf-8") as arq:
                conteudo = json.load(arq)

                itens_l = conteudo.get("l", []) if isinstance(conteudo, dict) else []
                if isinstance(conteudo, list):
                    for elem in conteudo:
                        itens_l.extend(elem.get("l", []))

                for item in itens_l:
                    if not isinstance(item, dict):
                        continue
                    linha = item.get("c")
                    for v in item.get("vs", []):
                        records.append({
                            "linha": linha,
                            "p": v.get("p"),
                            "ta": v.get("ta"),
                            "lat": v.get("py"),
                            "lon": v.get("px"),
                        })
        except Exception as e:
            print(f"Erro ao ler arquivo {f}: {e}")
            continue

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

    caminho_saida_1 = dir_parquet / f"distancia_veiculos_{data_alvo}.parquet"
    caminho_saida_2 = dir_csv / f"distancia_veiculos_{data_alvo}.csv"

    df_resumo.to_parquet(caminho_saida_1, index=False)
    df_resumo.to_csv(caminho_saida_2, index=False)

    print(f"Sucesso! Arquivos gerados:")
    print(f" -> Parquet: {caminho_saida_1.resolve()}")
    print(f" -> CSV:     {caminho_saida_2.resolve()}")


# --- Função de varredura para o intervalo de datas ---
def processar_dias_pendentes(data_inicio_str: str = "2026-08-27"):
    dt_inicio = datetime.strptime(data_inicio_str, "%Y-%m-%d").date()
    dt_fim = date.today()

    total_dias = (dt_fim - dt_inicio).days + 1
    print(f"--- Verificando intervalo de {dt_inicio} até {dt_fim} ({total_dias} dia(s)) ---")

    for i in range(total_dias):
        dia_atual = dt_inicio + timedelta(days=i)
        data_formatada = dia_atual.strftime("%Y-%m-%d")
        data_alvo = dia_atual.strftime("%Y%m%d")

        # Verifica se o arquivo final já existe
        saida_parquet = Path(
            f"/opt/airflow/data/processed/data={data_formatada}/files_parquet/distancia_veiculos_{data_alvo}.parquet"
        )

        if saida_parquet.exists():
            print(f"[PULANDO] Data {data_alvo} ({data_formatada}) já foi processada.")
            continue

        print(f"[PROCESSANDO] Data pendente encontrada: {data_alvo} ({data_formatada})")
        processar_distancia_dia_anterior(data_alvo)


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
    schedule="0 3 * * *",
    catchup=False,
    tags=["processamento_distancia", "distancia_km", "distancia_m"],
    max_active_runs=1,
) as dag:

    task_processar = PythonOperator(
        task_id="processar_pendentes_ou_dia_anterior",
        python_callable=processar_dias_pendentes,
        op_kwargs={"data_inicio_str": "2026-08-27"},
    )


# Execução manual fora do Airflow
if __name__ == "__main__":
    processar_dias_pendentes("2026-08-27")