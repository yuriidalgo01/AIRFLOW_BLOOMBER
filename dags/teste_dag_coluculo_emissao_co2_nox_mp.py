"""
DAG: SPTrans - Cálculo de Emissão de Poluentes
Autor: Yuri Idalgo de Matos da Silva
Criação: 28/08/2026
"""

from datetime import datetime, timedelta
from pathlib import Path
import numpy as np
import pandas as pd
import pendulum

from airflow.decorators import dag, task

# Configurações de diretórios base
BASE_DATA_DIR = Path("/opt/airflow/data")
MANUAL_DIR = BASE_DATA_DIR / "MANUAL"
PROCESSED_DIR = BASE_DATA_DIR / "processed"


def calcular_emissoes(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica os fatores de emissão diretamente ao DataFrame consolidado."""
    fator_emissao_co2 = 2.671  # kg por litro de diesel
    
    # Emissão de CO2 em toneladas (t)
    df["emissao_co2(t)"] = (
        (df["distancia_km"] * df["com_ar_l_km"] * fator_emissao_co2) / 1000
    ).round(3)
    
    # Emissão de NOx e Material Particulado (MP)
    df["emissao_nox"] = (
        df["distancia_km"] * df["NOx(kg poluentes/kg diesel)"] * df["com_ar_kg_km"]
    )
    df["emissao_mp"] = (
        df["distancia_km"] * df["MP(kg poluentes/kg diesel)"] * df["com_ar_kg_km"]
    )
    
    return df


@dag(
    dag_id="sptrans_calculo_emissoes_onibus",
    description="Pipeline ETL para cálculo diário de emissão de poluentes da frota SPTrans (D-1)",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 8, 27, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["sptrans", "emissoes", "etl"],
)
def dag_calculo_emissoes():

    @task
    def consolidar_e_calcular_emissoes(logical_date: datetime) -> str:
        """
        Lê as bases manuais e a partição de distância do dia anterior (D-1),
        realiza os tratamentos, aplica os cálculos de emissão e salva o resultado.
        """
        # 1. Calcula a data do dia anterior (D-1)
        data_d_menos_1 = logical_date - timedelta(days=1)
        target_ds = data_d_menos_1.strftime("%Y-%m-%d")          # Ex: '2026-08-27'
        target_ds_nodash = data_d_menos_1.strftime("%Y%m%d")      # Ex: '20260827'

        # 2. Definição dos caminhos com base na partição D-1
        path_distancia = PROCESSED_DIR / f"data={target_ds}" / "files_csv" / f"distancia_veiculos_{target_ds_nodash}.csv"
        path_patrimonial = MANUAL_DIR / "tabela_patrimonial_normalizada.csv"
        path_consumo = MANUAL_DIR / "consumo_diesel.csv"

        # Leitura dos arquivos
        df_patrimonial = pd.read_csv(path_patrimonial)
        df_consumo = pd.read_csv(path_consumo)
        df_distancia = pd.read_csv(path_distancia)

        # 3. Padronização e enriquecimento da tabela de consumo
        filtro_articulados = df_consumo["tecnologia"].isin(["Articulado (18m)", "Articulado (23m)"])
        media_articulados = df_consumo[filtro_articulados].mean(numeric_only=True)
        
        linha_articulado = pd.DataFrame([media_articulados])
        linha_articulado["tecnologia"] = "Articulado"
        
        df_consumo = pd.concat([df_consumo, linha_articulado], ignore_index=True)
        df_consumo["tecnologia"] = df_consumo["tecnologia"].astype(str).str.strip()
        df_consumo.rename(columns={"tecnologia": "Tecnologia"}, inplace=True)

        # 4. Tratamento e Merges
        df_patrimonial["bus_id"] = df_patrimonial["Prefixo"].astype(str).str.strip()
        df_patrimonial["Tecnologia"] = df_patrimonial["Tecnologia"].astype(str).str.replace("\xa0", " ").str.strip()
        df_distancia["bus_id"] = df_distancia["bus_id"].astype(str).str.strip()

        # Merge: Patrimonial + Consumo
        df_merged = pd.merge(df_patrimonial, df_consumo, on="Tecnologia", how="inner")

        # Merge: (+ Distância)
        df_final = pd.merge(df_merged, df_distancia, on="bus_id", how="inner")

        # 5. Cálculo das emissões
        df_final = calcular_emissoes(df_final)

        # 6. Exportação dos resultados
        output_dir = PROCESSED_DIR / "calculo_emissoes"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        arquivo_destino = output_dir / f"tabela_emissoes_{target_ds_nodash}.csv"
        df_final.to_csv(arquivo_destino, index=False)
        
        return str(arquivo_destino)

    consolidar_e_calcular_emissoes()


# Instancia a DAG
dag_instance = dag_calculo_emissoes()