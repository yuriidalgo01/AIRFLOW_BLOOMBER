"""
DAG: SPTrans - Cálculo de Emissão de Poluentes
Autor: Yuri Idalgo de Matos da Silva
Criação: 28/08/2026
"""

from datetime import datetime, timedelta
from pathlib import Path
import sys
import numpy as np
import pandas as pd
import pendulum
from scipy.stats import median_abs_deviation


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
    df["emissao_nox(kg)"] = (
        df["distancia_km"] * df["NOx(kg poluentes/kg diesel)"] * df["com_ar_kg_km"]
    )
    df["emissao_mp(kg)"] = (
        df["distancia_km"] * df["MP(kg poluentes/kg diesel)"] * df["com_ar_kg_km"]
    )
    
    return df

# Exemplo de função para filtrar usando o MAD (Median Absolute Deviation)
def remove_outliers_mad(df, coluna, threshold=3.5):
    dados = df[coluna]
    mediana = np.median(dados)
    mad = median_abs_deviation(dados)

    # Evita divisão por zero se o MAD for 0
    if mad == 0:
        return df

    z_scores_modificados = 0.6745 * (dados - mediana) / mad
    return df[np.abs(z_scores_modificados) < threshold]


def executar_calculo_emissoes_por_data(target_ds_nodash: str):
    """
    Executa a consolidação e cálculo para uma data específica (formato YYYYMMDD).
    """
    target_ds = datetime.strptime(target_ds_nodash, "%Y%m%d").strftime("%Y-%m-%d")
    
    path_distancia = PROCESSED_DIR / f"data={target_ds}" / "files_csv" / f"distancia_veiculos_{target_ds_nodash}.csv"
    path_patrimonial = MANUAL_DIR / "tabela_patrimonial_normalizada.csv"
    path_consumo = MANUAL_DIR / "consumo_diesel.csv"

    if not path_distancia.exists():
        print(f"[AVISO] Arquivo de distância não encontrado para a data {target_ds_nodash}: {path_distancia}")
        return None

    # Leitura dos arquivos
    df_patrimonial = pd.read_csv(path_patrimonial)
    df_consumo = pd.read_csv(path_consumo)
    df_distancia = pd.read_csv(path_distancia)

    # Padronização e enriquecimento da tabela de consumo
    filtro_articulados = df_consumo["tecnologia"].isin(["Articulado (18m)", "Articulado (23m)"])
    media_articulados = df_consumo[filtro_articulados].mean(numeric_only=True)
    
    linha_articulado = pd.DataFrame([media_articulados])
    linha_articulado["tecnologia"] = "Articulado"
    
    df_consumo = pd.concat([df_consumo, linha_articulado], ignore_index=True)
    df_consumo["tecnologia"] = df_consumo["tecnologia"].astype(str).str.strip()
    df_consumo.rename(columns={"tecnologia": "Tecnologia"}, inplace=True)

    # Tratamento e Merges
    df_patrimonial["bus_id"] = df_patrimonial["Prefixo"].astype(str).str.strip()
    df_patrimonial["Tecnologia"] = df_patrimonial["Tecnologia"].astype(str).str.replace("\xa0", " ").str.strip()
    df_distancia["bus_id"] = df_distancia["bus_id"].astype(str).str.strip()

    df_merged = pd.merge(df_patrimonial, df_consumo, on="Tecnologia", how="inner")
    df_final = pd.merge(df_merged, df_distancia, on="bus_id", how="inner")

    # Cálculo das emissões
    df_final = calcular_emissoes(df_final)

    # Tratamento de outliers e remoção de veículos em "taxiamento" -> lógica de negócio
    df_final = remove_outliers_mad(df_final,'distancia_km')
    df_final = df_final[df_final['distancia_km'] >= 15]

    # Exportação dos resultados
    output_dir = PROCESSED_DIR / "calculo_emissoes"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    arquivo_destino = output_dir / f"tabela_emissoes_{target_ds_nodash}.csv"
    df_final.to_csv(arquivo_destino, index=False)
    
    print(f"[SUCESSO] Arquivo gerado: {arquivo_destino}")
    return str(arquivo_destino)


def processar_emissoes_pendentes():
    """Varre todas as partições de distância geradas e processa as emissões faltantes."""
    pastas_distancia = sorted(PROCESSED_DIR.glob("data=*"))
    output_dir = PROCESSED_DIR / "calculo_emissoes"

    for pasta in pastas_distancia:
        data_formatada = pasta.name.split("=")[-1]  # Ex: '2026-08-27'
        try:
            data_alvo = datetime.strptime(data_formatada, "%Y-%m-%d").strftime("%Y%m%d")
        except ValueError:
            continue

        arquivo_destino = output_dir / f"tabela_emissoes_{data_alvo}.csv"

        if arquivo_destino.exists():
            print(f"[PULANDO] Emissões para a data {data_alvo} já existem.")
            continue

        print(f"[PROCESSANDO] Gerando emissões pendentes para: {data_alvo}")
        executar_calculo_emissoes_por_data(data_alvo)


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
    def consolidar_e_calcular_emissoes(logical_date: datetime = None) -> str:
        data_d_menos_1 = logical_date - timedelta(days=1)
        target_ds_nodash = data_d_menos_1.strftime("%Y%m%d")
        return executar_calculo_emissoes_por_data(target_ds_nodash)

    consolidar_e_calcular_emissoes()


dag_instance = dag_calculo_emissoes()


# --- Execução Manual via Terminal ---
if __name__ == "__main__":
    if len(sys.argv) > 1:
        data_escolhida = sys.argv[1]
        print(f"Executando emissões para data específica: {data_escolhida}")
        executar_calculo_emissoes_por_data(data_escolhida)
    else:
        print("Nenhuma data informada. Verificando dias pendentes...")
        processar_emissoes_pendentes()