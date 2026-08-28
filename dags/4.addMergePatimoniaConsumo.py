from pathlib import Path
from datetime import timedelta
import numpy as np
import pandas as pd
import pendulum

from airflow.decorators import dag, task


@dag(
    dag_id="etl_tabela_patrimonial_conama",
    description="Pipeline para gerar tabela CONAMA e normalizar base patrimonial com emissões",
    schedule="0 0 1 * *",  # Ajuste para uma cron expression (ex: "0 6 * * *") se quiser agendar
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    tags=["MANUAL", "PATRIMONIAL", "CONAMA", "EMISSOES"],
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)
def pipeline_patrimonial_conama():

    @task
    def gerar_tabela_conama() -> str:
        """Gera a tabela de referência CONAMA e salva em CSV."""
        ano_atual = pendulum.now("America/Sao_Paulo").year

        tabela_conama_ano = pd.DataFrame(
            {
                "fase_conama": ["P3", "P4", "P5", "P6", "P7", "P8"],
                "ano_inicio": [1994, 1998, 2005, 2009, 2012, 2023],
            }
        )

        tabela_conama_ano["ano_fim"] = tabela_conama_ano["ano_inicio"].shift(-1) - 1
        tabela_conama_ano["ano_fim"] = tabela_conama_ano["ano_fim"].fillna(ano_atual).astype(int)

        output_dir = Path("/opt/airflow/data/MANUAL")
        output_dir.mkdir(parents=True, exist_ok=True)
        arquivo_destino = output_dir / "TABELA_CONAMA.csv"

        tabela_conama_ano.to_csv(arquivo_destino, index=False)
        print(f"Tabela CONAMA gravada em: {arquivo_destino}")
        return str(arquivo_destino)

    @task
    def normalizar_dados_patrimoniais() -> str:
        """Lê os dados brutos de patrimônio, aplica regras CONAMA/poluentes e salva a base normalizada."""
        path_file = Path("/opt/airflow/data/MANUAL")
        name_file_patrimonial = "Patrimonial - 31 05 2026 Modificado.xlsx"
        arquivo_patrimonial = path_file / name_file_patrimonial

        if not arquivo_patrimonial.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {arquivo_patrimonial}")

        df_tabela_patrimonial = pd.read_excel(arquivo_patrimonial)

        # 1° FASE: LIMPEZA PRELIMINAR DOS DADOS
        remover_tecnologias = [
            "eMidiônibus\xa0",
            "eMiniônibus\xa0",
            "Midiônibus Rural\xa0",
            "Micro Ônibus\xa0",
            "nan",
        ]
        condicao_remover = (
            df_tabela_patrimonial["Tecnologia"].isin(remover_tecnologias)
            | df_tabela_patrimonial["Tecnologia"].isna()
        )
        df_limpo = df_tabela_patrimonial[~condicao_remover].copy()

        # Limpeza e conversão de Ano Modelo
        df_limpo["Ano Modelo"] = df_limpo["Ano Modelo"].astype(str).str.strip("\xa0")
        df_limpo["Ano Modelo"] = pd.to_numeric(df_limpo["Ano Modelo"], errors="coerce")
        df_limpo.dropna(subset=["Ano Modelo"], inplace=True)
        df_limpo["Ano Modelo"] = df_limpo["Ano Modelo"].astype(pd.Int64Dtype())
        df_limpo.sort_values("Ano Modelo", inplace=True)

        # 2° FASE: APLICAÇÃO DO CRITÉRIO CONAMA
        tipos_tecnologia = [
            "Midiônibus\xa0",
            "Padron\xa0",
            "Básico\xa0",
            "Articulado\xa0",
            "Miniônibus\xa0",
            "eBásico\xa0",
        ]

        df_diesel = df_limpo[df_limpo["Tecnologia"].isin(tipos_tecnologia)].copy()
        df_outros = df_limpo[~df_limpo["Tecnologia"].isin(tipos_tecnologia)].copy()

        limites_anos = [1994, 1997, 2004, 2008, 2011, 2022, np.inf]
        nomes_fases = ["P3", "P4", "P5", "P6", "P7", "P8"]

        df_diesel["fase_conama"] = pd.cut(
            df_diesel["Ano Modelo"],
            bins=limites_anos,
            labels=nomes_fases,
            right=False,
        )
        df_diesel["fase_conama"] = (
            df_diesel["fase_conama"]
            .cat.add_categories("Não se aplica")
            .fillna("Não se aplica")
        )

        df_outros["fase_conama"] = "Não se aplica"
        df_resultado_final = pd.concat([df_diesel, df_outros], ignore_index=True)

        # 3° FASE: MAPEAMENTO DE CONSUMO E EMISSÃO
        mapeamento_poluentes = {
            "P5": {"NOx(kg poluentes/kg diesel)": 0.020982, "MP(kg poluentes/kg diesel)": 0.000388},
            "P7": {"NOx(kg poluentes/kg diesel)": 0.006575, "MP(kg poluentes/kg diesel)": 0.000055},
            "P8": {"NOx(kg poluentes/kg diesel)": 0.001112, "MP(kg poluentes/kg diesel)": 0.000026},
        }

        df_resultado_final["NOx(kg poluentes/kg diesel)"] = df_resultado_final["fase_conama"].map(
            lambda x: mapeamento_poluentes.get(x, {}).get("NOx(kg poluentes/kg diesel)", np.nan)
        )

        df_resultado_final["MP(kg poluentes/kg diesel)"] = df_resultado_final["fase_conama"].map(
            lambda x: mapeamento_poluentes.get(x, {}).get("MP(kg poluentes/kg diesel)", np.nan)
        )

        # Salva o arquivo final
        output_dir_csv = Path("/opt/airflow/data/MANUAL")
        output_dir_csv.mkdir(parents=True, exist_ok=True)
        arquivo_destino = output_dir_csv / "tabela_patrimonial_normalizada.csv"

        df_resultado_final.to_csv(arquivo_destino, index=False)
        print(f"Processamento concluído! Arquivo gerado em: {arquivo_destino}")
        return str(arquivo_destino)

    # Define dependências e ordem de execução
    task_conama = gerar_tabela_conama()
    task_patrimonial = normalizar_dados_patrimoniais()

    task_conama >> task_patrimonial


# Instancia a DAG
dag_instancia = pipeline_patrimonial_conama()