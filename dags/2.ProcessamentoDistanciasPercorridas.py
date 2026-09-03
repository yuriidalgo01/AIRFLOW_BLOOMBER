from datetime import datetime, timedelta, date
from pathlib import Path
import glob
import argparse
import sys
import orjson
import polars as pl
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator


def haversine_polars(lat1, lon1, lat2, lon2):
    R = 6371000.0  # Raio médio em metros
    phi1, phi2 = lat1.radians(), lat2.radians()
    dphi = (lat2 - lat1).radians()
    dlambda = (lon2 - lon1).radians()

    a = (dphi / 2.0).sin().pow(2) + phi1.cos() * phi2.cos() * (dlambda / 2.0).sin().pow(2)
    c = 2.0 * pl.arctan2(a.sqrt(), (1.0 - a).sqrt())
    return R * c


def extrair_dados_arquivo(fpath: str):
    linhas, ps, tas, lats, lons = [], [], [], [], []
    try:
        with open(fpath, "rb") as arq:
            conteudo = orjson.loads(arq.read())

        itens_l = conteudo.get("l", []) if isinstance(conteudo, dict) else []
        if isinstance(conteudo, list):
            for elem in conteudo:
                itens_l.extend(elem.get("l", []))

        for item in itens_l:
            if not isinstance(item, dict):
                continue
            linha = item.get("c")
            for v in item.get("vs", []):
                linhas.append(linha)
                ps.append(v.get("p"))
                tas.append(v.get("ta"))
                lats.append(v.get("py"))
                lons.append(v.get("px"))
    except Exception as e:
        print(f"Erro ao ler {fpath}: {e}")

    return linhas, ps, tas, lats, lons


def processar_distancia_dia_anterior(data_alvo: str, forcar: bool = False, **kwargs):
    """
    Processa os dados de uma data específica (formato YYYYMMDD).
    Verifica automaticamente se o arquivo final já foi gerado.
    """
    data_formatada = datetime.strptime(data_alvo, "%Y%m%d").strftime("%Y-%m-%d")
    base_dir = Path(f"/opt/airflow/data/raw/POSICAO/data={data_formatada}")
    output_dir = Path(f"/opt/airflow/data/processed/data={data_formatada}")

    dir_parquet = output_dir / "files_parquet"
    dir_csv = output_dir / "files_csv"
    caminho_parquet = dir_parquet / f"distancia_veiculos_{data_alvo}.parquet"
    caminho_csv = dir_csv / f"distancia_veiculos_{data_alvo}.csv"

    # Verificação automática de processamentos anteriores
    if caminho_parquet.exists() and not forcar:
        print(f"[PULANDO] {data_alvo} ({data_formatada}) já processado em: {caminho_parquet.name}")
        return

    arquivos = glob.glob(str(base_dir / f"posicao_{data_alvo}_*.json"))
    if not arquivos:
        print(f"[AVISO] Nenhum arquivo JSON encontrado em: {base_dir}")
        return

    print(f"[PROCESSANDO] {len(arquivos)} arquivos da data {data_alvo}...")
    dir_parquet.mkdir(parents=True, exist_ok=True)
    dir_csv.mkdir(parents=True, exist_ok=True)

    all_linhas, all_ps, all_tas, all_lats, all_lons = [], [], [], [], []
    for f in arquivos:
        l, p, ta, lat, lon = extrair_dados_arquivo(f)
        all_linhas.extend(l)
        all_ps.extend(p)
        all_tas.extend(ta)
        all_lats.extend(lat)
        all_lons.extend(lon)

    if not all_ps:
        print(f"[AVISO] Nenhum registro válido encontrado em {data_alvo}.")
        return

    df = pl.DataFrame({
        "linha": pl.Series(all_linhas, dtype=pl.Utf8),
        "p": pl.Series(all_ps, dtype=pl.Int64),
        "ta": pl.Series(all_tas, dtype=pl.Utf8),
        "lat": pl.Series(all_lats, dtype=pl.Float64),
        "lon": pl.Series(all_lons, dtype=pl.Float64),
    })

    del all_linhas, all_ps, all_tas, all_lats, all_lons

    # Pipeline lazy/vetorizado no Polars
    df_resumo = (
        df.with_columns(
            pl.col("ta").str.to_datetime(time_zone="UTC")
        )
        .sort(["p", "ta"])
        .with_columns([
            pl.col("lat").shift(1).over("p").alias("lat_prev"),
            pl.col("lon").shift(1).over("p").alias("lon_prev"),
        ])
        .with_columns(
            haversine_polars(
                pl.col("lat_prev"), pl.col("lon_prev"),
                pl.col("lat"), pl.col("lon")
            ).fill_null(0.0).alias("distancia_m")
        )
        .group_by(["linha", "p"])
        .agg(pl.col("distancia_m").sum())
        .with_columns([
            (pl.col("distancia_m") / 1000.0).round(3).alias("distancia_km"),
            pl.lit(data_formatada).str.to_date().alias("data")
        ])
        .rename({"linha": "line_id", "p": "bus_id"})
    )

    df_resumo.write_parquet(caminho_parquet)
    df_resumo.write_csv(caminho_csv)
    print(f"[CONCLUÍDO] Sucesso para {data_alvo}.")


def processar_periodo(data_inicio: str, data_fim: str = None, forcar: bool = False):
    """Varre todas as pastas no intervalo de datas e processa apenas as pendentes."""
    dt_inicio = datetime.strptime(data_inicio, "%Y-%m-%d").date()
    dt_fim = datetime.strptime(data_fim, "%Y-%m-%d").date() if data_fim else date.today()

    total_dias = (dt_fim - dt_inicio).days + 1
    print(f"--- Checando período: {dt_inicio} até {dt_fim} ({total_dias} dia(s)) ---")

    for i in range(total_dias):
        dia_atual = dt_inicio + timedelta(days=i)
        data_alvo = dia_atual.strftime("%Y%m%d")
        processar_distancia_dia_anterior(data_alvo, forcar=forcar)


def executar_tarefa_airflow(data_inicio_fixa: str = "2026-08-27", forcar: bool = False, **kwargs):
    """
    Função ponte executada pelo PythonOperator do Airflow.
    Permite passar datas manuais via DAG Conf no Airflow Web UI se desejar:
    Ex: {"data_inicio": "2026-08-20", "forcar": true}
    """
    dag_run = kwargs.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}

    data_inicio = conf.get("data_inicio", data_inicio_fixa)
    data_fim = conf.get("data_fim", None)
    forcar_execucao = conf.get("forcar", forcar)

    processar_periodo(data_inicio=data_inicio, data_fim=data_fim, forcar=forcar_execucao)


# --- Definição da DAG no Airflow ---
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="processamento_diario_posicao_onibus_polars",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 8, 27, tz="America/Sao_Paulo"),
    schedule="0 3 * * *",
    catchup=False,
    tags=["processamento_distancia", "polars", "telemetria"],
    max_active_runs=1,
) as dag:

    task_processar = PythonOperator(
        task_id="processar_pendentes_ou_dia_anterior",
        python_callable=executar_tarefa_airflow,
        op_kwargs={
            "data_inicio_fixa": "2026-08-27",
            "forcar": False
        },
    )


# --- Bloco de Linha de Comando (CLI) ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Processamento vetorial de telemetria de ônibus via Polars.")

    parser.add_argument(
        "--data",
        type=str,
        help="Processa uma data única (formato: YYYYMMDD ou YYYY-MM-DD)."
    )
    parser.add_argument(
        "--data-inicio",
        type=str,
        default="2026-08-27",
        help="Data inicial para backfill (formato: YYYY-MM-DD). Padrão: 2026-08-27"
    )
    parser.add_argument(
        "--data-fim",
        type=str,
        default=None,
        help="Data final para backfill (formato: YYYY-MM-DD). Se omitido, usa a data atual."
    )
    parser.add_argument(
        "--forcar",
        action="store_true",
        help="Força o reprocessamento mesmo se o arquivo Parquet de saída já existir."
    )

    args = parser.parse_args()

    if args.data:
        data_limpa = args.data.replace("-", "")
        processar_distancia_dia_anterior(data_limpa, forcar=args.forcar)
    else:
        processar_periodo(
            data_inicio=args.data_inicio,
            data_fim=args.data_fim,
            forcar=args.forcar
        )