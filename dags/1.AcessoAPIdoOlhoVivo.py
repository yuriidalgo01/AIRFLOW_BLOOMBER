import json
from pathlib import Path
import pendulum
import requests
import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable


#Função Para Auxiliar as Extrações dos dados de Interesse:

def transformar_dados_sptrans(json_data):
    hora_referencia = json_data.get("hr")
    registros = []

    # Itera sobre cada linha de ônibus
    for linha in json_data.get("l", []):
        codigo_linha = linha.get("c")     # Ex: "8677-10"
        letreiro_origem = linha.get("lt0")
        letreiro_destino = linha.get("lt1")
        sentido = linha.get("sl")

        # Itera sobre cada veículo daquela linha
        for veiculo in linha.get("vs", []):
            registros.append({
                "prefixo_veiculo": veiculo.get("p"),     # 'p'
                "hr_referencia": hora_referencia,
                #"codigo_linha": codigo_linha,
                #"letreiro_origem": letreiro_origem,
                #"letreiro_destino": letreiro_destino,
                #"sentido": sentido,
                #"acessivel": veiculo.get("a"),          # 'a'
                "latitude": veiculo.get("py"),          # 'py'
                "longitude": veiculo.get("px"),         # 'px'
                "timestamp_gps": veiculo.get("ta"),     # 'ta'
            })

    # Cria o DataFrame
    df = pd.DataFrame(registros)
    
    # Opcional: Tipagem adequada
    if not df.empty:
        df["timestamp_gps"] = pd.to_datetime(df["timestamp_gps"])
        df["latitude"] = df["latitude"].astype(float)
        df["longitude"] = df["longitude"].astype(float)
        df["prefixo_veiculo"] = df["prefixo_veiculo"].astype(str)

    return df

@dag(
    dag_id="SensorAPIOlhoVivo",
    description="Sensor da API Olho Vivo SPTrans",
    schedule="* * * * *",  # Altere para "*/5 * * * *" quando quiser rodar a cada 5 min
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs = 1,
    tags=["API", "OLHO_VIVO"],
)
def pipeline_olho_vivo():

    @task
    def extrair_posicoes_sptrans():
        token = Variable.get("sptrans_token", default_var="1274680751ad10c6dd7991ac8b7e3d2a51380c8414bc576b9dfc692b848287c8")
        base_url = "http://api.olhovivo.sptrans.com.br/v2.1"
        session = requests.Session()

        # Login
        auth = session.post(f"{base_url}/Login/Autenticar", params={"token": token})
        if auth.text.strip().lower() != "true":
            raise ValueError(f"Falha de autenticação SPTrans: {auth.text}")

        # Posição
        resp = session.get(f"{base_url}/Posicao")
        resp.raise_for_status()
        dados = resp.json()

        # Cria pasta de saída se não existir
        output_dir = Path("/opt/airflow/data/row/POSICAO")
        output_dir.mkdir(parents=True, exist_ok=True)

        ts = pendulum.now("America/Sao_Paulo").strftime("%Y%m%d_%H%M%S")
        arquivo_destino = output_dir / f"posicao_{ts}.json"

        df = transformar_dados_sptrans(dados)

        # Cria pasta de saída se não existir para o formato parquet
        output_dir_parquet = Path("/opt/airflow/data/row/POSICAO_parquet")
        output_dir_parquet.mkdir(parents=True, exist_ok=True)

        arquivo_destino_parquet = output_dir_parquet / f"posicao_{ts}.parquet"

        df.to_parquet(path=arquivo_destino_parquet)

        with open(arquivo_destino, "w", encoding="utf-8") as f:
            json.dump(dados, f, ensure_ascii=False, indent=2)

        print(f"Sucesso! Arquivo gravado em: {arquivo_destino}")
        return str(arquivo_destino)

    extrair_posicoes_sptrans()

pipeline_olho_vivo()