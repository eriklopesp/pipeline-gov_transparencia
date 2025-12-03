# save como etl_portal_transparencia.py
import os
import io
import time
import json
import math
import logging
from datetime import datetime
from typing import List, Dict, Any

import requests
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter, Retry

load_dotenv()

# ---------- CONFIG VARIÁVEIS ----------

token = os.getenv("token_api")
endpoint = os.getenv("gastos_cartao_corporativo")
bucket = os.getenv("bucket_name")
prefix = "bronze/cartao_corporativo/"

MAX_BYTES = 200 * 1024 * 1024

# requests retry config
RETRIES = 3
BACKOFF_FACTOR = 1.0

TIMESTAMP_CANDIDATES = [
    "data", "dataHora", "data_transacao", "dataExtrato", "dataMovimento",
    "dataDocumento", "dataReferencia", "dataInicio", "dataFim", "timestamp"
]

# logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("etl_portal")

# ---------- S3 client ----------
session = boto3.Session()
s3 = session.client("s3")

# ---------- HTTP session com retry ----------
http = requests.Session()
retries = Retry(total=RETRIES, backoff_factor=BACKOFF_FACTOR,
                status_forcelist=[400, 429, 500, 502, 503, 504],
                allowed_methods=["GET", "POST"])
http.mount("https://", HTTPAdapter(max_retries=retries))

HEADERS = {
    "accept": "*/*",
    "chave-api-dados": token
}

# ---------- utilitários ----------
def find_timestamp_value(record: Dict[str, Any]) -> datetime:
    """Tenta encontrar um campo timestamp no registro e parsear para datetime.
       Se falhar, retorna None."""
    for k in record.keys():
        kl = k.lower()
        for cand in TIMESTAMP_CANDIDATES:
            if cand.lower() in kl:
                val = record.get(k)
                if not val:
                    continue
                # tentar alguns formats comuns
                for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y %H:%M:%S"):
                    try:
                        return datetime.strptime(val, fmt)
                    except Exception:
                        continue
                # se for timestamp epoch (int/float)
                try:
                    if isinstance(val, (int, float)):
                        return datetime.fromtimestamp(val)
                except Exception:
                    pass
                # fallback: tentar parse via pandas (mais permissivo)
                try:
                    return pd.to_datetime(val)
                except Exception:
                    pass
    return None

def df_to_parquet_bytes(df: pd.DataFrame, compression="snappy") -> bytes:
    """Serializa DataFrame para parquet em memória e retorna bytes."""
    table = pa.Table.from_pandas(df)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression=compression)
    return buf.getvalue()

def upload_bytes_to_s3(data_bytes: bytes, bucket: str, key: str):
    s3.put_object(Bucket=bucket, Key=key, Body=data_bytes)
    logger.info(f"Uploaded {len(data_bytes):,} bytes to s3://{bucket}/{key}")

# ---------- pipeline ----------
def run_pipeline(max_bytes: int = MAX_BYTES):
    pagina = 1
    accumulator: List[Dict[str, Any]] = []
    batch_index = 0
    total_records = 0

    while True:
        params = {"pagina": pagina}
        logger.info(f"Lendo página {pagina} ...")
        try:
            resp = http.get(endpoint, headers=HEADERS, params=params, timeout=30)
        except Exception as e:
            logger.error(f"Erro de conexão: {e}")
            break

        if resp.status_code != 200:
            logger.error(f"Status {resp.status_code} na página {pagina}. Conteúdo: {resp.text}")
            break

        page_data = resp.json()
        if not page_data:
            logger.info("Nenhum dado retornado — fim da paginação.")
            break

        # page_data esperado ser lista de registros
        if not isinstance(page_data, list):
            # se API retornar objeto com lista dentro, tentar extrair
            if isinstance(page_data, dict):
                # heurística: pegar primeiro valor que seja lista
                for v in page_data.values():
                    if isinstance(v, list):
                        page_data = v
                        break

        # se ainda não é lista, log e aborta
        if not isinstance(page_data, list):
            logger.error("Formato inesperado da página (não é lista). Aborting.")
            break

        # adiciona página ao acumulador
        accumulator.extend(page_data)
        total_records += len(page_data)

        # estimar tamanho atual serializando para parquet (exato)
        try:
            df_try = pd.DataFrame(accumulator)
            bytes_buf = df_to_parquet_bytes(df_try)
            size_now = len(bytes_buf)
            logger.info(f"Lote atual: {len(accumulator)} registros — parquet bytes estimado: {size_now:,}")
        except Exception as e:
            logger.error(f"Erro ao serializar parquet para estimativa: {e}")
            # se falhar na serialização, força upload do que já tinha antes (fallback)
            size_now = max_bytes + 1

        # se ultrapassou limite, então precisamos separar:
        if size_now > max_bytes:
            logger.info("Limite excedido — criando arquivo com o que havia antes desta página.")
            # remover os registros desta última página para voltar ao estado anterior
            # mas só se for possível identificar o tamanho da página adicionada
            # estratégia: se a página inteira causou a ultrapassagem, então:
            # se o batch anterior (sem a página) tinha algo -> escrever esse batch
            # caso contrário (uma única página > max_bytes) -> escrevemos mesmo assim

            # recalcular sem a última página
            # supondo que page_data foi a última append, vamos remover len(page_data) registros
            for _ in range(len(page_data)):
                if accumulator:
                    accumulator.pop()

            if len(accumulator) > 0:
                # write previous batch
                df_batch = pd.DataFrame(accumulator)
                bytes_batch = df_to_parquet_bytes(df_batch)
                # determinar key de particionamento por data
                ts = find_timestamp_value(df_batch.iloc[0].to_dict()) or datetime.utcnow()
                part_key = f"ano={ts.year}/mes={ts.month:02d}/dia={ts.day:02d}"
                key = f"{prefix}{part_key}/batch_{batch_index:05d}.parquet"
                upload_bytes_to_s3(bytes_batch, bucket, key)
                batch_index += 1
                # limpar accumulator, e iniciar com os registros da página atual
                accumulator = list(page_data)  # nova lista com apenas a última página
                logger.info(f"Iniciando novo batch com {len(accumulator)} registros (da página corrente).")
            else:
                # o caso em que uma única página excede max_bytes
                logger.warning("Uma única página excedeu o limite máximo. Salvando a página inteira como um arquivo.")
                df_single = pd.DataFrame(page_data)
                bytes_single = df_to_parquet_bytes(df_single)
                ts = find_timestamp_value(df_single.iloc[0].to_dict()) or datetime.utcnow()
                part_key = f"ano={ts.year}/mes={ts.month:02d}/dia={ts.day:02d}"
                key = f"{prefix}{part_key}/batch_{batch_index:05d}.parquet"
                upload_bytes_to_s3(bytes_single, bucket, key)
                batch_index += 1
                accumulator = []  # nada a manter
        else:
            logger.info("Continuando acumulação (não atingiu limite).")

        pagina += 1
        # salvar checkpoint opcional aqui (em S3 ou DynamoDB) para restart/resume

    # fim da paginação: se há algo no accumulator, enviar último arquivo
    if accumulator:
        logger.info("Serializando e enviando último lote remanescente.")
        df_final = pd.DataFrame(accumulator)
        bytes_final = df_to_parquet_bytes(df_final)
        ts = find_timestamp_value(df_final.iloc[0].to_dict()) or datetime.utcnow()
        part_key = f"ano={ts.year}/mes={ts.month:02d}/dia={ts.day:02d}"
        key = f"{prefix}{part_key}/batch_{batch_index:05d}.parquet"
        upload_bytes_to_s3(bytes_final, bucket, key)
        batch_index += 1

    logger.info(f"Concluído. Total de registros processados: {total_records}. Arquivos enviados: {batch_index}.")

if __name__ == "__main__":
    # ajuste MAX_BYTES se quiser outro valor
    run_pipeline(max_bytes=200 * 1024 * 1024)
