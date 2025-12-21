# Databricks notebook source
from pyspark.sql import SparkSession
import requests
from datetime import datetime
import json
from pyspark.sql.functions import col, length

spark = (
    SparkSession.builder
    .appName("BronzeFullLoadAPI")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

secret_data_api = dbutils.secrets.get(scope = "secret_scope", key = "key_api")

api_url = "https://api.portaldatransparencia.gov.br/api-de-dados/cartoes"

today = datetime.today().date()

headers = {
    "accept": "*/*",
    "chave-api-dados": secret_data_api,
}

todos_dados = []
pagina = 1

print("🔄 Iniciando FULL LOAD da API...")

while True:
    params = {
        "codigoOrgao": "38000",
        "pagina": pagina,
        "dataTransacaoInicio": "01/01/2020",
        "dataTransacaoFim": today.strftime("%d/%m/%Y")
    }

    print(f"📡 Requisitando página {pagina}")

    response = requests.get(
        api_url,
        headers=headers,
        params=params,
        timeout=30
    )
    response.raise_for_status()

    dados_pagina = response.json()

    if not dados_pagina:
        print(f"🚫 Página {pagina} vazia. Encerrando paginação.")
        break

    todos_dados.extend(dados_pagina)

    print(
        f"✅ Página {pagina} | "
        f"{len(dados_pagina)} registros | "
        f"Total acumulado: {len(todos_dados)}"
    )

    pagina += 1

print(f"📦 Total de registros extraídos: {len(todos_dados)}")

if len(todos_dados) == 0:
    raise Exception("API retornou 0 registros. Abortando.")

print("🔧 Convertendo registros para JSON string")

todos_dados_json = [
    json.dumps(reg, ensure_ascii=False)
    for reg in todos_dados
    if reg is not None
]

print(f"🔧 Registros convertidos: {len(todos_dados_json)}")

print("🔥 Criando DataFrame Spark (Bronze RAW)")

df_bronze = spark.createDataFrame(
    todos_dados_json,
    "string"
).toDF("raw_json")

print("📏 Calculando tamanho médio dos registros")

df_size = df_bronze.select(length(col("raw_json")).alias("row_size"))

avg_row_size = df_size.agg({"row_size": "avg"}).collect()[0][0]
total_rows = df_bronze.count()

total_size_bytes = avg_row_size * total_rows
total_size_mb = total_size_bytes / (1024 * 1024)

print(f"📏 Tamanho médio por registro: {avg_row_size:.2f} bytes")
print(f"📏 Tamanho total estimado: {total_size_mb:.2f} MB")

target_file_size_mb = 100

num_partitions = max(
    1,
    int(total_size_mb / target_file_size_mb)
)

print(f"📦 Partições calculadas: {num_partitions}")
print("💾 Iniciando escrita no Delta Bronze")

(
    df_bronze
    .repartition(num_partitions)
    .write
    .format("delta")
    .mode("overwrite")
    .save(bronze_path)
)

print("💾 Escrita concluída")

df_bronze.printSchema()
df_bronze.show(10, truncate=False)

print(f"✔ Bronze reconstruído com {total_rows} registros")
