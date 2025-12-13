# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, from_json, max as spark_max, to_date, current_date, sha2
from pyspark.sql.types import StructType, StructField, StringType
from delta.tables import DeltaTable
import requests
import json
from datetime import datetime, timedelta

spark = (
    SparkSession.builder
    .appName("BronzeIncrementalAPI")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

bronze_path_full_load = "s3://aws-us-east-1-112557133617-gov-transparencia-pipe/bronze/cartoes"
bronze_path_incremental = "s3://aws-us-east-1-112557133617-gov-transparencia-pipe/bronze/cartoes_incremental"
api_url = "https://api.portaldatransparencia.gov.br/api-de-dados/cartoes"
key_api = "e941af3608f165f7b41587c237ec0924"

schema = StructType([
    StructField("id", StringType(), True),
    StructField("mesExtrato", StringType(), True),
    StructField("dataTransacao", StringType(), True),
    StructField("valorTransacao", StringType(), True),
    StructField("tipoCartao", StructType([
        StructField("id", StringType(), True),
        StructField("codigo", StringType(), True),
        StructField("descricao", StringType(), True)
    ]), True),
    StructField("estabelecimento", StructType([
        StructField("nome", StringType(), True),
        StructField("cnpjFormatado", StringType(), True)
    ]), True)
])

print("📖 Lendo tabela full load...")
bronze_table_full_load = DeltaTable.forPath(spark, bronze_path_full_load)
df_bronze_full_load = bronze_table_full_load.toDF()

df_parsed_full_load = df_bronze_full_load.withColumn("parsed_json", from_json(col("raw_json"), schema))
df_exploded_full_load = df_parsed_full_load.select("parsed_json.*")

ultima_data_full = df_exploded_full_load.select(
    spark_max(to_date(col("dataTransacao"), "dd/MM/yyyy")).alias("ultima_data")
).collect()[0]["ultima_data"]

print(f"✅ Última dataTransacao no full load: {ultima_data_full}")

try:
    delta_table_inc = DeltaTable.forPath(spark, bronze_path_incremental)
    df_incremental = delta_table_inc.toDF()
    
    ultima_data_inc = df_incremental.select(
        spark_max(to_date(col("dataTransacao"), "dd/MM/yyyy")).alias("ultima_data")
    ).collect()[0]["ultima_data"]
    
    print(f"✅ Última dataTransacao no incremental: {ultima_data_inc}")
    
    if ultima_data_inc and ultima_data_inc > ultima_data_full:
        ultima_data = ultima_data_inc
        print(f"🎯 Usando data do incremental (mais recente)")
    else:
        ultima_data = ultima_data_full
        print(f"🎯 Usando data do full load")
    
    tabela_incremental_existe = True
    
except:
    print("⚠️  Tabela incremental não existe ainda")
    ultima_data = ultima_data_full
    tabela_incremental_existe = False

print(f"\n📅 Última dataTransacao considerada: {ultima_data}")
data_extracao_inicio = ultima_data - timedelta(days=2)
print(f"📅 Extrair de: {data_extracao_inicio} até hoje")
print(f"💡 Overlap de 2 dias para garantir que não perca nenhum dado\n")

today = datetime.today().date()
headers = {"accept": "*/*", "chave-api-dados": key_api}

todos_dados = []
pagina = 1

print("🔄 Iniciando EXTRAÇÃO incremental da API...")

while True:
    params = {
        "codigoOrgao": "38000",
        "dataTransacaoInicio": data_extracao_inicio.strftime("%d/%m/%Y"),
        "dataTransacaoFim": today.strftime("%d/%m/%Y"),
        "pagina": pagina
    }

    print(f"📡 Requisitando página {pagina}")
    response = requests.get(api_url, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    dados_pagina = response.json()
    
    if not dados_pagina:
        print(f"🚫 Página {pagina} vazia. Encerrando paginação.")
        break
    
    todos_dados.extend(dados_pagina)
    print(f"✅ Página {pagina} | {len(dados_pagina)} registros | Total acumulado: {len(todos_dados)}")
    pagina += 1

if len(todos_dados) == 0:
    print("⚠️  Nenhum dado novo para inserir. Encerrando.")
    exit(0)

print("🔨 Processando dados extraídos...")

todos_dados_json = [json.dumps(reg, ensure_ascii=False) for reg in todos_dados if reg is not None]
df_novos = spark.createDataFrame(todos_dados_json, "string").toDF("raw_json")

df_novos_parsed = df_novos.withColumn("parsed_json", from_json(col("raw_json"), schema))
df_novos_exploded = (
    df_novos_parsed
    .select("parsed_json.*")
    .withColumn("data_extracao", current_date())
    .withColumn("id_hash", sha2(col("id"), 256))  # 🔑 Hash SHA-256 do ID
)

df_size = df_novos.select(length(col("raw_json")).alias("row_size"))
avg_row_size = df_size.agg({"row_size": "avg"}).collect()[0][0] or 0
total_rows = df_novos.count()
total_size_mb = avg_row_size * total_rows / (1024 * 1024)
num_partitions = max(1, int(total_size_mb / 100))
df_novos_exploded = df_novos_exploded.repartition(num_partitions)

print(f"📦 Partições calculadas: {num_partitions}")
print(f"📊 Total de registros a processar: {total_rows}")

if tabela_incremental_existe:
    delta_table = DeltaTable.forPath(spark, bronze_path_incremental)
    print("✅ Tabela incremental encontrada")
    
    df_existente = delta_table.toDF()
    ids_existentes = set([row.id_hash for row in df_existente.select("id_hash").collect()])
    ids_novos = set([row.id_hash for row in df_novos_exploded.select("id_hash").collect()])
    
    ids_duplicados = ids_novos.intersection(ids_existentes)
    ids_realmente_novos = ids_novos - ids_existentes
    
    print(f"📊 Registros a processar: {len(ids_novos)}")
    print(f"🔄 Registros que serão atualizados: {len(ids_duplicados)}")
    print(f"✨ Registros novos que serão inseridos: {len(ids_realmente_novos)}")
    
    print("🔄 Executando MERGE idempotente (UPSERT)...")
    
    merge_result = (
        delta_table.alias("tgt")
        .merge(
            source=df_novos_exploded.alias("src"),
            condition="tgt.id_hash = src.id_hash"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    
    print("💾 MERGE concluído com sucesso!")
    
else:
    
    print("📝 Tabela incremental não existe. Criando pela primeira vez...")
    
    df_novos_exploded.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("dataTransacao") \
        .save(bronze_path_incremental)
    
    print("✅ Tabela Delta criada com sucesso!")
    print(f"✨ Total de registros inseridos: {total_rows}")

print("\n" + "=" * 70)
print("✨ PROCESSAMENTO CONCLUÍDO COM SUCESSO")
print("=" * 70)

df_novos_exploded.printSchema()
print("\n📋 Amostra dos dados processados:")
df_novos_exploded.show(10, truncate=False)

print(f"\n✔ Total de registros processados: {total_rows}")
print(f"✔ Caminho da tabela incremental: {bronze_path_incremental}")
print(f"✔ Idempotência garantida via id_hash (SHA-256)")
print("=" * 70)
