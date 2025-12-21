# Databricks notebook source
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = (
    SparkSession.builder
    .appName("VerificarIncremental")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

bronze_path_incremental = "s3://aws-us-east-1-112557133617-gov-transparencia-pipe/bronze/cartoes_incremental"

print("=" * 70)
print("🔍 VERIFICAÇÃO DA TABELA INCREMENTAL")
print("=" * 70)

delta_table = DeltaTable.forPath(spark, bronze_path_incremental)
df = delta_table.toDF()

total_registros = df.count()
print(f"\n📊 Total de registros na tabela: {total_registros}")

total_ids_unicos = df.select("id").distinct().count()
print(f"🔑 Total de IDs únicos: {total_ids_unicos}")

total_hash_unicos = df.select("id_hash").distinct().count()
print(f"🔐 Total de ID_HASH únicos: {total_hash_unicos}")

if total_registros == total_ids_unicos == total_hash_unicos:
    print("\n✅ IDEMPOTÊNCIA CONFIRMADA: Não há duplicatas!")
else:
    print(f"\n⚠️  ATENÇÃO: Possíveis duplicatas detectadas!")
    print(f"   Registros totais: {total_registros}")
    print(f"   IDs únicos: {total_ids_unicos}")
    print(f"   Diferença: {total_registros - total_ids_unicos} duplicatas")

print("\n📅 Registros agrupados por data de extração:")
df.groupBy("data_extracao").count().orderBy("data_extracao", ascending=False).show()

print("\n🔎 Detalhes do registro ID 477877605:")
df.filter(df.id == "477877605").select(
    "id", "dataTransacao", "valorTransacao", "data_extracao", "id_hash"
).show(truncate=False)

print("\n📜 Histórico de versões da tabela (últimas 5 operações):")
delta_table.history(5).select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)

print("\n" + "=" * 70)
print("✅ VERIFICAÇÃO CONCLUÍDA")
print("=" * 70)
