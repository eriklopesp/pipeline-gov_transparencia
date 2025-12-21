# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_date, regexp_replace, 
    year, month, dayofmonth, sha2
)
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    DecimalType, IntegerType, DateType
)
from delta.tables import DeltaTable

spark = (
    SparkSession.builder
    .appName("SilverCartoesCorporativos")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .enableHiveSupport()
    .getOrCreate()
)

bronze_path_full = "s3://aws-us-east-1-112557133617-gov-transparencia-pipe/bronze/cartoes"
catalog = "gov_transparencia"
schema = "silver"
table = "cartao_corporativo"
table_full_name = f"{catalog}.{schema}.{table}"

schema_bronze = StructType([
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
        StructField("id", StringType(), True),
        StructField("nome", StringType(), True),
        StructField("cnpjFormatado", StringType(), True),
        StructField("nomeFantasiaReceita", StringType(), True),
        StructField("tipo", StringType(), True)
    ]), True),
    StructField("unidadeGestora", StructType([
        StructField("codigo", StringType(), True),
        StructField("nome", StringType(), True),
        StructField("descricaoPoder", StringType(), True),
        StructField("orgaoMaximo", StructType([
            StructField("codigo", StringType(), True),
            StructField("nome", StringType(), True),
            StructField("sigla", StringType(), True)
        ]), True),
        StructField("orgaoVinculado", StructType([
            StructField("cnpj", StringType(), True),
            StructField("codigoSIAFI", StringType(), True),
            StructField("nome", StringType(), True),
            StructField("sigla", StringType(), True)
        ]), True)
    ]), True),
    StructField("portador", StructType([
        StructField("cpfFormatado", StringType(), True),
        StructField("nome", StringType(), True),
        StructField("nis", StringType(), True)
    ]), True)
])

print("=" * 80)
print("🔄 PROCESSAMENTO SILVER - CARTÕES CORPORATIVOS")
print("=" * 80)

print("\n📖 Lendo dados do Bronze (full load)...")
df_bronze_raw = spark.read.format("delta").load(bronze_path_full)

df_bronze_parsed = df_bronze_raw.withColumn(
    "parsed_json", 
    from_json(col("raw_json"), schema_bronze)
)

df_bronze = df_bronze_parsed.select("parsed_json.*")
print(f"✅ Total de registros no Bronze: {df_bronze_raw.count()}")

print("\n🔨 Aplicando transformações e tipagem...")

df_silver = df_bronze.select(

    col("id").cast(StringType()).alias("id"),
    sha2(col("id"), 256).alias("id_hash"),
    
    to_date(col("dataTransacao"), "dd/MM/yyyy").alias("data_transacao"),
    col("mesExtrato").alias("mes_extrato"),
    
    regexp_replace(col("valorTransacao"), r"\.", "").cast(StringType()).alias("valor_temp1"),
    regexp_replace(col("valor_temp1"), ",", ".").cast(DecimalType(15, 2)).alias("valor_transacao"),
    
    col("tipoCartao.id").cast(IntegerType()).alias("tipo_cartao_id"),
    col("tipoCartao.codigo").cast(IntegerType()).alias("tipo_cartao_codigo"),
    col("tipoCartao.descricao").cast(StringType()).alias("tipo_cartao_descricao"),
    
    col("estabelecimento.id").cast(StringType()).alias("estabelecimento_id"),
    col("estabelecimento.nome").cast(StringType()).alias("estabelecimento_nome"),
    col("estabelecimento.cnpjFormatado").cast(StringType()).alias("estabelecimento_cnpj"),
    col("estabelecimento.nomeFantasiaReceita").cast(StringType()).alias("estabelecimento_nome_fantasia"),
    col("estabelecimento.tipo").cast(StringType()).alias("estabelecimento_tipo"),
    
    col("unidadeGestora.codigo").cast(StringType()).alias("unidade_gestora_codigo"),
    col("unidadeGestora.nome").cast(StringType()).alias("unidade_gestora_nome"),
    col("unidadeGestora.descricaoPoder").cast(StringType()).alias("unidade_gestora_poder"),
    
    col("unidadeGestora.orgaoMaximo.codigo").cast(StringType()).alias("orgao_maximo_codigo"),
    col("unidadeGestora.orgaoMaximo.nome").cast(StringType()).alias("orgao_maximo_nome"),
    col("unidadeGestora.orgaoMaximo.sigla").cast(StringType()).alias("orgao_maximo_sigla"),
    
    col("unidadeGestora.orgaoVinculado.cnpj").cast(StringType()).alias("orgao_vinculado_cnpj"),
    col("unidadeGestora.orgaoVinculado.codigoSIAFI").cast(StringType()).alias("orgao_vinculado_codigo_siafi"),
    col("unidadeGestora.orgaoVinculado.nome").cast(StringType()).alias("orgao_vinculado_nome"),
    col("unidadeGestora.orgaoVinculado.sigla").cast(StringType()).alias("orgao_vinculado_sigla"),
    
    col("portador.cpfFormatado").cast(StringType()).alias("portador_cpf"),
    col("portador.nome").cast(StringType()).alias("portador_nome"),
    col("portador.nis").cast(StringType()).alias("portador_nis")
).drop("valor_temp1")

df_silver = df_silver \
    .withColumn("ano", year(col("data_transacao")).cast(IntegerType())) \
    .withColumn("mes", month(col("data_transacao")).cast(IntegerType())) \
    .withColumn("dia", dayofmonth(col("data_transacao")).cast(IntegerType()))

print("\n🔍 Removendo duplicatas por ID...")
registros_antes = df_silver.count()
df_silver_deduplicated = df_silver.dropDuplicates(["id"])
registros_depois = df_silver_deduplicated.count()
duplicatas_removidas = registros_antes - registros_depois

print(f"  📊 Registros antes: {registros_antes}")
print(f"  ✅ Registros depois: {registros_depois}")
print(f"  🗑️  Duplicatas removidas: {duplicatas_removidas}")

print(f"\n💾 Inserindo dados no catálogo: {table_full_name}")

df_silver_deduplicated.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("ano", "mes") \
    .saveAsTable(table_full_name)

print(f"✅ Tabela criada com sucesso no catálogo!")

print(f"\n📋 Verificando tabela:")
spark.sql(f"SELECT COUNT(*) as total FROM {table_full_name}").show()

print("\n" + "=" * 80)
print("📊 VERIFICAÇÃO DA TABELA NO CATÁLOGO")
print("=" * 80)

df_verificacao = spark.table(table_full_name)
print(f"\n✅ Total de registros: {df_verificacao.count()}")

print("\n📋 Schema:")
df_verificacao.printSchema()

print("\n📊 Amostra:")
df_verificacao.select(
    "id", "data_transacao", "valor_transacao", 
    "estabelecimento_nome", "portador_nome", "ano", "mes"
).show(10, truncate=False)

print("\n" + "=" * 80)
print(f"✨ TABELA {table_full_name} CRIADA COM SUCESSO!")
print("=" * 80)
