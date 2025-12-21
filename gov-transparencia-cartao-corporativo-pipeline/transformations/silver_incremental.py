# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, to_date, regexp_replace, year, month, dayofmonth, lit
from pyspark.sql.types import DecimalType, IntegerType, StringType
from delta.tables import DeltaTable

bronze_path_incremental = "s3://aws-us-east-1-112557133617-gov-transparencia-pipe/bronze/cartoes_incremental"
table_full_name = "gov_transparencia.silver.cartao_corporativo"

df_bronze = spark.read.format("delta").load(bronze_path_incremental)
print(f"📊 Total de registros no Bronze incremental: {df_bronze.count()}")
df_bronze.printSchema()

df_silver_novos = df_bronze.select(
    col("id").cast(StringType()).alias("id"),
    sha2(col("id").cast(StringType()), 256).alias("id_hash"),
    to_date(col("dataTransacao"), "dd/MM/yyyy").alias("data_transacao"),
    col("mesExtrato").alias("mes_extrato"),
    regexp_replace(regexp_replace(col("valorTransacao"), r"\.", ""), ",", ".").cast(DecimalType(15,2)).alias("valor_transacao"),
    
    (col("tipoCartao.id").cast(StringType()) if "tipoCartao" in df_bronze.columns else lit(None)).alias("tipo_cartao_id"),
    (col("tipoCartao.codigo").cast(StringType()) if "tipoCartao" in df_bronze.columns else lit(None)).alias("tipo_cartao_codigo"),
    (col("tipoCartao.descricao").cast(StringType()) if "tipoCartao" in df_bronze.columns else lit(None)).alias("tipo_cartao_descricao"),
    
    lit(None).cast(StringType()).alias("estabelecimento_id"),
    (col("estabelecimento.nome").cast(StringType()) if "estabelecimento" in df_bronze.columns else lit(None)).alias("estabelecimento_nome"),
    (col("estabelecimento.cnpjFormatado").cast(StringType()) if "estabelecimento" in df_bronze.columns else lit(None)).alias("estabelecimento_cnpj"),
    lit(None).cast(StringType()).alias("estabelecimento_nome_fantasia"),
    lit(None).cast(StringType()).alias("estabelecimento_tipo"),
    
    lit(None).cast(StringType()).alias("unidade_gestora_codigo"),
    lit(None).cast(StringType()).alias("unidade_gestora_nome"),
    lit(None).cast(StringType()).alias("unidade_gestora_poder"),
    
    lit(None).cast(StringType()).alias("orgao_maximo_codigo"),
    lit(None).cast(StringType()).alias("orgao_maximo_nome"),
    lit(None).cast(StringType()).alias("orgao_maximo_sigla"),
    
    lit(None).cast(StringType()).alias("orgao_vinculado_cnpj"),
    lit(None).cast(StringType()).alias("orgao_vinculado_codigo_siafi"),
    lit(None).cast(StringType()).alias("orgao_vinculado_nome"),
    lit(None).cast(StringType()).alias("orgao_vinculado_sigla"),
    
    lit(None).cast(StringType()).alias("portador_cpf"),
    lit(None).cast(StringType()).alias("portador_nome"),
    lit(None).cast(StringType()).alias("portador_nis"),
    
    col("data_extracao").cast(StringType()).alias("data_extracao")
)

df_silver_novos = df_silver_novos \
    .withColumn("ano", year(col("data_transacao")).cast(IntegerType())) \
    .withColumn("mes", month(col("data_transacao")).cast(IntegerType())) \
    .withColumn("dia", dayofmonth(col("data_transacao")).cast(IntegerType()))

df_silver_novos_deduplicated = df_silver_novos.dropDuplicates(["id_hash"])

ids_incremental = set([row.id_hash for row in df_silver_novos_deduplicated.select("id_hash").collect()])
print(f"🟢 Total IDs únicos no incremental: {len(ids_incremental)}")

delta_silver = DeltaTable.forName(spark, table_full_name)
df_silver_existente = delta_silver.toDF()
ids_existentes = set([row.id_hash for row in df_silver_existente.select("id_hash").distinct().collect()])
print(f"🔵 Total IDs já existentes na Silver: {len(ids_existentes)}")

ids_duplicados = ids_incremental.intersection(ids_existentes)
ids_novos = ids_incremental - ids_existentes
print(f"✴️ IDs que serão atualizados: {len(ids_duplicados)}")
print(f"✨ IDs que serão inseridos: {len(ids_novos)}")

delta_silver.alias("tgt").merge(
    source=df_silver_novos_deduplicated.alias("src"),
    condition="tgt.id_hash = src.id_hash"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

print(f"✅ Merge incremental concluído!")

df_verificacao = spark.table(table_full_name)
print(f"📊 Total de registros na Silver após merge: {df_verificacao.count()}")
df_verificacao.orderBy(col("data_transacao").desc()).show(10, truncate=False)

