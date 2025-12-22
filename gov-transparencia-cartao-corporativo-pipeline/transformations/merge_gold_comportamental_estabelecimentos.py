# Databricks notebook source
spark.sql("""
MERGE INTO gov_transparencia.gold.gold_comportamental_estabelecimentos tgt
USING (

SELECT
  XXHASH64(TRIM(UPPER(estabelecimento_nome))) AS id_hash_estabelecimento,

  MAX(estabelecimento_nome) AS estabelecimento_nome,
  MAX(estabelecimento_id) AS estabelecimento_id,
  MAX(estabelecimento_cnpj) AS estabelecimento_cnpj,
  MAX(estabelecimento_tipo) AS estabelecimento_tipo,

  CAST(SUM(valor_transacao) AS DECIMAL(18,2)) AS total_transacionado,
  COUNT(id) AS qtd_transacoes,
  CAST(AVG(valor_transacao) AS DECIMAL(18,2)) AS valor_medio_p_transacao,
  CAST(STDDEV(valor_transacao) AS DECIMAL(18,2)) AS desvio_padrao_transacao,
  CAST(
    (STDDEV(valor_transacao) / NULLIF(AVG(valor_transacao), 0)) * 100
    AS DECIMAL(10,2)
  ) AS cv_percentual,

  DATEDIFF(CURRENT_DATE(), MAX(data_transacao)) AS recencia_dias,
  CAST(
    COUNT(id) / COUNT(DISTINCT DATE_TRUNC('month', data_transacao))
    AS DECIMAL(18,2)
  ) AS freq_media_mensal

FROM gov_transparencia.silver.cartao_corporativo

GROUP BY
  id_hash_estabelecimento

) src

ON tgt.id_hash_estabelecimento = src.id_hash_estabelecimento


WHEN MATCHED THEN UPDATE SET
  estabelecimento_nome = src.estabelecimento_nome,
  estabelecimento_id = src.estabelecimento_id,
  estabelecimento_cnpj = src.estabelecimento_cnpj,
  estabelecimento_tipo = src.estabelecimento_tipo,
  total_transacionado = src.total_transacionado,
  qtd_transacoes = src.qtd_transacoes,
  valor_medio_p_transacao = src.valor_medio_p_transacao,
  desvio_padrao_transacao = src.desvio_padrao_transacao,
  cv_percentual = src.cv_percentual,
  recencia_dias = src.recencia_dias,
  freq_media_mensal = src.freq_media_mensal

WHEN NOT MATCHED THEN INSERT *

""")

