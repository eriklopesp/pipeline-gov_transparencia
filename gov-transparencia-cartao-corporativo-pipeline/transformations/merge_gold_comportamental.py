# Databricks notebook source
spark.sql("""
MERGE INTO gov_transparencia.gold.gold_comportamental tgt
USING (
  SELECT 
    XXHASH64(TRIM(UPPER(portador_nome))) AS id_portador,
    portador_nome AS nome,
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
  GROUP BY portador_nome
) src
ON tgt.id_portador = src.id_portador

WHEN MATCHED THEN UPDATE SET
  tgt.nome = src.nome,
  tgt.total_transacionado = src.total_transacionado,
  tgt.qtd_transacoes = src.qtd_transacoes,
  tgt.valor_medio_p_transacao = src.valor_medio_p_transacao,
  tgt.desvio_padrao_transacao = src.desvio_padrao_transacao,
  tgt.cv_percentual = src.cv_percentual,
  tgt.recencia_dias = src.recencia_dias,
  tgt.freq_media_mensal = src.freq_media_mensal

WHEN NOT MATCHED THEN INSERT *
""")

