# Databricks notebook source
spark.sql("""
MERGE INTO gov_transparencia.gold.gold_series_temporais tgt
USING (
  SELECT
    XXHASH64(TRIM(UPPER(portador_nome))) AS id_portador,
    portador_nome AS nome,

    YEAR(data_transacao) AS ano,
    DATE_TRUNC('month', data_transacao) AS mes,
    date_format(DATE_TRUNC('month', data_transacao), 'MMMM') AS mes_nome,

    CAST(SUM(valor_transacao) AS DECIMAL(18,2)) AS total_mes,
    COUNT(*) AS qtd_transacoes_mes,
    CAST(AVG(valor_transacao) AS DECIMAL(18,2)) AS media_mes

  FROM gov_transparencia.silver.cartao_corporativo

  GROUP BY
    XXHASH64(TRIM(UPPER(portador_nome))),
    portador_nome,
    YEAR(data_transacao),
    DATE_TRUNC('month', data_transacao)
) src

ON  tgt.id_portador = src.id_portador
AND tgt.ano = src.ano
AND tgt.mes = src.mes

WHEN MATCHED THEN UPDATE SET
  nome = src.nome,
  mes_nome = src.mes_nome,
  total_mes = src.total_mes,
  qtd_transacoes_mes = src.qtd_transacoes_mes,
  media_mes = src.media_mes

WHEN NOT MATCHED THEN INSERT *
""")

