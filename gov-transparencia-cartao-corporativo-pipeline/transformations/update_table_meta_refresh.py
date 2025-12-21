# Databricks notebook source
spark.sql("""
MERGE INTO gov_transparencia.silver.metadata_refresh t
USING (
  SELECT
    'pipeline_cartao_corporativo' AS cartao_corporativo,
    current_timestamp()           AS last_update,
    'SUCCESS'                     AS status
) s
ON t.cartao_corporativo = s.cartao_corporativo
WHEN MATCHED THEN
  UPDATE SET
    last_update = s.last_update,
    status      = s.status
WHEN NOT MATCHED THEN
  INSERT (cartao_corporativo, last_update, status)
  VALUES (s.cartao_corporativo, s.last_update, s.status)
""")

