# Databricks notebook source
@dp.table(
    name="parquet_ingestion",
    comment="Ingests Parquet data from S3 archive"
)
def parquet_ingestion():
    return spark.read.format("parquet").load("s3://your-bucket/path/to/archive/*.parquet")
