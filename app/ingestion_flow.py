#!/usr/bin/env python
# coding: utf-8

from datetime import timedelta

from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame, types
from pyspark.sql.functions import col, year, month, mean, median, udf
from prefect import flow, task

from spark_context import get_spark_session


@task
def read_azure_dataset(spark: SparkSession) -> DataFrame:
    # Azure storage access info
    blob_account_name = "azureopendatastorage"
    blob_container_name = "nyctlc"
    blob_relative_path = "yellow"
    blob_sas_token = "r"

    # Allow Spark to read from Blob remotely
    wasbs_path = "wasbs://%s@%s.blob.core.windows.net/%s" % (
        blob_container_name,
        blob_account_name,
        blob_relative_path,
    )
    spark.conf.set(
        "fs.azure.sas.%s.%s.blob.core.windows.net"
        % (blob_container_name, blob_account_name),
        blob_sas_token,
    )

    df = spark.read.parquet(wasbs_path)
    return df


@udf(types.StringType())
def get_payment_type(payment_type: str) -> str:
    payment_type_map = {
        "1": "Credit card",
        "2": "Cash",
        "3": "No charge",
        "4": "Dispute",
        "5": "Unknown",
        "6": "Voided trip",
    }
    return payment_type_map.get(payment_type.strip(), types.NullType())


@task
def clean(df: DataFrame) -> DataFrame:
    df = df.withColumn("paymentType", get_payment_type(df.paymentType))

    df = df.filter(
        (col("passengerCount") > 0)
        & (col("paymentType").isNotNull())
        & (col("puYear") >= 2009)
        & (col("puYear") <= 2018)
        & (col("fareAmount") > 0)
        & (col("totalAmount") > 0)
    )

    return df


@task
def aggregate(df: DataFrame) -> DataFrame:
    df_agg = (
        df.filter((col("passengerCount") > 0) & (col("paymentType").isNotNull()))
        .groupBy(
            col("paymentType"),
            col("puYear").alias("year"),
            col("puMonth").alias("month"),
        )
        .agg(
            mean("fareAmount").alias("meanFareAmount"),
            median("fareAmount").alias("medianFareAmount"),
            mean("totalAmount").alias("meanTotalAmount"),
            median("totalAmount").alias("medianTotalAmount"),
            mean("passengerCount").alias("meanPassengerCount"),
            median("passengerCount").alias("medianPassengerCount"),
        )
    )
    return df_agg


@task
def save_df(df: DataFrame, output_path: str) -> None:
    df.write.partitionBy("paymentType", "year", "month").parquet(
        output_path, mode="overwrite"
    )


@flow
def data_ingestion_flow():
    conf = (
        SparkConf()
        .setMaster("local[*]")
        .setAppName("TaxiDataProcessing")
        .set(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-azure:3.3.1,com.microsoft.azure:azure-storage:8.6.6",
        )
    )
    with get_spark_session(conf=conf) as spark:
        df = read_azure_dataset(spark)
        df_cleaned = clean(df)
        df_aggregated = aggregate(df_cleaned)
        save_df(df_aggregated, "./data/result")
