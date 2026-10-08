# Databricks notebook source
"""Contratos de Silver streaming: todo rechazo conserva la entrada original."""


STREAM_EVENT_JSON_SCHEMA = (
    "event_id STRING, event_ts STRING, customer_id STRING, product_id STRING, "
    "amount STRING, payment_channel STRING, is_fraud STRING, event_type STRING, "
    "source_batch_id STRING, _corrupt_record STRING"
)


def classify_stream_events(raw, customers, products):
    """Transformación sin estado: parseo, tipos, joins estáticos y motivo de calidad."""
    from pyspark.sql import functions as F

    typed = (raw.withColumn("payload", F.from_json("raw_json", STREAM_EVENT_JSON_SCHEMA,
                                                  {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"}))
             .withColumn("event_id", F.col("payload.event_id"))
             .withColumn("event_ts", F.expr("try_cast(payload.event_ts AS TIMESTAMP)"))
             .withColumn("customer_id", F.expr("try_cast(payload.customer_id AS BIGINT)"))
             .withColumn("product_id", F.expr("try_cast(payload.product_id AS BIGINT)"))
             .withColumn("amount", F.expr("try_cast(payload.amount AS DECIMAL(12,2))"))
             .withColumn("payment_channel", F.col("payload.payment_channel"))
             .withColumn("is_fraud", F.expr("try_cast(payload.is_fraud AS INT)"))
             .withColumn("event_type", F.col("payload.event_type"))
             .withColumn("source_batch_id", F.col("payload.source_batch_id")))
    dimension_customers = customers.select(F.col("customer_id").alias("known_customer"), "country")
    dimension_products = products.select(F.col("product_id").alias("known_product"), "category")
    joined = (typed.join(dimension_customers, typed.customer_id == dimension_customers.known_customer, "left")
              .join(dimension_products, typed.product_id == dimension_products.known_product, "left"))
    reason = (F.when(F.col("payload").isNull() | F.col("payload._corrupt_record").isNotNull(), "INVALID_JSON")
              .when(F.col("event_id").isNull() | (F.trim("event_id") == ""), "INVALID_EVENT_ID")
              .when(F.col("event_ts").isNull(), "INVALID_EVENT_TS")
              .when(F.col("customer_id").isNull(), "INVALID_CUSTOMER_ID")
              .when(F.col("product_id").isNull(), "INVALID_PRODUCT_ID")
              .when(F.col("event_type").isNull() | ~F.col("event_type").isin("purchase", "control"), "INVALID_EVENT_TYPE")
              .when(F.col("amount").isNull() | (F.col("amount") < 0)
                    | ((F.col("event_type") == "purchase") & (F.col("amount") == 0))
                    | ((F.col("event_type") == "control") & (F.col("amount") != 0)), "INVALID_AMOUNT")
              .when(F.col("payment_channel").isNull()
                    | ((F.col("event_type") == "purchase") & ~F.col("payment_channel").isin("card", "wallet", "transfer"))
                    | ((F.col("event_type") == "control") & (F.col("payment_channel") != "control")), "INVALID_PAYMENT_CHANNEL")
              .when(F.col("is_fraud").isNull() | ~F.col("is_fraud").isin(0, 1)
                    | ((F.col("event_type") == "control") & (F.col("is_fraud") != 0)), "INVALID_FRAUD_FLAG")
              .when(F.col("source_batch_id").isNull()
                    | (F.col("source_batch_id") != F.regexp_extract("source_file", r"(stream_00[1-5])\.jsonl$", 1)), "SOURCE_BATCH_MISMATCH")
              .when(F.col("known_customer").isNull(), "UNKNOWN_CUSTOMER")
              .when(F.col("known_product").isNull(), "UNKNOWN_PRODUCT"))
    return joined.withColumn("quality_reason", reason).select(
        "raw_json", "source_file", "ingested_at", "event_id", "event_ts", "customer_id", "product_id",
        "amount", "payment_channel", "is_fraud", "event_type", "source_batch_id", "country", "category", "quality_reason")
