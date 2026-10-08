# Databricks notebook source
"""Rutas, preflight y observabilidad del TP; no cambia ni borra checkpoints."""

import json
import uuid


STREAM_TABLE_NAMES = (
    "bronze_stream_events", "silver_stream_classified", "silver_stream_events",
    "stream_customers_snapshot", "stream_products_snapshot", "stream_lab_settings",
    "gold_stream_windows", "stream_query_progress", "stream_run_audit",
)


def stream_checkpoint(config, stage):
    if stage not in ("bronze", "quality", "dedup", "gold"):
        raise ValueError("Etapa de streaming inválida")
    return f"{config.volume_path}/streaming_lab/checkpoints/{stage}"


def initialize_stream_lab(spark, dbutils, config, repetitions):
    """Crea recursos faltantes y congela dimensiones para repetir el experimento."""
    from pyspark.sql import functions as F

    for name, expected in [("silver_customers", config.rows["customers"]),
                           ("silver_products", config.rows["products"])]:
        table = f"{config.namespace}.`{name}`"
        if not spark.catalog.tableExists(table):
            raise ValueError(f"Falta {table}. Ejecutá clases 1 y 2 primero.")
        frame = spark.table(table)
        if frame.count() != expected:
            raise ValueError(f"La escala no coincide con {name}")
        key = "customer_id" if name == "silver_customers" else "product_id"
        if frame.select(key).distinct().count() != expected:
            raise ValueError(f"La dimensión {name} contiene claves duplicadas")

    settings = f"{config.namespace}.`stream_lab_settings`"
    if spark.catalog.tableExists(settings):
        rows = spark.table(settings).collect()
        if len(rows) != 1 or rows[0].scale != config.scale or rows[0].watermark != "10 minutes" or rows[0].window_duration != "5 minutes":
            raise ValueError("La configuración difiere del experimento existente. Conservá escala y duraciones.")
    else:
        (spark.range(1).select(F.lit(config.scale).alias("scale"),
                              F.lit("10 minutes").alias("watermark"),
                              F.lit("5 minutes").alias("window_duration"),
                              F.lit(repetitions).alias("repetitions"),
                              F.current_timestamp().alias("created_at"))
         .write.format("delta").mode("errorifexists").saveAsTable(settings))

    for source, target in [("silver_customers", "stream_customers_snapshot"),
                           ("silver_products", "stream_products_snapshot")]:
        spark.sql(f"CREATE TABLE IF NOT EXISTS {config.namespace}.`{target}` USING DELTA AS SELECT * FROM {config.namespace}.`{source}`")
    definitions = {
        "bronze_stream_events": "raw_json STRING, source_file STRING, ingested_at TIMESTAMP",
        "silver_stream_classified": "raw_json STRING, source_file STRING, ingested_at TIMESTAMP, event_id STRING, event_ts TIMESTAMP, customer_id BIGINT, product_id BIGINT, amount DECIMAL(12,2), payment_channel STRING, is_fraud INT, event_type STRING, source_batch_id STRING, country STRING, category STRING, quality_reason STRING",
        "silver_stream_events": "event_id STRING, event_ts TIMESTAMP, customer_id BIGINT, product_id BIGINT, amount DECIMAL(12,2), payment_channel STRING, is_fraud INT, event_type STRING, source_batch_id STRING, country STRING, category STRING",
        "gold_stream_windows": "window_start TIMESTAMP, window_end TIMESTAMP, payment_channel STRING, event_count BIGINT, total_amount DECIMAL(18,2), fraud_count BIGINT",
        "stream_query_progress": "attempt_id STRING, job_run_id STRING, stage STRING, progress_json STRING, recorded_at TIMESTAMP",
        "stream_run_audit": "validation_id STRING, job_run_id STRING, expected_batch_id STRING, metrics_json STRING, idempotence_compared BOOLEAN, recorded_at TIMESTAMP",
    }
    for name, definition in definitions.items():
        spark.sql(f"CREATE TABLE IF NOT EXISTS {config.namespace}.`{name}` ({definition}) USING DELTA")
    spark.sql(f"CREATE OR REPLACE VIEW {config.namespace}.`silver_stream_quarantine` AS SELECT * FROM {config.namespace}.`silver_stream_classified` WHERE quality_reason IS NOT NULL")
    spark.sql(f"CREATE OR REPLACE VIEW {config.namespace}.`silver_stream_valid` AS SELECT * FROM {config.namespace}.`silver_stream_classified` WHERE quality_reason IS NULL")
    dbutils.fs.mkdirs(f"{config.volume_path}/streaming_lab/input")


def require_stream_lab(spark, config):
    missing = [name for name in STREAM_TABLE_NAMES
               if not spark.catalog.tableExists(f"{config.namespace}.`{name}`")]
    if missing:
        raise ValueError(f"Faltan {missing}; ejecutá 00_preflight.")
    settings = spark.table(f"{config.namespace}.`stream_lab_settings`").collect()
    if len(settings) != 1 or settings[0].scale != config.scale:
        raise ValueError("La escala no coincide con 00_preflight")
    spark.sql(f"USE CATALOG `{config.catalog}`")
    spark.sql(f"USE SCHEMA `{config.schema}`")
    spark.conf.set("spark.sql.session.timeZone", "UTC")


def wait_and_record_stream(spark, config, query, stage, job_run_id):
    """Espera en intervalos y persiste progreso; falla si la consulta no termina."""
    from pyspark.sql import functions as F

    try:
        for attempt in range(10):
            if query.awaitTermination(60):
                break
            print(f"{stage}: sigue procesando, espera {attempt + 1}/10")
        if query.isActive:
            query.stop()
            raise TimeoutError("La consulta superó 10 minutos. Revisá su progreso y reejecutá con el mismo checkpoint.")
        progress = query.recentProgress
    except Exception:
        if query.isActive:
            query.stop()
        raise
    attempt_id = str(uuid.uuid4())
    records = []
    for update in progress:
        if isinstance(update, dict):
            payload = update
        else:
            payload = json.loads(update.json)
        records.append((attempt_id, job_run_id, stage, json.dumps(payload, default=str)))
        print(json.dumps({key: payload.get(key) for key in
                          ["batchId", "numInputRows", "eventTime", "stateOperators"]}, default=str))
    if records:
        (spark.createDataFrame(records, "attempt_id STRING, job_run_id STRING, stage STRING, progress_json STRING")
         .withColumn("recorded_at", F.current_timestamp()).write.format("delta").mode("append")
         .saveAsTable(f"{config.namespace}.`stream_query_progress`"))
    print(f"{stage}: finalizó; actualizaciones de progreso = {len(records)}")
