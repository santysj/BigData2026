# Databricks notebook source
"""Llegadas controladas de eventos para el TP de Structured Streaming."""

import json
import re


STREAM_REPETITIONS = {"test": 1, "small": 20, "demo": 100}
STREAM_BATCHES = tuple(f"stream_{index:03d}" for index in range(1, 6))


def normalize_stream_batch(value):
    value = value.strip().lower()
    if value not in STREAM_BATCHES:
        raise ValueError(f"Elegí uno de {STREAM_BATCHES}")
    return value


def build_stream_rows(config, batch_id):
    """Crea eventos inmutables; customer/product_id remiten a las clases previas.

    Cada bloque conserva tiempos e importes, con claves propias. El productor
    representa compras (importe positivo) y un control de tiempo (importe cero).
    """
    batch_id = normalize_stream_batch(batch_id)
    if config.scale not in STREAM_REPETITIONS:
        raise ValueError("Escala de streaming inválida")
    rows = []
    for block in range(STREAM_REPETITIONS[config.scale]):
        def event(key, minute, amount, channel="card", customer=0, product=0, fraud=0, kind="purchase"):
            return {
                "event_id": f"{key}_{block:03d}",
                "event_ts": f"2026-03-12T12:{minute:02d}:00Z",
                "customer_id": str(block * 2 + customer),
                "product_id": str(product),
                "amount": str(amount),
                "payment_channel": channel,
                "is_fraud": str(fraud),
                "event_type": kind,
                "source_batch_id": batch_id,
            }

        if batch_id == "stream_001":
            rows.extend([
                event("e001", 0, 100),
                event("e002", 1, 200, customer=1, product=1, fraud=1),
                event("e003", 4, 50, "wallet", product=1),
            ])
        elif batch_id == "stream_002":
            unknown = event("bad_customer", 7, 90)
            unknown["customer_id"] = str(config.rows["customers"] + 999)
            rows.extend([
                event("e004", 6, 300, customer=1, fraud=1),
                event("e005", 8, 80, "wallet"),
                event("late_ok", 3, 120),
                event("e002", 1, 200, customer=1, product=1, fraud=1),
                event("bad_amount", 7, "N/A"),
                unknown,
            ])
        elif batch_id == "stream_003":
            rows.extend([event("e006", 25, 200), event("e007", 26, 150, "wallet")])
        elif batch_id == "stream_004":
            rows.extend([event("late_bad", 2, 999), event("e008", 27, 75)])
        else:
            rows.append(event("clock", 45, 0, "control", kind="control"))
    return rows


def stream_rows_to_jsonl(rows):
    if not rows:
        raise ValueError("El lote no puede estar vacío")
    return "\n".join(json.dumps(row, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
                     for row in rows) + "\n"


def stream_input_directory(config):
    return f"{config.volume_path}/streaming_lab/input"


def published_stream_batches(dbutils, config):
    """Sólo reconoce los cinco nombres del experimento; no oculta archivos extra."""
    paths = dbutils.fs.ls(stream_input_directory(config))
    names = [item.name for item in paths if not item.isDir()]
    unexpected = [name for name in names if not re.fullmatch(r"stream_00[1-5]\.jsonl", name)]
    if unexpected:
        raise ValueError(f"Archivos ajenos al experimento: {unexpected}")
    return sorted(name.removesuffix(".jsonl") for name in names)


def publish_stream_batch(dbutils, spark, config, batch_id):
    """Publica un archivo nuevo después de validar la llegada precedente.

    No sobrescribe archivos. El archivo y el Job son acciones independientes:
    si el Job falla, se reejecuta con el mismo archivo y los mismos checkpoints.
    """
    batch_id = normalize_stream_batch(batch_id)
    expected_before = list(STREAM_BATCHES[:STREAM_BATCHES.index(batch_id)])
    actual_before = published_stream_batches(dbutils, config)
    if batch_id in actual_before:
        raise FileExistsError("El archivo ya existe. Reejecutá el Job; no vuelvas a generar este lote.")
    if actual_before != expected_before:
        raise ValueError(f"Antes de {batch_id} deben existir exactamente {expected_before}")
    if expected_before:
        prior = expected_before[-1]
        audit = f"{config.namespace}.`stream_run_audit`"
        if spark.table(audit).where(f"expected_batch_id = '{prior}'").count() == 0:
            raise ValueError(f"Validá el Job para {prior} antes de publicar {batch_id}")
    rows = build_stream_rows(config, batch_id)
    path = f"{stream_input_directory(config)}/{batch_id}.jsonl"
    dbutils.fs.put(path, stream_rows_to_jsonl(rows), overwrite=False)
    return {"batch_id": batch_id, "physical_rows": len(rows), "path": path}
