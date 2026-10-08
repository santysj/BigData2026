# Databricks notebook source
"""Contrato del experimento por etapas, independiente del número de microbatches."""


STREAM_STAGE_COUNTS = {
    "stream_001": {"bronze": 3, "classified": 3, "valid": 3, "quarantine": 0, "unique": 3, "gold": 0},
    "stream_002": {"bronze": 9, "classified": 9, "valid": 7, "quarantine": 2, "unique": 6, "gold": 0},
    "stream_003": {"bronze": 11, "classified": 11, "valid": 9, "quarantine": 2, "unique": 8, "gold": 4},
    "stream_004": {"bronze": 13, "classified": 13, "valid": 11, "quarantine": 2, "unique": 9, "gold": 4},
    "stream_005": {"bronze": 14, "classified": 14, "valid": 12, "quarantine": 2, "unique": 10, "gold": 6},
}


def expected_stream_counts(batch_id, repetitions):
    if batch_id not in STREAM_STAGE_COUNTS:
        raise ValueError("Etapa inválida")
    return {key: count * (1 if key == "gold" else repetitions)
            for key, count in STREAM_STAGE_COUNTS[batch_id].items()}


def expected_gold_rows(batch_id, repetitions):
    """Grano (window_start, payment_channel); todos los tiempos son UTC."""
    if batch_id not in STREAM_STAGE_COUNTS:
        raise ValueError("Etapa inválida")
    rows = []
    if batch_id >= "stream_003":
        rows.extend([(0, "card", 3, "420.00", 1), (0, "wallet", 1, "50.00", 0),
                     (5, "card", 1, "300.00", 1), (5, "wallet", 1, "80.00", 0)])
    if batch_id == "stream_005":
        rows.extend([(25, "card", 2, "275.00", 0), (25, "wallet", 1, "150.00", 0)])
    from decimal import Decimal
    return {(f"2026-03-12 12:{minute:02d}:00", channel):
            (count * repetitions, Decimal(amount) * repetitions, fraud * repetitions)
            for minute, channel, count, amount, fraud in rows}
