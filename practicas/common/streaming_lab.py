# Databricks notebook source
"""Utilidades del laboratorio demostrativo de streaming (clase 4).

Cada notebook trabaja en su propia carpeta del volumen y con sus propias tablas
`stream_*`. Al comenzar, el notebook reinicia SÓLO esos recursos, así puede
ejecutarse de nuevo desde el principio sin pasos manuales.
"""

import json


def lab_path(config, *parts):
    return "/".join([f"{config.volume_path}/streaming_lab", *parts])


def reset_lab(spark, dbutils, config, lab, tables):
    """Borra las tablas indicadas y la carpeta del laboratorio (datos y checkpoints)."""
    for name in tables:
        spark.sql(f"DROP TABLE IF EXISTS {qualified_table(config, name)}")
    try:
        dbutils.fs.rm(lab_path(config, lab), True)
    except Exception:
        pass
    dbutils.fs.mkdirs(lab_path(config, lab))


def publish_arrival(dbutils, config, lab, batch_id):
    """Simula al productor: deposita un archivo JSON Lines nuevo en la entrada."""
    rows = build_stream_rows(config, batch_id)
    path = lab_path(config, lab, "input", f"{batch_id}.jsonl")
    dbutils.fs.put(path, stream_rows_to_jsonl(rows), overwrite=False)
    return len(rows)


def _as_dict(progress):
    if isinstance(progress, dict):
        return progress
    return json.loads(progress.json)


def run_stream(stream_df, checkpoint, table=None, mode="append", foreach_batch=None):
    """Ejecuta una consulta con trigger AvailableNow: procesa lo pendiente y termina.

    Devuelve la lista de progresos (uno por microbatch) como diccionarios.
    """
    writer = (stream_df.writeStream.outputMode(mode)
              .option("checkpointLocation", checkpoint)
              .trigger(availableNow=True))
    if foreach_batch is not None:
        query = writer.foreachBatch(foreach_batch).start()
    else:
        query = writer.format("delta").toTable(table)
    query.awaitTermination()
    return [_as_dict(p) for p in query.recentProgress]


def input_rows(progresses):
    return sum(int(p.get("numInputRows", 0) or 0) for p in progresses)


def last_watermark(progresses):
    marks = [p.get("eventTime", {}).get("watermark") for p in progresses if p.get("eventTime")]
    marks = [m for m in marks if m]
    return marks[-1] if marks else None


def dropped_by_watermark(progresses):
    return sum(int(op.get("numRowsDroppedByWatermark", 0) or 0)
               for p in progresses for op in p.get("stateOperators", []))


def source_end_offset(progresses):
    """Offset final de la primera fuente (para Delta: versión de la tabla leída)."""
    for p in reversed(progresses):
        for source in p.get("sources", []):
            offset = source.get("endOffset")
            if isinstance(offset, str):
                try:
                    offset = json.loads(offset)
                except ValueError:
                    return offset
            if offset:
                return offset
    return None


# En serverless (Spark Connect) el progreso de una consulta terminada puede llegar
# vacío. Por eso las cantidades se miden en las tablas y el offset en el checkpoint.

def table_count(spark, table):
    return spark.table(table).count() if spark.catalog.tableExists(table) else 0


def rows_added(spark, table, run):
    """Ejecuta `run()` y devuelve cuántas filas nuevas aparecieron en `table`."""
    before = table_count(spark, table)
    run()
    return table_count(spark, table) - before


def checkpoint_offset(dbutils, checkpoint):
    """Lee del checkpoint el último offset confirmado de la fuente (para Delta, la versión)."""
    try:
        files = [f for f in dbutils.fs.ls(f"{checkpoint}/offsets") if f.name.rstrip("/").isdigit()]
    except Exception:
        return None
    if not files:
        return None
    last = max(files, key=lambda f: int(f.name.rstrip("/")))
    lines = [l for l in dbutils.fs.head(last.path, 65536).splitlines() if l.strip()]
    try:
        offset = json.loads(lines[-1])
    except (ValueError, IndexError):
        return None
    return offset.get("reservoirVersion", offset) if isinstance(offset, dict) else offset
