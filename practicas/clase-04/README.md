# Clase práctica 4 — Streaming: logs, ventanas y garantías

## Propósito

Ver funcionando los conceptos de la clase de streaming sobre la misma plataforma de e-commerce: un log con consumidores independientes, ingesta incremental de archivos, ventanas por tiempo del evento con datos tardíos, garantías de entrega y Change Data Capture.

La práctica es **demostrativa**: los notebooks no se modifican, sólo se ejecutan en orden. La entrega es un README con 20 preguntas cortas que se responden mirando los resultados.

Se usa **Structured Streaming en Databricks Free Edition** con tablas Delta y `trigger(availableNow=True)`: cada consulta procesa lo pendiente y termina, y su checkpoint recuerda hasta dónde llegó. No hace falta Kafka ni crear un Job.

## Requisitos previos

- Clase 2 ejecutada: se usan `silver_customers` y `silver_products` para enriquecer los eventos.
- Mismo `student_id` y misma escala que en las clases anteriores.
- Compute serverless. No hay que instalar librerías.
- Para importación manual, conservar `clase-04/` y `common/` como carpetas hermanas.

## Secuencia

Ejecutá los notebooks en orden, celda por celda. Cada uno reinicia sus propias tablas `stream_*` y su carpeta `streaming_lab/` al comenzar, así que se puede volver a correr desde el principio sin pasos manuales. Los notebooks 03 y 04 usan el resultado del 02.

| Notebook | Tema de la teoría | Qué se ve | Minutos |
|---|---|---|---:|
| `00_preflight.ipynb` | — | Verifica las tablas de la clase 2 y muestra los eventos del experimento | 5 |
| `01_log_y_offsets.ipynb` | Pub/Sub, log, offsets, fan-out, replay | Una tabla Delta como *topic*; dos consumidores a distinto ritmo; replay desde el inicio | 20 |
| `02_ingesta_y_enriquecimiento.ipynb` | Ingesta incremental, calidad, join stream-tabla | Cinco llegadas de archivos con Auto Loader; cuarentena; enriquecimiento con clientes y productos | 20 |
| `03_tiempo_y_ventanas.ipynb` | Event time, watermark, ventanas, tardíos | Watermark por llegada; `late_ok` aceptado y `late_bad` descartado; tumbling, hopping y session; append vs. update | 30 |
| `04_garantias_y_fallas.ipynb` | At-least-once, exactly-once, idempotencia | Duplicados, reinicio con checkpoint, checkpoint perdido y escritura idempotente con `MERGE` | 20 |
| `05_cdc.ipynb` | CDC, estado y stream, log compaction | Change Data Feed de una tabla de clientes y reconstrucción de la tabla desde el log | 15 |

## El experimento

Un productor ficticio envía compras del 12/03/2026 entre las 12:00 y las 12:45 (UTC) en cinco llegadas. Con escala `small` el bloque base se repite 20 veces con otros IDs y clientes.

| Llegada | Eventos del bloque base | Qué pasa |
|---|---|---|
| `stream_001` | Compras a 12:00, 12:01 (`e002`) y 12:04 | Todavía no se cierra ninguna ventana |
| `stream_002` | Compras a 12:06 y 12:08; `late_ok` (12:03); `e002` repetido; dos errores de calidad | Tardío tolerado, duplicado y cuarentena |
| `stream_003` | Compras a 12:25 y 12:26 | El watermark avanza y se cierran las primeras ventanas |
| `stream_004` | `late_bad` (12:02) y una compra a 12:27 | `late_bad` es más viejo que el watermark: se descarta |
| `stream_005` | Control a 12:45 (importe cero) | Hace avanzar el tiempo para cerrar la última ventana |

## Tablas que se crean

```text
01: stream_topic_payments, stream_consumer_fraud, stream_consumer_reports, stream_consumer_replay
02: stream_bronze_events, stream_silver_classified
03: stream_arrivals, stream_gold_tumbling, stream_gold_hopping, stream_gold_sessions, stream_gold_updates
04: stream_sink_naive, stream_sink_dedup, stream_sink_merge
05: stream_cdc_customers, stream_cdc_log
Volumen: landing/streaming_lab/<notebook>/ (archivos de entrada y checkpoints)
```

Ninguna tabla de las clases anteriores se modifica.

## Preguntas

Las preguntas son cortas: buscan comprobar que entendiste cada concepto y sus ventajas y desventajas. Cuando piden un dato, copiá el número que te dio el notebook (puede variar según tu escala). Alcanza con una a tres oraciones por respuesta.

### Log y offsets (`01_log_y_offsets`)

1. En el ejemplo en Python, ¿cuántos mensajes quedaron en la cola y cuántos en el log después de leer tres? ¿Qué diferencia hay entre una cola tradicional y un log como Kafka?
2. Después de la llegada 2, ¿cuántos mensajes había leído el consumidor `antifraude` y cuántos `reportes`? ¿El atraso de `reportes` afectó a `antifraude`? ¿Por qué?
3. ¿Qué es un offset y dónde lo guarda cada consumidor en esta práctica?
4. ¿Cuántos mensajes leyó el replay? ¿Qué propone la arquitectura Kappa para reprocesar datos y en qué se diferencia de Lambda?

### Ingesta y enriquecimiento (`02_ingesta_y_enriquecimiento`)

5. ¿Cuántas filas nuevas procesó Bronze en cada llegada y cuántas al reejecutar sin archivos nuevos? ¿Qué componente evita leer dos veces el mismo archivo?
6. ¿Qué motivos de rechazo aparecieron en la cuarentena y en qué llegada? ¿Por qué Bronze guarda el texto original en lugar de descartar los registros con errores?
7. ¿Qué columnas agregó el join con `silver_customers` y `silver_products`? ¿Qué tipo de join es (stream-stream, stream-tabla o tabla-tabla) según la teoría?

### Tiempo y ventanas (`03_tiempo_y_ventanas`)

8. ¿Qué diferencia hay entre event time y processing time? Usá `late_ok` como ejemplo.
9. ¿Cuánto valía el watermark al terminar cada llegada? ¿Cómo se calcula a partir del máximo event time visto?
10. ¿En qué llegada aparecieron las primeras ventanas tumbling en modo append y por qué no antes?
11. ¿Qué pasó con `late_bad`? ¿Por qué se aceptó `late_ok` y se descartó `late_bad`?
12. Mirando el gráfico de ventanas, ¿en qué se diferencian tumbling, hopping y session? Da un ejemplo de negocio para cada una.
13. Para la ventana [12:00, 12:05) del canal card, ¿cuántas veces y con qué valores se emitió en modo update y en modo append? ¿Qué ventaja y qué desventaja tiene cada modo?
14. ¿Qué se gana y qué se pierde si el watermark fuera de 1 minuto en lugar de 10?

### Garantías y fallas (`04_garantias_y_fallas`)

15. ¿Cuántas filas quedaron en el destino sin deduplicar y cuántas deduplicando por `event_id`? ¿Por qué el productor puede enviar el mismo evento dos veces?
16. ¿Qué pasó al reiniciar con el mismo checkpoint y qué pasó al perderlo? ¿Qué garantía de entrega se observa en cada caso?
17. ¿Por qué con `MERGE` el resultado no cambió aunque se perdiera el checkpoint? ¿Qué significa que una escritura sea idempotente?

### CDC (`05_cdc`)

18. ¿Cuántos eventos de cambio generó el log y de qué tipos? ¿Por qué un `UPDATE` genera dos eventos?
19. ¿Se pudo reconstruir la tabla a partir del log? ¿Qué información tiene el log que la tabla actual no tiene?

### Cierre

20. Según la comparación de motores de la teoría, ¿cuándo elegirías Spark Structured Streaming, Flink o Kafka Streams? Mencioná una ventaja de cada uno.

## Entrega

La publicación, el acceso público y el envío a los profesores siguen el [formato común de entrega](../README.md#formato-común-de-entrega).

La entrega es **un único `README.md`** con las respuestas a las 20 preguntas, dentro de `resolucion-practica-4/` en tu repositorio personal:

```text
resolucion-practica-4/
└── README.md
```

El README debe incluir nombre, `student_id` y escala. Podés partir de la [plantilla](PLANTILLA_ENTREGA.md). No hace falta exportar los notebooks.

No incluyas datos generados, archivos del volumen, checkpoints, credenciales ni tokens.

## Problemas frecuentes

- **`Falta ... silver_customers`**: ejecutá la clase 2 con el mismo `student_id` y escala.
- **`Ejecutá primero 02_ingesta_y_enriquecimiento`**: los notebooks 03 y 04 usan la tabla que crea el 02.
- **Una celda tarda**: cada consulta streaming arranca, procesa y termina; en serverless cada una puede tardar algunos segundos. No interrumpas la celda.
- **Querés empezar de nuevo**: volvé a ejecutar el notebook desde el principio; él mismo borra sus tablas y checkpoints.
