# Clase práctica 4 — Structured Streaming, ventanas y recuperación

## Propósito

Extender la plataforma ficticia de comercio electrónico de las clases 1 y 2 para procesar nuevas compras de forma incremental. El TP permite observar qué ocurre cuando los eventos llegan duplicados, atrasados o con problemas de calidad, y cómo se recupera el pipeline conservando sus checkpoints.

Se trabaja con **Structured Streaming en Databricks Free Edition**, usando Auto Loader, tablas Delta y `trigger(availableNow=True)`. Cada ejecución procesa lo disponible y termina; el estado persiste para la siguiente llegada. Es streaming por ejecuciones disparadas, con una fuente abierta a futuros archivos. La práctica no mide latencia en tiempo real ni requiere Kafka o una base externa.

## Objetivos

- Diferenciar procesamiento batch, streaming, microbatch, event time y processing time.
- Ingerir archivos nuevos con Auto Loader sin releerlos en cada ejecución.
- Aplicar contratos de calidad y enriquecimiento stream-static.
- Deduplicar por identidad del evento dentro de un horizonte temporal.
- Calcular ventanas de cinco minutos con un watermark de diez minutos.
- Observar estado, eventos descartados y ventanas finalizadas.
- Probar reejecución y recuperación con checkpoints persistentes.
- Comunicar resultados con consultas, visualizaciones y evidencia reproducible.

## Requisitos previos

Las clases 1 y 2 deben haberse ejecutado en el mismo catálogo y esquema personal. Usá el mismo `student_id` y la misma escala. El preflight comprueba `silver_customers` y `silver_products`, sus cantidades y la unicidad de sus claves.

La práctica congela las dimensiones en `stream_customers_snapshot` y `stream_products_snapshot` para mantener estable el enriquecimiento entre ejecuciones. No utiliza las tablas `nosql_*` de la clase 3.

Requisitos técnicos: cuenta Free Edition, Git folder con este repositorio, compute serverless y conocimientos de SQL/PySpark de las clases anteriores. No instalar bibliotecas. Para importación manual, conservar `clase-04/` y `common/` como carpetas hermanas e incluir los archivos de `common/` referenciados por `%run`.

## Arquitectura

```text
Productor manual → archivos JSON Lines inmutables en landing/streaming_lab/input
    → Auto Loader / checkpoint bronze
    → bronze_stream_events
    → parseo, contrato y joins stream-static / checkpoint quality
    → silver_stream_classified
        ├── silver_stream_quarantine (vista: calidad rechazada)
        └── silver_stream_valid (vista: calidad válida)
              → watermark + deduplicación / checkpoint dedup
              → silver_stream_events
              → ventanas + watermark / checkpoint gold
              → gold_stream_windows (ventanas finalizadas)
    → validación y stream_run_audit
```

Cada consulta tiene un checkpoint propio en `landing/streaming_lab/checkpoints/`. Las fuentes Delta reciben sólo append durante el experimento. La deduplicación y la agregación son consultas separadas para observar y recuperar sus estados individualmente.

El progreso se guarda en `stream_query_progress`: microbatch, entradas, watermark y operadores de estado. La auditoría registra una fila por validación exitosa, aunque las métricas de negocio no cambien en una reejecución.

## Secuencia de notebooks

| Notebook | Ejecución | Función |
|---|---|---|
| `00_preflight.ipynb` | Manual, una vez | Requisitos, snapshots, tablas y rutas |
| `00_generate_stream_batch.ipynb` | Manual, por llegada | Simular un productor externo |
| `01_ingest_bronze_stream.ipynb` | Job | Auto Loader → Bronze |
| `02_quality_enrichment.ipynb` | Job | Clasificación y joins estáticos |
| `03_deduplicate_stream.ipynb` | Job | Watermark y deduplicación por event_id |
| `04_windowed_gold.ipynb` | Job | Agregaciones por ventana y canal |
| `05_validate_stream.ipynb` | Job | Validación, progreso e idempotencia |
| `06_visualizacion.ipynb` | Manual, al finalizar | Cuatro preguntas y gráficos |
| `07_desafio.ipynb` | Manual, al finalizar | Referencia batch y replay aislado |

Prepará el Job siguiendo la [guía de creación](GUIA_CREAR_JOB.md). Las tareas deben ejecutarse en secuencia y con una sola ejecución simultánea. El generador, la visualización y el desafío quedan fuera del Job.

## Experimento de cinco llegadas

**Publicá un solo lote y ejecutá/validá el Job antes de publicar el siguiente.** Este orden crea la experiencia temporal que queremos analizar; cargar todos los archivos juntos cambia el experimento.

Todos los event times son del **12 de marzo de 2026, en UTC**. La fecha es fija y didáctica: el watermark usa esos tiempos de evento y no la fecha actual de la computadora.

| Etapa | Eventos del bloque base | Qué observar |
|---|---|---|
| `stream_001` | Tres compras a 12:00, 12:01 y 12:04 | Bronze y Silver crecen; Gold todavía no emite ventanas |
| `stream_002` | Compras a 12:06 y 12:08; `late_ok` a 12:03; repetición de `e002`; importe N/A y cliente desconocido | Atraso tolerado, duplicado y dos rechazos de calidad |
| `stream_003` | Compras a 12:25 y 12:26 | Avanza el tiempo observado y se finalizan las primeras ventanas |
| `stream_004` | `late_bad` a 12:02 y una compra a 12:27 | El evento demasiado tardío es válido por contrato, pero no pasa dedup |
| `stream_005` | Control a 12:45, con importe cero | Permite finalizar la ventana de negocio [12:25, 12:30) |

Los IDs visibles incluyen un sufijo de bloque: `e002_000`, `late_ok_000`, `late_bad_000`. El duplicado mantiene los mismos campos de negocio, pero tiene otro `source_batch_id`.

El control de 005 pertenece al experimento: avanza el tiempo observado, no representa una compra. En Gold se conserva hasta aplicar el watermark, sus métricas son cero y se omite su resultado agregado. En un sistema real, una señal así requiere un contrato confiable; timestamps futuros erróneos pueden cerrar estado prematuramente.

### Procedimiento

1. Ejecutá `00_preflight` y verificá escala, esquema y rutas.
2. Generá `stream_001` y ejecutá el Job con `expected_batch_id=stream_001`.
3. Repetí el mismo Job sin generar un archivo; registrá la estabilidad de métricas y `idempotence_compared=True`.
4. Generá `stream_002`; ejecutá y validá el Job con esa expectativa.
5. Generá y procesá `stream_003` antes de continuar. Revisá las ventanas ya emitidas.
6. Generá y procesá `stream_004`. Seguí `late_bad_000` por las capas.
7. Generá y procesá `stream_005`. Revisá las últimas ventanas de negocio.
8. Repetí el Job con `stream_005` sin nuevos archivos y conservá la segunda prueba de idempotencia.
9. Ejecutá `06_visualizacion` y resolvé las cuatro preguntas.
10. Resolvé `07_desafio`, seleccioná `validar_entrega=si` y ejecutá su control final.

El productor impide sobrescribir archivos o adelantar una etapa sin una validación exitosa de la precedente. Si el Job falla, **se repite con el mismo archivo y los mismos checkpoints**. No hay que publicar otro lote para reparar una ejecución.

### Qué significa `expected_batch_id`

El flujo no utiliza este parámetro para filtrar archivos ni elegir datos. Auto Loader y los checkpoints determinan el trabajo pendiente. `expected_batch_id` indica qué estado acumulado espera verificar `05_validate_stream`. Si lo dejás en una etapa anterior, la expectativa puede fallar aunque la ingesta haya recibido correctamente el archivo nuevo.

## Escalas y contratos

Cada bloque representa el mismo escenario con IDs y clientes distintos. `test` genera un bloque; `small`, 20; `demo`, 100. La escala debe coincidir con las dimensiones de clases 1 y 2, aunque los volúmenes del TP sean mucho menores. No se trata de un benchmark de rendimiento.

El experimento completo recibe 14 entradas por bloque. El contrato separa dos errores de calidad, conserva los eventos válidos con duplicados y luego aplica deduplicación temporal. El evento demasiado tardío permanece trazable en Bronze y Silver valid: no se mueve automáticamente a cuarentena.

Gold agrupa por `(window_start, window_end, payment_channel)` y presenta sólo ventanas finalizadas. Sus conteos e importes no deben reconciliarse con todas las filas físicas de Bronze. Los campos `is_fraud` son etiquetas sintéticas del generador, no resultados de un detector real; los montos están expresados en unidades monetarias ficticias.

Los controles comprueban conservación de filas en la clasificación, motivos de rechazo, IDs, atrasos, ventanas, duración, importes y estabilidad de la reejecución. El TP no exige un número fijo de microbatches: el runtime puede dividir la ejecución de distintas maneras.

## Preguntas de análisis y comprensión

Respondé después de completar las cinco etapas. Para análisis de datos, incluí **consulta SQL o PySpark, resultado relevante y explicación**. Para preguntas sobre código, indicá **notebook o helper y sección**. No alcanza con copiar una salida sin interpretarla.

### Análisis de datos y ejecuciones

1. ¿Cuántas filas físicas ingresaron por `source_batch_id` a Bronze? ¿Cuántos archivos distintos hay por lote? Mostrá una consulta y explicá por qué filas y archivos son unidades diferentes.
2. ¿Cómo se reconcilian entradas clasificadas, válidos y cuarentena? Mostrá el resultado por lote e identificá los dos motivos de rechazo introducidos por el productor.
3. Seguí `e002_000` por Bronze, Silver valid y Silver deduplicada. ¿Cuántas apariciones hay en cada capa y qué campos comparten o cambian?
4. Seguí `late_ok_000`: ¿en qué llegada aparece, cuál es su event time y en qué ventana/canal contribuye? Explicá por qué se acepta aunque llegue fuera de orden.
5. Seguí `late_bad_000`: ¿dónde permanece y dónde falta? Usá la evidencia del avance temporal de 003 y el progreso de dedup para explicar la exclusión.
6. ¿En qué etapas Gold está vacía, cuándo aparecen sus primeras ventanas y cuándo se finaliza [12:25, 12:30)? Sustentá la respuesta con auditorías y contenido de Gold.
7. ¿Qué ventana presenta más compras y cuál el mayor monto? ¿Coinciden? Recalculá el ticket promedio por ventana.
8. ¿Qué canal tiene la mayor tasa global de fraude sintético? Calculá `SUM(fraud_count)/SUM(event_count)` y mostrá también el denominador; explicá por qué no promediar tasas por ventana.
9. Compará los dos pares de ejecuciones sin archivos nuevos —001 y 005—. ¿Qué métricas permanecen iguales, qué evidencia nueva se agrega y cómo se identifica la comparación de idempotencia?
10. Compará la referencia batch del desafío con Gold streaming. ¿En qué ventana/canal difieren, por cuántas compras y por qué importe? Identificá el evento responsable.

### Interpretación de código y diseño

11. ¿Qué convierte este flujo en Structured Streaming aunque use `AvailableNow` y termine? Distinguí trigger, microbatch, lote del productor y ventana de event time.
12. ¿Qué conservan los checkpoints y por qué cada consulta usa una ruta distinta? Explicá qué ocurre en las tres ejecuciones del replay del desafío.
13. ¿Qué diferencias tienen `event_ts` e `ingested_at`? ¿A qué ventana pertenece un evento exactamente a 12:05 y por qué usamos intervalos [inicio, fin)?
14. ¿Cómo se relaciona el watermark de diez minutos con el máximo event time observado? ¿Por qué esperar tiempo de reloj sin nuevas entradas no finaliza por sí mismo todas las ventanas?
15. ¿Por qué leer los JSON Lines como texto en Bronze y aplicar `from_json` y `try_cast` en Silver? ¿Qué información conserva un registro rechazado?
16. ¿Qué tipo de joins usa el enriquecimiento y por qué se congelan las dimensiones? ¿Qué riesgo tendríamos al cambiar una dimensión entre intentos del mismo procesamiento?
17. ¿Por qué deduplicar por `event_id` en lugar de por todas las columnas? ¿Qué garantías y límites tiene `dropDuplicatesWithinWatermark` frente a una restricción de unicidad histórica?
18. ¿Por qué usamos append para Gold? Compará qué se observaría en append, update y complete y explicá qué modos admite la escritura directa a Delta.
19. ¿Qué función tiene el control de 005? ¿Qué sucedería si se filtrara antes del watermark? ¿Qué problema podría provocar un timestamp futuro incorrecto en producción?
20. Si falla la tarea de Gold después de completarse Bronze y Silver, ¿qué se debe repetir y conservar? Explicá por qué cambiar el checkpoint, modificar el grano de estado o sobrescribir una fuente Delta requiere un plan de recuperación distinto.

## Visualizaciones y desafío

`06_visualizacion` sigue el formato de clase 2: un ejemplo resuelto y cuatro consignas. Cada gráfico debe tener título, ejes, unidades y una respuesta explícita. Debe ser evidencia para responder una pregunta, no una captura decorativa.

`07_desafio` pide reconstruir el histórico válido en batch, comparar semánticas y ejecutar un replay aislado. El punto A tiene un TODO; el replay incluye código para tres ejecuciones y aserciones. El control verifica columnas y tipos, inicio y fin de las ventanas y todas las métricas por canal, incluido el conteo de fraude. `validar_entrega=no` permite recorrer el notebook, pero sólo `si` ejecuta su control final. Este control de entrega es distinto de los checkpoints que conservan el estado de las consultas. El plan de recuperación y las respuestas conceptuales se evalúan por separado.

## Duración estimada

| Bloque | Minutos |
|---|---:|
| Repaso, preflight y arquitectura | 15 |
| Bronze y calidad/enriquecimiento | 25 |
| Deduplicación y ventanas | 30 |
| Creación del Job | 20 |
| Cinco llegadas y reejecuciones | 35 |
| Pausa | 10 |
| Visualización orientada a preguntas | 25 |
| Desafío, recuperación y cierre | 30 |
| Total | 190 |

Los tiempos del entorno y sus cuotas pueden alterar la duración. Las 20 preguntas pueden completarse después del encuentro.

## Formato de entrega

Usá el repositorio personal `mi-primer-proyecto` de la [guía de Git y GitHub](../GUIA_GIT_GITHUB.md). Creá este directorio en su raíz:

```text
resolucion-practica-4/
├── README.md
├── 01_ingest_bronze_stream.ipynb
├── 02_quality_enrichment.ipynb
├── 03_deduplicate_stream.ipynb
├── 04_windowed_gold.ipynb
├── 05_validate_stream.ipynb
├── 06_visualizacion.ipynb
├── 07_desafio.ipynb
└── evidencias/
    ├── dag_job.png
    └── ...capturas de ejecuciones y gráficos...
```

Podés usar [PLANTILLA_ENTREGA.md](PLANTILLA_ENTREGA.md) como base del README. Debe contener:

- Nombre, `student_id`, escala, catálogo/esquema y nombre exacto o URL del Job.
- Captura del DAG con sus cinco tareas y parámetros usados.
- Tabla de las cinco etapas, con ejecución, cantidades por capa y ventanas finalizadas.
- Evidencia de las reejecuciones de 001 y 005 sin archivos nuevos, con `idempotence_compared=True`.
- Seguimiento de e002, late_ok y late_bad, con consultas y resultados.
- Las cuatro visualizaciones, o capturas legibles, y sus respuestas explícitas.
- Comparación batch/stream, cantidades del replay y plan breve de recuperación.
- Respuestas a las 20 preguntas, identificadas P01–P20.

### Exportar y publicar

1. Ejecutá cada notebook correspondiente para conservar sus salidas. En los notebooks del Job, exportá la ejecución o abrí el resultado de la tarea; comprobá que el `.ipynb` descargado incluya las salidas. **No reejecutes el Job con una expectativa antigua sólo para exportarlo.**
2. Usá **File → Export → IPython Notebook** cuando esté disponible en el resultado/notebook. Si la interfaz sólo permite exportar el código fuente, adjuntá además capturas de la ejecución y sus controles.
3. Copiá los archivos a `resolucion-practica-4/` y completá el README.
4. Publicá desde tu repositorio:

```bash
git add resolucion-practica-4
git commit -m "Entrega práctica 4 - Streaming"
git push origin main
```

Verificá en una ventana privada que el repositorio público y la carpeta son accesibles. Enviá la URL del repositorio a **dabadie@itba.edu.ar** y **ghenrion@itba.edu.ar**, como en la práctica 1.

No incluyas tokens, credenciales, archivos del volumen, checkpoints, dumps completos de tablas ni información personal ajena. Los datos del TP se reconstruyen a partir del productor y las evidencias necesarias son sus resultados y controles.

## Evaluación

| Criterio | Peso |
|---|---:|
| Pipeline, Job y ejecución ordenada con checkpoints | 25% |
| Consultas, reconciliación y análisis de datos tardíos | 25% |
| Visualizaciones y comparación batch/stream | 20% |
| Interpretación de código y recuperación | 20% |
| Evidencia y formato de entrega | 10% |

## Reejecución y problemas frecuentes

- El preflight crea recursos faltantes; no reinicia el experimento. Mantener la escala y duraciones originales.
- No usar `processingTime`, `continuous`, el trigger por defecto ni un sink memory en esta práctica serverless.
- Si una tarea falla, abrir su error y reejecutar el Job con el mismo archivo/checkpoints. Los resultados de tareas ya completadas se retoman incrementalmente.
- Si Gold está vacía después de 001/002, es esperado. Si sigue vacía después de 003, revisar orden de llegadas y progreso del watermark.
- Si el conteo coincide pero los importes fallan, revisar el duplicado, el filtro de controles y el evento demasiado tardío.
- No borrar un checkpoint dejando el destino append como si fuera una reejecución normal: el desafío muestra cómo produce reprocesamiento.
- No cambiar el esquema de estado, las claves o el grano dentro del experimento existente. Para un experimento nuevo usar destinos y checkpoints nuevos; acordar cualquier reinicio integral con el docente.
- Free Edition puede alcanzar cuotas. Las consultas terminan automáticamente; no dejar procesos abiertos ni aumentar escala para reparar errores.

## Referencias oficiales y verificación

Revisadas el **7 de octubre de 2026**. La ejecución en el workspace confirma la compatibilidad efectiva del entorno.

- [Streaming en serverless](https://docs.databricks.com/aws/en/compute/serverless/streaming) y [triggers](https://docs.databricks.com/aws/en/structured-streaming/triggers).
- [Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/).
- [Checkpoints](https://docs.databricks.com/aws/en/structured-streaming/checkpoints).
- [Watermarks](https://docs.databricks.com/aws/en/structured-streaming/watermarks) y [dropDuplicatesWithinWatermark](https://docs.databricks.com/aws/en/pyspark/reference/classes/dataframe/dropDuplicatesWithinWatermark).
- [Fuentes y destinos Delta](https://docs.databricks.com/aws/en/structured-streaming/delta-lake) y [modos de salida](https://docs.databricks.com/aws/en/structured-streaming/output-mode).
- [Guía de Apache Spark](https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html): semántica de AvailableNow y microbatches sin entradas para avanzar el estado.

El [material docente](docente/README.md) incluye resultados esperados, guía de respuestas y solución del desafío. Los tests locales verifican fixtures y estructura del material; no sustituyen la ejecución de Structured Streaming en Databricks.
