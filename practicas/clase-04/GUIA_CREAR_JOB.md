# Guía — Lakeflow Job del TP de streaming

El productor simula un sistema externo y se ejecuta manualmente. El Job procesa cada llegada con cinco tareas dependientes y luego termina. Configurarlo una sola vez; las cinco etapas utilizan el mismo Job y los mismos checkpoints.

## 1. Crear el Job

1. Abrí **Jobs & Pipelines → Create → Job**.
2. Nombralo `bigdata_<student_id>_streaming`.
3. Usá **Serverless** en todas las tareas, sin librerías adicionales.
4. En las opciones de ejecución, fijá **Maximum concurrent runs = 1**. No iniciar dos ejecuciones sobre los mismos checkpoints al mismo tiempo.
5. Ejecutalo manualmente durante el TP; no hace falta programar una periodicidad.

## 2. Parámetros

| Parámetro | Valor |
|---|---|
| `student_id` | El de clases 1 y 2 |
| `scale` | La misma escala de esas clases |
| `expected_batch_id` | `stream_001` para comenzar |
| `job_run_id` | `{{job.run_id}}` |

Usá **Job details → Edit parameters** o la sección equivalente de parámetros del Job. Los parámetros se propagan a las tareas de notebook. Comprobá sus valores en el detalle de cada ejecución.

## 3. Tareas y dependencias

| Task name | Notebook | Depends on |
|---|---|---|
| `ingest_bronze` | `01_ingest_bronze_stream.ipynb` | — |
| `quality_enrichment` | `02_quality_enrichment.ipynb` | `ingest_bronze` |
| `dedup_events` | `03_deduplicate_stream.ipynb` | `quality_enrichment` |
| `windowed_gold` | `04_windowed_gold.ipynb` | `dedup_events` |
| `validate` | `05_validate_stream.ipynb` | `windowed_gold` |

En cada tarea, elegí **Notebook**, seleccioná el archivo dentro del Git folder y confirmá serverless. El DAG debe ser una línea de cinco nodos. Guardá su captura para la entrega.

No agregar el preflight, el productor, la visualización ni el desafío como tareas. Las consultas ya usan AvailableNow y checkpoints explícitos: cada tarea espera su finalización antes de habilitar la siguiente.

## 4. Ejecutar el experimento

1. Ejecutá el preflight manualmente.
2. Generá sólo `stream_001`.
3. Usá **Run now** con `expected_batch_id=stream_001` y comprobá los cinco resultados.
4. Sin nuevos archivos, repetí el mismo Job. La validación debe mostrar `idempotence_compared=True`.
5. Generá `stream_002` y usá **Run now with different parameters** cambiando `expected_batch_id` a esa etapa.
6. Repetí publicación → Job → validación para 003, 004 y 005, en ese orden.
7. Repetí 005 sin publicar un archivo y guardá la segunda evidencia de idempotencia.

Son siete ejecuciones planificadas del Job: cinco llegadas y dos reejecuciones. Las tareas pueden producir varios microbatches; no se exige una cantidad fija.

## 5. Recuperación

Si una tarea falla, las posteriores deben quedar omitidas. Revisá la salida de la tarea fallida y reejecutá el Job conservando archivos, destinos y checkpoints. No publicar el siguiente lote hasta que `validate` termine correctamente.

Una tarea anterior que ya terminó correctamente volverá a consultar su fuente, pero su checkpoint hará que procese sólo lo pendiente. El checkpoint de Gold no se comparte con el de dedup.

## Problemas frecuentes

| Síntoma | Revisar |
|---|---|
| Faltan dimensiones o escala incorrecta | Reejecutar preflight con el identificador y escala originales |
| `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED` | Todas las escrituras deben conservar `trigger(availableNow=True)` |
| Error del productor por archivo existente | El archivo ya se publicó; repetir el Job, no el productor |
| La validación dice que hay varias etapas adelantadas | No publicar todos los archivos juntos; revisar la secuencia |
| Gold vacía en 001/002 | Es esperado en append con ventanas todavía abiertas |
| Lectura Delta falla tras un cambio | No sobrescribir ni modificar las tablas fuente del experimento |
| Checkpoint en uso o conflicto de ejecución | Concurrencia del Job en 1 y ausencia de una ejecución manual simultánea |
| Consulta supera diez minutos | Revisar error/progreso y cuota; repetir con el mismo checkpoint |

Los nombres de menú pueden variar. Los requisitos son cinco tareas secuenciales, parámetros explícitos, serverless y una sola ejecución simultánea.
