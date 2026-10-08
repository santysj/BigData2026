# Prácticas integradas — Herramientas de procesamiento para grandes volúmenes de datos

Estas prácticas usan un único caso conductor: una plataforma ficticia de comercio electrónico que necesita detectar operaciones potencialmente fraudulentas. Cada encuentro agrega una capa al mismo sistema.

## Recorrido

1. **Clase 1 — Ingesta y Bronze:** workspace, Unity Catalog, PySpark, CSV/JSON, Parquet y Delta.
2. **Clase 2 — Silver y Gold:** calidad, deduplicación, joins, `MERGE` y productos analíticos.
3. **Clase 3 — Streaming:** procesamiento incremental, ventanas, watermarks y checkpoints.
4. **Clase 4 — MLOps (planificada, todavía sin TP implementado):** features, MLflow, registro, scoring y monitoreo.

Las clases 1, 2 y 3 están implementadas sobre Databricks Free Edition. La segunda continúa directamente desde las tablas Bronze construidas en la primera; la tercera usa sus dimensiones Silver para un experimento de streaming por llegadas controladas.

### Práctica complementaria — NoSQL y modelado documental

El módulo [NoSQL con JSON en Databricks](nosql/README.md) trabaja documentos anidados, datos embebidos y referencias, esquemas flexibles, nulls, arrays y consultas SQL/PySpark sobre `VARIANT`. Incluye fuentes reproducibles, cuarentena, un desafío de evolución de documentos y solución docente. Puede realizarse después de la clase 1 o de forma autónoma una vez preparado el entorno; no necesita un motor NoSQL externo.

## Empezar desde cero

Si es tu primer contacto con Git y GitHub, empezá por la [guía básica de Git y GitHub](GUIA_GIT_GITHUB.md). La Parte 1 incluye los conceptos esenciales y un ejercicio desde el navegador; la Parte 2 permite clonar ese repositorio en tu computadora, crear un archivo Markdown y practicar `add`, `commit` y `push`.

Si todavía no tenés el entorno preparado, seguí primero la [guía paso a paso de Databricks Free Edition](GUIA_SETUP_DATABRICKS_FREE.md). Incluye creación de cuenta, GitHub, compute serverless, verificación de Unity Catalog, importación manual y resolución de problemas.

## Requisitos

- Una cuenta personal de Databricks Free Edition.
- Un Git folder conectado a este repositorio, o los notebooks importados manualmente.
- Python y SQL. No se requieren librerías externas.

Free Edition utiliza compute serverless y tiene cuotas diarias. Todos los notebooks incluyen un modo `small` y evitan procesos que queden ejecutándose indefinidamente.

## Convenciones

Cada alumno trabaja en un esquema propio dentro del catálogo predeterminado:

```text
<catalogo_actual>.bigdata_<identificador>
```

Los archivos crudos se guardan en un volumen administrado llamado `landing`. Las capas del recorrido integrado usan `bronze_*`, `silver_*` y `gold_*`; el módulo documental usa `nosql_*` y el TP de streaming agrega recursos operativos `stream_*`.

## Formato común de entrega

Las entregas se publican en el repositorio personal `mi-primer-proyecto` de la [guía de Git y GitHub](GUIA_GIT_GITHUB.md), bajo `resolucion-practica-1/`, `resolucion-practica-2/`, `resolucion-practica-3/` o `resolucion-nosql/`, según el módulo.

Todas incluyen un `README.md` con nombre, `student_id`, escala, resultados y respuestas requeridas, más los notebooks solicitados exportados con sus salidas. Para análisis, adjuntá consulta SQL o PySpark, resultado relevante e interpretación; para preguntas sobre código, identificá notebook/helper y sección. Si una exportación no conserva las salidas, adjuntá capturas legibles de la ejecución.

El repositorio debe ser público y accesible desde una ventana privada. Enviá su URL a **dabadie@itba.edu.ar** y **ghenrion@itba.edu.ar**. No publiques credenciales, checkpoints ni archivos generados del volumen.

Cada TP define sus archivos obligatorios, preguntas, visualizaciones y desafíos. La clase 1 tiene una entrega introductoria y desafío opcional; las clases 2 y 3 incluyen 20 preguntas y cuatro visualizaciones; NoSQL tiene un desafío documental y preguntas propias.

## Uso de la clase 1

Ejecutar en orden:

1. `GUIA_SETUP_DATABRICKS_FREE.md`
2. `clase-01/00_setup.ipynb`
3. `clase-01/01_ingesta_bronze.ipynb`
4. `clase-01/02_desafio.ipynb`

El desafío es opcional para la entrega de clase 1. Este directorio todavía no incluye una solución docente publicada. Los módulos NoSQL y streaming sí tienen material bajo sus carpetas `docente/`.

## Uso de la clase 2

Conservá el mismo `student_id` y la misma escala de la clase 1. Luego ejecutá:

1. `clase-02/00_preflight.ipynb`
2. `clase-02/00_generate_new_batch.ipynb`
3. La secuencia de cuatro notebooks mediante el Job descripto en `clase-02/GUIA_CREAR_JOB.md`
4. `clase-02/05_visualizacion.ipynb`, una vez validado el pipeline

La práctica construye Silver y Gold, incorpora cuarentena y `MERGE`, y valida la llegada de un archivo nuevo y la reejecución idempotente del pipeline.

## Uso de la clase 3

Conservá el mismo `student_id` y escala de las clases 1 y 2. Seguí el [TP de streaming](clase-03/README.md): preflight, productor de cinco llegadas secuenciales, Job de cinco tareas, visualizaciones y desafío de recuperación. No publiques todos los archivos juntos; cada etapa se procesa y valida antes de la siguiente. Todas las consultas usan `AvailableNow` y checkpoints persistentes.

La entrega continúa en el repositorio personal bajo `resolucion-practica-3/`, con notebooks, evidencias, cuatro visualizaciones y respuestas a 20 preguntas. Hay una [plantilla de README](clase-03/PLANTILLA_ENTREGA.md) y [guía de creación del Job](clase-03/GUIA_CREAR_JOB.md).

## Reinicio seguro

Los módulos comparten el esquema personal y el volumen `landing`. Eliminar ese esquema con `DROP SCHEMA ... CASCADE` elimina recursos de todos los TPs del alumno, incluidos datos y checkpoints de streaming; no es un reinicio aislado de clase 1.

El setup no ejecuta esa eliminación automáticamente. Para repetir una etapa, seguí las instrucciones del TP: algunas reconstruyen tablas derivadas y streaming conserva destinos y checkpoints. No cambies la escala de clases 1–3 dentro de un experimento existente. No se debe borrar el catálogo ni esquemas ajenos.

## Revisión del material

La [revisión de consistencia del 7 de octubre de 2026](REVISION_CONSISTENCIA.md) registra las correcciones realizadas y una inconsistencia pendiente en la interpretación del resumen por lote de clase 2.
