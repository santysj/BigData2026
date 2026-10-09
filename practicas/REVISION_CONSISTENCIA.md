# Revisión de consistencia de los TPs

> **Nota del 8 de octubre de 2026:** después de esta revisión el TP de streaming pasó a `clase-04/` (clase 4), el módulo complementario `nosql/` fue retirado y la clase 3 pasó a ser el TP de NoSQL en `clase-03/`. Las referencias de este documento a «clase 3» corresponden al TP de streaming.

Fecha: **7 de octubre de 2026**. Alcance: clases 1, 2 y 3, módulo NoSQL, guías comunes, helpers, consignas, entregas y material docente existente.

Se encontraron inconsistencias de documentación y validación que ya fueron corregidas, y una inconsistencia de significado en las métricas por lote de clase 2 que sigue pendiente. Las diferencias entre el recorrido integrado y el módulo documental son mayormente intencionales.

## Hallazgo pendiente: calidad por lote en clase 2

**Prioridad alta para la evaluación del TP.** `gold_batch_summary` cuenta aceptadas desde `silver_transactions`, que conserva solamente la versión vigente de cada transacción. Cuenta rechazadas desde una cuarentena que conserva rechazos de lotes anteriores. Por eso el denominador mezcla estado vigente e historia de recepción.

La corrección de `transaction_id=42` llega tanto en `batch_002` como en `batch_003`. El segundo `MERGE` reemplaza su `source_batch_id`: la transacción deja de pertenecer a `batch_002` en Silver aunque ese archivo no haya cambiado.

Con escala `test`, cada archivo tiene 24 filas físicas: 20 altas, una corrección, un duplicado exacto y dos rechazos. Tras quitar el duplicado, cada lote aporta 21 versiones válidas y dos rechazadas.

| Métrica de `batch_002` | Después de procesar 002 | Después de procesar 003 |
|---|---:|---:|
| Filas físicas de su archivo | 24 | 24 |
| Versiones válidas recibidas en ese lote | 21 | 21 |
| Transacciones vigentes atribuidas a ese lote en Silver | 21 | 20 |
| Rechazadas históricas del lote | 2 | 2 |
| Tasa calculada con el Gold actual | 2/23 = 8,70% | 2/22 = 9,09% |

El problema afecta la pregunta 2, la tasa de rechazo de la pregunta 6 y la visualización 4. El validador actual reconcilia Gold contra Silver vigente, por lo que puede aprobar sin detectar esta diferencia conceptual.

**Corrección recomendada:** conservar una clasificación por lote de las versiones recibidas, con duplicados exactos resueltos y antes de elegir la última versión global por `transaction_id`. Usar esa fuente para las aceptadas y rechazadas de `gold_batch_summary`; mantener `silver_transactions` como estado vigente para ventas y riesgo. Deben ajustarse juntos el validador y las consignas para declarar ese grano. Una corrección válida cuenta como versión aceptada en cada lote que la recibió.

La implementación de este cambio queda pendiente: esta revisión no redefine el modelo de clase 2. Hasta corregirlo, el resumen actual no debe interpretarse como tasa de calidad del archivo original.

Fuentes: [generador de lotes](common/generate_incremental_data.py), [Silver](clase-02/02_build_silver.ipynb), [Gold](clase-02/03_build_gold.ipynb), [validador](clase-02/04_validate_pipeline.ipynb), [preguntas](clase-02/README.md) y [visualización](clase-02/05_visualizacion.ipynb).

## Inconsistencias corregidas

| Hallazgo | Efecto anterior | Corrección aplicada |
|---|---|---|
| Desafío opcional de clase 1 | La entrega pedía su reflexión y verificar tres archivos aunque no se hubiera realizado | Reflexión y notebook del desafío exigidos sólo si se realiza; dos archivos obligatorios |
| Aserción de unicidad en clase 1 | `None == None` podía aprobar sin completar los conteos | Exige conteos enteros, los contrasta con la tabla y después verifica unicidad |
| Trazabilidad de clase 1 | `_source_file` contenía el directorio de entrada; en otros módulos representa un archivo físico | Obtiene el archivo desde `_metadata.file_path` y conserva el directorio como `_source_path` |
| Uso de `expected_batch_id` en clase 2 | La guía lo reservaba al validador, pero la ingesta también podía detenerse por ese parámetro | La ingesta procesa archivos nuevos; la comprobación de la expectativa queda en `04_validate_pipeline` |
| Publicación de entregas | Clases 1 y 3 detallaban repositorio público y correo; clase 2 y NoSQL no explicitaban esos pasos | Política común enlazada desde clase 2 y NoSQL; identificación y escala explícitas |
| Material docente y carpetas | Se mencionaban una solución de clase 1 y directorios históricos que no existen | Documentación alineada con las carpetas presentes y soluciones disponibles |
| Cobertura de contenidos | El índice prometía Time Travel sin ejercicio de consulta histórica; clase 4 figuraba junto a módulos implementados | Se elimina esa promesa de clase 2 y se identifica clase 4 como planificada |
| Reinicio compartido | El borrado del esquema podía interpretarse como reinicio de una sola práctica | Se aclara que afecta todos los TPs del alumno, volumen y checkpoints incluidos |
| GitHub Actions | El README describía un workflow ausente como parte del estado actual | Los comandos se identifican como ejemplos históricos ajenos a los requisitos de los TPs |

La validación reforzada de clase 1 comprueba los conteos; no constituye una prueba completa de todas las decisiones de integración o de la conservación de historia. Las aserciones deben acompañarse de las evidencias solicitadas.

## Comparación de formato y continuidad

| Aspecto | Clase 1 | Clase 2 | Clase 3: streaming | NoSQL |
|---|---|---|---|---|
| Dependencia | Entorno preparado | Bronze de clase 1 | Dimensiones Silver de clase 2 | Entorno preparado; fuentes propias |
| Identidad y escala | Setup inicial `test`; ingesta de clase `small` | Mismo identificador y escala de la ingesta de clase 1 | Mismo identificador y escala de clases 1–2 | Mismo identificador; escala propia del módulo |
| Entrega | README e ingesta; desafío opcional | README y cuatro notebooks | README, siete notebooks y evidencias | README y desafío |
| Preguntas | Observaciones introductorias y cinco V | 20 preguntas y cuatro visualizaciones | 20 preguntas y cuatro visualizaciones | Decisiones de diseño y preguntas del desafío |
| Reejecución | Reconstrucción de Bronze; desafío en otra tabla | `COPY INTO`, `MERGE` y reconstrucción de Gold | Checkpoints persistentes y Gold de ventanas cerradas | Reconstrucción de sus fuentes/tablas; desafío en una copia |
| Material docente publicado | No | Sin carpeta de solución específica | Sí | Sí |

La guía de setup ya indica cambiar de `test` a `small` antes de la ingesta de clase 1: los valores por defecto diferentes no son una contradicción de escala en esa secuencia. Los widgets son propios de cada notebook; el identificador y la escala de los datos deben mantenerse explícitamente en las clases siguientes.

Las cantidades de preguntas y notebooks reflejan objetivos y dificultad distintos. Clases 2 y 3 mantienen el mismo formato de análisis y visualización. NoSQL trabaja el modelo documental con un alcance propio; no requiere cuatro gráficos ni 20 preguntas según su consigna actual.

NoSQL utiliza identificadores, categorías y moneda propios. Sus fuentes no deben unirse directamente con las dimensiones de clases 1–3. Compartir el escenario y el esquema personal no implica compartir las mismas entidades. La diferencia entre reconstruir Gold en batch y publicar ventanas cerradas en streaming también es intencional.

## Verificación y límites

- Revisión estructural de los **25 notebooks**, celdas Python, helpers y dependencias `%run`, además de enlaces locales del material.
- **14 tests locales aprobados:** seis de NoSQL y ocho de streaming, sobre fuentes reproducibles, resultados esperados, contratos de publicación y estructura del material.
- Verificación adicional del control de clase 1: rechaza valores pendientes, conteos inventados y duplicados reales; acepta un resultado válido.
- Reproducción local del cambio de atribución de `transaction_id=42` entre los dos lotes de clase 2.

No se ejecutaron los pipelines en un workspace Databricks durante esta revisión. Los controles locales no confirman compatibilidad efectiva de Free Edition, permisos de Unity Catalog, ejecución SQL/Spark ni comportamiento real de los microbatches. Esa validación del entorno sigue siendo necesaria antes de distribuir el TP como material probado en plataforma.
