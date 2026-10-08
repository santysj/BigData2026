# Entrega NoSQL — Modelado documental en Databricks

## Identificación

- Nombre:
- `student_id`:
- Escala NoSQL (`test` o `small`):
- Catálogo y esquema:
- Commit o URL del repositorio con la entrega:

## Ejecución y controles

| Evidencia | Resultado | Consulta o captura |
|---|---|---|
| Entradas originales, pedidos aceptados y cuarentena | | |
| Cantidad de líneas y reconciliación de importes | | |
| Control final del desafío con `validar_entrega=si` | | |

Los resultados de A y B se calculan sobre `nosql_orders` y su vista de líneas original. El pedido nuevo de C sólo se incorpora a `nosql_orders_challenge`.

## A — Ventas por categoría

Incluir la consulta SQL/PySpark que crea `nosql_entrega_ventas`, su resultado y una interpretación. Explicar el grano y por qué se usan los precios históricos de las líneas.

## B — Alertas

Incluir la consulta SQL/PySpark que crea `nosql_entrega_alertas`, su resultado y una interpretación. Explicar por qué la regla señala sospechas y no confirma fraude.

## C — Evolución e idempotencia

- Documento nuevo y justificación de sus campos:
- Consulta de `gift` en documentos anteriores y en la versión 3, con resultado:
- Cantidades antes de incorporar el documento, después de incorporarlo y después de repetir únicamente la celda `MERGE`:
- Evidencia de conservación de documentos anteriores y columnas físicas:
- Resultado del control final:

Para demostrar idempotencia, repetir la celda de incorporación sin reconstruir antes la copia de trabajo.

## Decisiones de modelado de 01

Incluir las decisiones discutidas en `01_modelado_documental`, con una referencia a su sección. Se puede remitir a D01 cuando la respuesta cubra la misma decisión.

## Respuestas conceptuales

Copiar las cinco preguntas de la sección D de `03_desafio` y responderlas. Fundamentar con observaciones del laboratorio y referencias a notebooks/secciones.

### D01 — Embeber y referenciar

### D02 — Ausente, null y vacío

### D03 — Regla adicional de calidad

### D04 — Evolución y consumidores

### D05 — Alcance y base documental real

## Archivos y publicación

Comprobar `README.md` y `03_desafio.ipynb`, con salidas y control final aprobado. Si faltan salidas, adjuntar capturas legibles bajo `evidencias/`. Verificar el acceso público a `resolucion-nosql/` y enviar la URL a los profesores indicados en el README del TP. No incluir datos generados ni credenciales.
