# Entrega práctica 3 — Streaming en Databricks

## Identificación

- Nombre:
- `student_id`:
- Escala:
- Catálogo y esquema:
- Nombre exacto o URL del Job:
- Commit o URL del repositorio con la entrega:

## Arquitectura y Job

Insertar captura del DAG de cinco tareas y tabla de parámetros. Explicar el grano de cada capa, las cuatro rutas de checkpoint y la función de los snapshots de dimensiones.

## Ejecuciones y controles

| Etapa / intento | ID o enlace de ejecución | Bronze | Válidos | Cuarentena | Silver deduplicada | Filas Gold | Idempotencia comparada |
|---|---|---:|---:|---:|---:|---:|---|
| 001 primera | | | | | | | |
| 001 reejecución | | | | | | | |
| 002 | | | | | | | |
| 003 | | | | | | | |
| 004 | | | | | | | |
| 005 primera | | | | | | | |
| 005 reejecución | | | | | | | |

Las cantidades son acumuladas al terminar cada etapa; las filas Gold son grupos de ventana/canal, no compras. Registrar aparte las compras y montos al interpretar los gráficos.

Adjuntar evidencia de controles aprobados y de watermarks/estado relevantes. Explicar por qué las filas de auditoría pueden aumentar aunque el resultado de negocio sea estable. Para comparar etapas, usar una validación exitosa por etapa; las reejecuciones documentan idempotencia y no deben sumarse como nuevas llegadas.

## Seguimiento de eventos

Para `e002_000`, `late_ok_000` y `late_bad_000`, incluir consultas, resultados por capa y una explicación de identidad, calidad y tratamiento temporal.

## Visualización orientada a preguntas

### V1 — Evolución por ventanas

Pregunta, consulta, gráfico con ejes/unidades y conclusión de dos o tres oraciones.

### V2 — Fraude y tamaño de muestra

Pregunta, consulta, gráfico con tasa y volumen y conclusión.

### V3 — Llegadas y event time

Pregunta, consulta, gráfico/distribución y explicación de eventos fuera de orden.

### V4 — Watermark y cierre

Pregunta, consulta, gráfico por etapa, evidencia del progreso y conclusión.

## Desafío

- Referencia batch: consulta o PySpark, tipos y métricas por inicio/fin de ventana y canal, incluido fraude.
- Diferencia por ventana/canal respecto de Gold: resultado y evento responsable.
- Replay: filas tras la primera ejecución con a, segunda con a y tercera con b.
- `experiment_id` y tabla aislada utilizados:
- Evidencia del control final del desafío con `validar_entrega=si`:
- Plan breve de recuperación ante un fallo entre Bronze y Gold:

## Preguntas de análisis y comprensión

Copiar las preguntas del README del TP y responderlas en estos apartados. Incluir consulta y resultado en P01–P10, y referencia a notebook/helper y sección en P11–P20.

### P01

### P02

### P03

### P04

### P05

### P06

### P07

### P08

### P09

### P10

### P11

### P12

### P13

### P14

### P15

### P16

### P17

### P18

### P19

### P20

## Archivos y publicación

Comprobar los siete notebooks exportados, el README y las evidencias. Si una exportación no conservó salidas, adjuntar capturas legibles del resultado de la tarea. Verificar acceso público a la carpeta y enviar la URL a los profesores según el README del TP.
