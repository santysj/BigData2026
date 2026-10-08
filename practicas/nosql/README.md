# Práctica complementaria — Modelado documental con JSON en Databricks

## Propósito y alcance

Explorar conceptos del modelo documental NoSQL usando pedidos de comercio electrónico: documentos anidados, arrays, datos embebidos, referencias y evolución de esquema. Procesaremos esos documentos en Databricks con SQL y PySpark, almacenándolos en tablas Delta con una columna `VARIANT`.

JSON es un formato de datos; NoSQL incluye modelos documentales, clave-valor, familias de columnas y grafos. Este laboratorio se concentra en el modelo documental y su análisis. Las tablas Delta son el soporte del laboratorio: no evaluamos CRUD de MongoDB, índices de una base documental, sharding, replicación ni consistencia distribuida.

Es un módulo complementario al recorrido de clases 1–4. Utiliza el mismo escenario ficticio, con fuentes independientes, y puede realizarse después de la clase 1 o de forma autónoma tras preparar el entorno.

## Objetivos

Al finalizar, el alumno podrá:

- Diseñar un documento a partir de un patrón de lectura y justificar qué embeber y qué referenciar.
- Preservar JSON original y consultarlo como `VARIANT`.
- Distinguir campo ausente, null JSON explícito y texto vacío.
- Reconocer que la flexibilidad de esquema requiere contratos y controles de calidad.
- Expandir listas, identificar el grano y calcular importes sin duplicaciones.
- Incorporar una nueva versión del documento y analizar su impacto sobre consumidores anteriores.

## Requisitos

1. Completar la [guía de Databricks Free Edition](../GUIA_SETUP_DATABRICKS_FREE.md).
2. Tener este repositorio como Git folder. Si importás manualmente, conservá `nosql/` y `common/` como carpetas hermanas; incluí `common/config.py` y `common/generate_nosql_data.py` para que funcionen los `%run`.
3. Usar compute serverless con soporte para `VARIANT`. El primer notebook verifica las funciones antes de escribir datos. Para compute clásico, usar Databricks Runtime 15.4 o superior.
4. Conocer Python básico y `SELECT`, `WHERE`, `GROUP BY` y `JOIN`.

La práctica no necesita bases externas, credenciales adicionales ni instalaciones de paquetes. La duración propuesta es de **120–130 minutos**.

## Ejecución

Usá el mismo `student_id` y la misma escala en todos los notebooks. Si ya tenés un esquema del curso, conservá tu identificador; las tablas del módulo usan nombres propios `nosql_*`.

| Orden | Notebook | Contenido | Minutos |
|---|---|---|---:|
| 1 | [00_setup_datos](00_setup_datos.ipynb) | Entorno, generación, ingesta y cuarentena | 20 |
| 2 | [01_modelado_documental](01_modelado_documental.ipynb) | Embebidos, referencias, versiones y nulls | 30 |
| 3 | [02_consultas_analiticas](02_consultas_analiticas.ipynb) | Arrays, reconciliación, SQL, PySpark y alertas | 35 |
| 4 | [03_desafio](03_desafio.ipynb) | Consultas, evolución y entrega conceptual | 35–45 |

Cada notebook recarga su configuración. Las variables Python y las vistas temporales son propias de cada sesión: las vistas del desafío deben crearse dentro de `03_desafio`.

El desafío contiene celdas TODO. Al principio, `validar_entrega = no` permite recorrerlo sin ejecutar su control final. Tras completarlo, seleccioná `si` y ejecutá la celda de validación. Un mensaje «Validación pendiente» indica que todavía no se comprobó la resolución. Este control es una validación de la entrega; no crea un checkpoint de Structured Streaming.

## Caso de negocio

Un pedido reúne los datos que se leen juntos:

```json
{
  "order_id": "O000001",
  "schema_version": 1,
  "status": "paid",
  "customer_id": "C0001",
  "customer_snapshot": {"name": "Cliente ficticio 1", "country": "AR"},
  "shipping": {"country": "AR", "city": "Ciudad ficticia"},
  "items": [
    {"product_id": "P001", "name": "Notebook", "category": "tecnologia", "quantity": 1, "unit_price": 1000},
    {"product_id": "P002", "name": "Auriculares", "category": "tecnologia", "quantity": 2, "unit_price": 100}
  ],
  "total": 1200,
  "currency": "USD",
  "risk": {"score": 0.1}
}
```

Es un extracto del documento generado; la fuente también incluye fecha y medio de pago. Los nombres, montos y puntajes son ficticios. Todos los importes se expresan en USD y los cupones son metadatos sin descuentos aplicados.

El snapshot del cliente y los precios de las líneas representan el momento de la compra. Las referencias permiten consultar el cliente y el producto actuales. El precio actual de P001 es 1.100; su precio histórico es 1.000. Esa diferencia permite discutir por qué no se recalcula una venta usando el catálogo actual.

## Datos y escalas

Las fuentes son JSON Lines: un documento por línea. Se generan en el volumen personal `landing/nosql/{orders,customers,products}`.

| Recurso | `test` | `small` |
|---|---:|---:|
| Entradas de pedidos | 16 | 1.600 |
| Pedidos aceptados | 12 | 1.200 |
| Entradas en cuarentena | 4 | 400 |
| Clientes actuales | 4 | 400 |
| Productos actuales | 6 | 6 |
| Líneas de pedidos aceptados | 18 | 1.800 |

`small` repite 100 bloques con identificadores propios; los seis productos se comparten. Las métricas agregadas crecen proporcionalmente. Es una ampliación didáctica, no una prueba representativa de rendimiento a gran escala. Esta escala es independiente de la usada en las clases 1 y 2.

Cada bloque incluye:

- Ocho pedidos de versión 1 y cuatro de versión 2; la segunda agrega atributos sin cambiar las columnas físicas.
- Cupones ausentes, null explícito, texto vacío y texto con valor.
- Un pedido pendiente con `items` vacío y total cero.
- Un total numérico almacenado como texto, que admite conversión a decimal.
- Cuatro rechazos: JSON roto, null JSON como raíz, array como raíz y objeto con total `N/A`.

## Tablas y controles

Los resultados se crean en `<catalogo_actual>.bigdata_<student_id>`:

| Objeto | Función |
|---|---|
| `nosql_raw_orders` | Todas las entradas, texto original, VARIANT y trazabilidad |
| `nosql_orders` | Documentos aceptados con `order_id` escalar |
| `nosql_quarantine` | Rechazos con un motivo explícito |
| `nosql_customers`, `nosql_products` | Entidades actuales por referencia |
| `nosql_order_items` | Vista persistente con una fila por pedido y posición de línea |
| `nosql_orders_challenge` | Copia de trabajo para la evolución de versión 3 |

El contrato de entrada es deliberadamente limitado: valida objeto raíz, identificadores, array de líneas, total convertible no negativo, moneda y estado. No valida todavía todas las líneas ni integridad referencial. El notebook 02 reconcilia los totales del conjunto suministrado; las consignas piden proponer reglas adicionales.

Las aserciones incorporadas comprueban conservación de filas, motivos de rechazo, unicidad, nulls, grano, reconciliación y equivalencia entre consultas SQL y PySpark. El desafío comprueba las columnas y tipos exactos de las vistas de ventas y alertas, sus resultados y la conservación de los documentos al incorporar uno nuevo. También verifica los campos exigidos para la versión 3, su línea y los tipos originales de `gift.message` y `gift.wrapped`. Estas verificaciones deben ejecutarse en Databricks.

## Reejecución y resolución de problemas

- `00_setup_datos` reconstruye sus cinco tablas y fuentes de `landing/nosql/`. Exportá antes cualquier modificación propia a esos objetos.
- `02_consultas_analiticas` reemplaza la vista de líneas; no necesita mantener la sesión de 01 abierta.
- La celda que prepara el desafío reemplaza sólo `nosql_orders_challenge`. Su helper usa `MERGE` por `order_id` para evitar duplicar el documento nuevo al repetir la incorporación.
- Para demostrar idempotencia, repetí sólo la celda de incorporación y compará cantidades antes/después. Reconstruir la copia antes de cada intento reinicia el ejercicio y no demuestra esa propiedad.
- Si faltan tablas, comprobá `student_id`, catálogo y orden de ejecución. Si la cantidad no coincide, comprobá la escala seleccionada.
- Si falla el soporte de `VARIANT`, usá un entorno compatible. No se resuelve instalando una biblioteca de Python.
- Para límites de compute, comenzá con `test`; no es necesario usar `small` para aprender los conceptos.
- No hace falta borrar esquemas ni catálogos para repetir la práctica.

## Entrega y evaluación

La publicación, el acceso público y el envío a los profesores siguen el [formato común de entrega](../README.md#formato-común-de-entrega).

En tu repositorio personal, creá `resolucion-nosql/` con:

1. `03_desafio.ipynb` exportado como IPython Notebook, ejecutado y con el control final aprobado.
2. `README.md` basado en [PLANTILLA_ENTREGA.md](PLANTILLA_ENTREGA.md), con nombre, `student_id`, escala NoSQL, catálogo/esquema, resultados de A/B, evidencia de evolución e idempotencia de C y respuestas D01–D05. Incluí las decisiones de diseño discutidas en 01.

En los análisis incluí consulta SQL o PySpark, resultado relevante e interpretación. Para las explicaciones, indicá notebook y sección. Exportá desde la sesión en que creaste las vistas temporales del desafío; si el archivo no conserva las salidas, agregá capturas legibles bajo `evidencias/`. Un mensaje «Validación pendiente» no es evidencia de aprobación.

No incluyas los archivos generados del volumen: se reconstruyen con 00.

| Criterio | Peso |
|---|---:|
| Justificación de embebidos, referencias y snapshots | 25% |
| Consultas con tipos y grano correctos | 30% |
| Evolución sin perder ni duplicar documentos anteriores | 25% |
| Calidad, nulls y explicación del alcance de NoSQL | 20% |

El [material docente](docente/README.md) contiene la solución y resultados esperados. Para distribuir una versión sin respuestas, compartí sólo los cuatro notebooks de alumnos, esta guía, la plantilla de entrega y los dos archivos de `common/` requeridos.

## Referencias oficiales

Funciones y requisitos revisados el **7 de octubre de 2026**. La compatibilidad real del workspace se verifica al ejecutar el notebook 00.

- [Consultar datos VARIANT](https://docs.databricks.com/aws/en/semi-structured/variant): navegación, conversiones y diferencias entre null SQL y null JSON.
- [try_parse_json](https://docs.databricks.com/aws/en/sql/language-manual/functions/try_parse_json): conservar errores de parseo como NULL SQL.
- [try_variant_get](https://docs.databricks.com/aws/en/sql/language-manual/functions/try_variant_get): extracción con tipo y tolerancia a fallos de conversión.
- [schema_of_variant](https://docs.databricks.com/aws/en/sql/language-manual/functions/schema_of_variant): inspección del tipo original para distinguir un booleano JSON de texto convertible.
- [variant_explode](https://docs.databricks.com/aws/en/sql/language-manual/functions/variant_explode) y [variant_explode_outer](https://docs.databricks.com/aws/en/sql/language-manual/functions/variant_explode_outer): expansión de documentos y arrays.
- [Tipo VARIANT](https://docs.databricks.com/aws/en/sql/language-manual/data-types/variant-type): representación y funciones disponibles.
