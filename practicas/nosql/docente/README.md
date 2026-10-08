# Guía docente — Modelado documental en Databricks

## Preparación

Ejecutá los notebooks 00 y 02 con `scale = test` y luego [04_solucion](04_solucion.ipynb). La solución recarga el contexto, crea las dos vistas de entrega en su sesión, reconstruye la copia de trabajo e incorpora la versión 3. Sus aserciones son las mismas que usa el control final del alumno. Las vistas tienen exactamente las columnas y tipos exigidos en A y B.

Las fuentes son independientes de las clases 1 y 2. Podés usar el mismo identificador sin reemplazar sus tablas. El setup sí reconstruye las tablas de entrada de este módulo.

## Resultados esperados

Para `test`:

- 16 entradas = 12 aceptadas + 4 rechazos. Un rechazo por motivo: `JSON_INVALIDO`, `JSON_NULO`, `DOCUMENTO_NO_OBJETO` y `TOTAL_NO_NUMERICO`.
- 4 clientes, 6 productos y 18 líneas. Los pedidos pagados son 9 y sus líneas son 14.
- Versiones: 8 documentos v1 y 4 v2.
- O000011 es pendiente, sin líneas, con total cero. La expansión normal lo omite; la expansión outer conserva una fila NULL.
- El total de O000012 se almacena como texto `"260.00"` y se convierte a decimal correctamente.
- O000009 tiene cupón ausente; O000010, null explícito; O000011, texto vacío; O000012, `BIENVENIDA`.

Ventas pagadas por categoría, con precios históricos:

| Categoría | Unidades | Importe USD | Pedidos distintos |
|---|---:|---:|---:|
| tecnologia | 10 | 4.600,00 | 5 |
| hogar | 13 | 530,00 | 4 |
| libros | 4 | 120,00 | 2 |
| Total de unidades e importe | 27 | 5.250,00 | — |

Los conteos de pedidos por categoría suman 11 porque algunos pedidos contribuyen a varias categorías. El conteo global es 9. No sumar conteos distintos por grupos como si fueran conjuntos disjuntos.

Alertas según `paid AND total >= 1000 AND risk.score >= 0.8`:

| Pedido | Total USD | Puntaje |
|---|---:|---:|
| O000005 | 2.000,00 | 0,95 |
| O000009 | 1.030,00 | 0,92 |

O000007 y O000012 tienen puntajes altos pero no alcanzan el importe mínimo. El puntaje es sintético; la regla señala candidatos y no confirma fraude.

Para `small`, multiplicar por 100 todos los conteos y métricas de ventas de `test`, excepto productos, que siguen siendo 6. Los identificadores de cada bloque son diferentes. Las alertas son 200; los controles generan las claves esperadas por bloque.

Tras la evolución, `nosql_orders_challenge` tiene 13 pedidos en `test` o 1.201 en `small`, con una sola versión 3 `O_NUEVO_001`. El nuevo pedido incluye dos unidades de P005 a USD 30, total USD 60 y objeto `gift`. Las vistas de entrega se calculan sobre la base original, por lo que no suman este pedido nuevo.

## Respuestas conceptuales y puntos para discutir

- **Agregado documental:** pedido y líneas se leen juntos y tienen un ciclo de vida común. Un array de todos los pedidos dentro de un cliente crecería sin límite y complicaría escrituras concurrentes.
- **Snapshot:** cliente, dirección y precio histórico deben conservar el significado de la compra. La entidad actual puede cambiar. El precio actual de P001 (1.100) difiere del histórico (1.000).
- **Referencia:** permite consultar datos actuales sin actualizar todas las copias históricas. El identificador lógico no crea automáticamente integridad referencial: revisar joins sin correspondencia.
- **Nulls:** la ausencia genera null SQL al navegar; el null JSON explícito se detecta con `is_variant_null`; `""` es un valor de texto. El casting a STRING puede unificar ausencia y null, por eso se examina primero el VARIANT.
- **Flexibilidad:** el campo `gift` cabe en la columna VARIANT sin agregar una columna física. Los consumidores requieren saber interpretarlo y tolerar su ausencia. La aplicación puede seguir necesitando validaciones y migraciones.
- **Contratos fijos:** `from_json` con STRUCT proyecta sólo los campos declarados; el JSON original y el VARIANT permiten conservar los demás.
- **Grano:** tras expandir, una fila representa una línea. Sumar `order.total` repetido en cada línea sobrecuenta ventas; sumar `quantity * unit_price` conserva el importe.
- **Calidad:** proponer cantidades positivas, tipos de items, precios no negativos, referencias existentes, categorías conocidas, rango de puntaje y coherencia entre total y suma de líneas. El contrato inicial del laboratorio es parcial.
- **Alcance:** JSON es un formato. Aprender sus consultas en Delta no mide índices documentales, unicidad automática de una colección, atomicidad de escrituras en un motor NoSQL, sharding ni tradeoffs de consistencia.

## Verificación del material

Desde la raíz del repositorio:

```powershell
py -3 -m unittest discover -s practicas/nosql/tests -v
```

Los tests locales verifican las fuentes, referencias, casos de calidad, resultados de negocio, sintaxis Python de las celdas y rutas `%run`. Incluyen casos negativos de tipos de vistas y campos de la versión 3, y comparan los controles del alumno y docente. No necesitan Spark. Los controles de los notebooks verifican el comportamiento de VARIANT y Delta al ejecutarse en Databricks; el chequeo local no sustituye esa ejecución.

La validación técnica comprueba ventas, alertas, tipos de las vistas, conservación de columnas y documentos, fecha convertible, campos de snapshot/envío/pago, categoría de la línea nueva y tipos de `gift`. Revisá manualmente el significado de esos campos y las respuestas conceptuales: un valor presente y convertible no garantiza que sea adecuado para el negocio.

Usá la [plantilla de entrega](../PLANTILLA_ENTREGA.md) para verificar evidencia A/B/C y respuestas D01–D05. Exigí consulta, resultado e interpretación en los análisis. Para idempotencia, los alumnos deben repetir únicamente el `MERGE`, conservando la copia del desafío entre intentos.
