# Clase práctica 3 — Bases de datos NoSQL: el mismo dato, cinco modelos

## Propósito

Tomar los clientes, productos y transacciones Silver de la clase 2 y reorganizarlos según los modelos NoSQL vistos en la teoría: clave-valor, documental, grafos, vectorial y columnar. Antes de eso se simula el problema común a todos al distribuirse: CAP y PACELC.

La práctica es **demostrativa y visual**. Databricks no es Redis, MongoDB, Neo4j ni Pinecone: cada modelo se simula con Spark, tablas Delta y Python para observar su idea central (cómo se guarda, cómo se consulta, qué cuesta distribuirlo) sin instalar motores externos.

## Objetivos

- Explicar con un ejemplo concreto qué se pierde ante una partición en un sistema CP y en uno AP, y el trade-off latencia/consistencia de PACELC.
- Reconocer el acceso por clave de un almacén clave-valor y el rol de la función hash dentro de un nodo y entre nodos.
- Diseñar un documento a partir de un patrón de lectura: qué se embebe y qué se referencia.
- Expresar un patrón de grafo en Cypher y en SQL, y entender por qué cada salto es costoso de distribuir.
- Buscar vecinos por similitud y entender el trade-off recall/costo de un índice ANN.
- Relacionar Parquet y Delta con el almacenamiento columnar: column pruning, compresión y data skipping.

## Requisitos previos

- Clases 1 y 2 ejecutadas en el mismo catálogo y esquema personal: se usan `silver_customers`, `silver_products` y `silver_transactions`.
- Mismo `student_id` y misma escala que en la clase 2.
- Databricks Free Edition con compute serverless. No hay que instalar librerías: se usan Spark, NumPy, pandas, matplotlib y pyarrow incluidos en la plataforma.
- Para importación manual, conservar `clase-03/` y `common/` como carpetas hermanas.

## Secuencia

Los notebooks son independientes entre sí (salvo el preflight) y se ejecutan manualmente, en orden, celda por celda. Leé el texto antes de cada celda y mirá cada gráfico: las preguntas se responden con lo que muestran.

| Notebook | Modelo | Qué se ve | Minutos |
|---|---|---|---:|
| `00_preflight.ipynb` | Relacional | Verifica las tablas Silver y muestra el punto de partida | 5 |
| `01_cap_pacelc.ipynb` | Distribución | Tres réplicas, una partición, modos CP y AP; latencia vs. `W`; quórum `W+R>N` | 20 |
| `02_clave_valor.ipynb` | Clave-valor | Perfiles `customer:<id>`; GET en memoria vs. Spark; hash mod N vs. hashing consistente | 20 |
| `03_documental.ipynb` | Documental | Un documento por cliente con sus transacciones embebidas; consultas anidadas; esquema flexible; tamaño | 25 |
| `04_grafos.ipynb` | Grafos | Clientes y dispositivos; Cypher vs. SQL; componentes conexos; dibujo; particionamiento | 25 |
| `05_vectorial.ipynb` | Vectorial | Vectores de comportamiento; PCA; k-NN exacto; índice IVF y curva recall/costo | 25 |
| `06_columnar.ipynb` | Columnar | Fila vs. columna; CSV/JSON/Parquet; plan físico; footer Parquet; data skipping | 20 |

Cada notebook termina con una sección **Tu turno** que forma parte de la entrega.

## Tablas y archivos que se crean

```text
nosql_kv_customer_profile        (key, value JSON)
nosql_customer_documents         (_id, profile, stats, transactions[])
nosql_graph_nodes / nosql_graph_edges / nosql_graph_components
nosql_graph_cc_a / nosql_graph_cc_b   (auxiliares de la propagación de etiquetas)
nosql_customer_vectors           (customer_id, embedding[], flagged)
landing/nosql_lab/customer_documents/   JSON Lines
landing/nosql_lab/flexible_documents/   JSON con esquemas distintos
landing/nosql_lab/columnar/             CSV, JSON y Parquet de las transacciones
```

Ninguna tabla de las clases 1 y 2 se modifica. Todos los notebooks pueden reejecutarse: sobrescriben sus propias tablas y archivos.

## Preguntas de comprensión

Respondé en el `README.md` de la entrega. Cuando la pregunta pide un resultado, copiá el número o la tabla que obtuviste y explicá qué significa; cuando pide interpretación, alcanza con 3 a 6 oraciones. Indicá siempre el notebook y la sección en que te basás.

### CAP y PACELC (`01_cap_pacelc`)

1. En el escenario de partición, ¿qué respondió la lectura desde `C` en modo CP y en modo AP? Relacioná cada resultado con la **C** y la **A** del teorema CAP.
2. En modo AP, después de la reparación, ¿qué valor quedó en las tres réplicas y qué escritura se perdió? ¿Por qué *last-write-wins* es peligroso para un saldo? Mencioná una alternativa (por ejemplo, la estrategia de Dynamo de conservar versiones en conflicto).
3. En el gráfico de latencia, ¿cuánto vale la p99 con `W=1` y con `W=3`? Explicá el resultado con la parte **ELC** de PACELC.
4. Con `N=3` y con `N=5` (Tu turno), ¿qué combinaciones de `W` y `R` dieron probabilidad 0 de lectura vieja? Explicá por qué la condición es `W+R>N`.

### Clave-valor (`02_clave_valor`)

5. ¿Cuántas veces más rápido fue el GET en memoria que el GET con Spark? ¿Por qué Delta no reemplaza a Redis para servir el perfil de un cliente durante un pago, y por qué Redis no reemplaza a Delta para la analítica de la clase 2?
6. Para contar los clientes de AR se parsearon todos los valores. ¿Por qué ocurre en un modelo clave-valor? ¿Qué estructura mantendrías en Redis para responder esa consulta?
7. Completá la tabla del Tu turno. ¿Qué porcentaje de claves se mueve con `mod N` y con hashing consistente al pasar de 4 a 5 nodos? ¿Para qué sirven los nodos virtuales?
8. La teoría presenta la función hash como el motor del modelo clave-valor **en dos niveles**. Identificá en el notebook dónde aparece cada nivel.

### Documental (`03_documental`)

9. ¿Qué datos quedaron **embebidos** en el documento del cliente y qué datos quedaron como **referencia**? ¿Por qué el producto se guarda como snapshot dentro de cada transacción? Mencioná una ventaja y un riesgo.
10. Compará la consulta del historial del cliente 42 en el modelo relacional y en el documental: tablas leídas, joins y filas devueltas. ¿Para qué patrón de acceso conviene cada uno?
11. ¿Por qué el monto por categoría obligó a usar `explode`? ¿Qué dice eso sobre diseñar documentos "a partir de las consultas"?
12. ¿Qué esquema infirió Spark para los documentos heterogéneos? ¿Qué diferencia hay entre los documentos `900002` y `900003` respecto del email y por qué esa diferencia se pierde al leer con esquema?
13. ¿Cuánto pesa el documento más grande y cuántas transacciones harían falta para llegar al límite de 16 MB de MongoDB? ¿Cómo rediseñarías el documento si un cliente pudiera tener millones de compras?

### Grafos (`04_grafos`)

14. Escribí el patrón de cuatro saltos del Tu turno en Cypher y en SQL. ¿Cuántos joins necesitaste para dos y para cuatro saltos? Explicá por qué Neo4j (*index-free adjacency*) no paga ese costo de la misma manera.
15. ¿Qué porcentaje de clientes marcados hay entre todos los clientes y entre los conectados a un marcado por un dispositivo? ¿Qué conclusión sacás? Revisá cómo genera los datos `common/generate_data.py` (columnas `device_id` e `is_fraud`) y explicá si el resultado era esperable.
16. ¿Cuántas iteraciones tardó en converger la propagación de etiquetas y qué relación tiene ese número con la cantidad de saltos del componente más largo? ¿Por qué este cálculo es OLAP de grafos y no una consulta OLTP?
17. ¿Qué porcentaje de aristas quedó entre máquinas distintas con `hash(id) mod 4` y con el particionamiento por componente? ¿Por qué un grafo real (una red social, por ejemplo) no puede particionarse tan limpiamente como en este ejemplo?

### Vectorial (`05_vectorial`)

18. ¿Qué proporción de marcados hay entre los vecinos de clientes marcados y entre los vecinos de no marcados, comparada con la tasa base? La exactitud del clasificador k-NN, ¿es mejor que predecir siempre "no marcado"? ¿Qué te dice esto sobre la exactitud como métrica y sobre la calidad de estos vectores? Usá también el resultado del Tu turno 2.
19. En la curva del índice IVF, ¿qué `nprobe` necesitás para un recall@10 ≥ 0,9 y qué porcentaje de vectores se comparan? Explicá el trade-off de ANN e indicá qué pasa cuando se insertan vectores nuevos con una distribución distinta.

### Columnar (`06_columnar`)

20. Reportá el tamaño de los tres formatos y la tasa de compresión Parquet/CSV. ¿Qué muestra el `ReadSchema` de la consulta sobre Parquet y cómo cambia con la consulta del Tu turno? ¿Cuántos archivos se pueden saltear con datos al azar y con datos ordenados? Relacioná estos tres efectos con por qué Delta es buena para analítica y mala para actualizar una fila por vez.

### Cierre: tabla de decisión

Completá la tabla eligiendo un modelo y un motor para cada necesidad de la plataforma de e-commerce, con una justificación de una línea. Incluí la clasificación CAP/PACELC del motor elegido según la teoría.

| Necesidad | Modelo | Motor | CAP / PACELC | Justificación |
|---|---|---|---|---|
| Carrito de compras y sesión del usuario | | | | |
| Ficha de producto con atributos distintos por categoría | | | | |
| Detección de redes de cuentas que comparten tarjetas y dispositivos | | | | |
| Recomendaciones "clientes parecidos compraron…" | | | | |
| Reporte mensual de ventas por país y categoría | | | | |

## Entrega

La publicación, el acceso público y el envío a los profesores siguen el [formato común de entrega](../README.md#formato-común-de-entrega).

En tu repositorio personal creá `resolucion-practica-3/` con:

```text
resolucion-practica-3/
├── README.md
├── 01_cap_pacelc.ipynb
├── 02_clave_valor.ipynb
├── 03_documental.ipynb
├── 04_grafos.ipynb
├── 05_vectorial.ipynb
└── 06_columnar.ipynb
```

- Los notebooks deben estar **ejecutados**, con los gráficos visibles y las celdas **Tu turno** resueltas. Exportalos desde **File → Export → IPython Notebook**. Si una exportación no conserva los gráficos, agregá capturas al README.
- El `README.md` debe incluir nombre, `student_id`, escala, las respuestas a las 20 preguntas y la tabla de decisión. Podés partir de la [plantilla](PLANTILLA_ENTREGA.md).

No incluyas datos generados, archivos del volumen, credenciales ni tokens.
