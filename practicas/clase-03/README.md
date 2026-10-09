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

Los notebooks son **demostrativos**: no hay que modificar ni completar código. Se ejecutan en orden, celda por celda, leyendo el texto y mirando cada resultado y gráfico. Las preguntas de la entrega se responden con lo que muestran.

| Notebook | Modelo | Qué se ve | Minutos |
|---|---|---|---:|
| `00_preflight.ipynb` | Relacional | Verifica las tablas Silver y muestra el punto de partida | 5 |
| `01_cap_pacelc.ipynb` | Distribución | Réplicas con una partición, modos CP y AP; latencia vs. `W`; quórum `W+R>N` | 15 |
| `02_clave_valor.ipynb` | Clave-valor | Perfiles `customer:<id>`; GET en memoria vs. Spark; hash mod N vs. hashing consistente | 20 |
| `03_documental.ipynb` | Documental | Un documento por cliente con sus transacciones embebidas; consultas anidadas; esquema flexible; tamaño | 20 |
| `04_grafos.ipynb` | Grafos | Clientes y dispositivos; Cypher vs. SQL; componentes conexos; dibujo; particionamiento | 20 |
| `05_vectorial.ipynb` | Vectorial | Vectores de comportamiento; PCA; k-NN exacto; índice IVF y curva recall/costo | 20 |
| `06_columnar.ipynb` | Columnar | Fila vs. columna; CSV/JSON/Parquet; plan físico; footer Parquet; data skipping | 15 |


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

## Preguntas

Las preguntas son cortas: buscan comprobar que entendiste la idea de cada modelo y sus ventajas y desventajas. Cuando piden un dato, copiá el número que te dio el notebook (puede variar un poco según tu escala). Alcanza con una a tres oraciones por respuesta.

### CAP y PACELC (`01_cap_pacelc`)

1. Con la red partida, ¿qué respondió la réplica `C` en modo CP y qué respondió en modo AP? ¿Qué propiedad de CAP sacrifica cada modo?
2. En modo AP, ¿qué saldo quedó en las tres réplicas después de la reparación y qué escritura se perdió?
3. Según el gráfico de latencia, ¿cuál es la p99 con `W=1` y con `W=3`? ¿Qué se gana a cambio de esperar más réplicas? (PACELC)
4. Con cinco réplicas partidas en `ABC | DE`, ¿qué lado pudo seguir escribiendo en modo CP y por qué?

### Clave-valor (`02_clave_valor`)

5. ¿Cuántas veces más rápido fue el GET en memoria que el GET con Spark? ¿Por qué Redis guarda los datos en memoria?
6. ¿Por qué para contar los clientes de AR hubo que leer y parsear todos los valores? Mencioná una ventaja y una desventaja del modelo clave-valor.
7. Al pasar de 4 a 5 nodos, ¿qué porcentaje de claves se movió con `hash mod N` y con hashing consistente? ¿Por qué conviene el segundo?

### Documental (`03_documental`)

8. ¿Cuántas filas devolvió la consulta relacional del cliente 42 y cuántos documentos la documental? ¿Qué datos quedaron embebidos dentro del documento?
9. Al leer documentos con campos distintos, ¿qué hizo Spark con los campos que faltaban en algunos? ¿Por qué se dice que el modelo documental tiene esquema flexible?
10. ¿Cuánto pesa el documento más grande? ¿Qué problema aparece si un documento crece sin límite?
11. Para calcular el monto por categoría hubo que usar `explode`. ¿Qué tipo de consultas resuelve bien el modelo documental y cuáles le cuestan más?

### Grafos (`04_grafos`)

12. ¿Cuántos nodos `Customer`, nodos `Device` y aristas `USES` tiene el grafo?
13. ¿Cuántos joins necesitó SQL para el patrón de 2 saltos y para el de 4? ¿Por qué Neo4j recorre relaciones más eficientemente que una base relacional?
14. ¿Cuántas iteraciones tardó en converger la búsqueda de componentes conexos y cuántos nodos tiene el componente más grande?
15. ¿Qué porcentaje de aristas quedó entre máquinas distintas al repartir los nodos con `hash(id) mod 4`? ¿Por qué es difícil distribuir una base de grafos?

### Vectorial (`05_vectorial`)

16. ¿Qué representa el vector de cada cliente y qué mide la similitud coseno?
17. ¿Qué porcentaje de clientes marcados hay entre los vecinos de clientes marcados y cuál es la tasa base? ¿Para qué sirve buscar "vecinos parecidos"?
18. En la curva del índice IVF, ¿qué recall y qué porcentaje de vectores escaneados se obtienen con `nprobe=4`? ¿Qué se gana y qué se pierde con la búsqueda aproximada (ANN)?

### Columnar (`06_columnar`)

19. ¿Cuánto ocupan los datos en CSV, JSON y Parquet? ¿Qué columnas leyó Spark para la consulta de `amount` sobre Parquet (`ReadSchema`)? ¿Cuántos archivos se pueden saltear con los datos ordenados?
20. ¿Por qué el formato columnar es bueno para analítica y poco conveniente para modificar una fila por vez?

## Entrega

La publicación, el acceso público y el envío a los profesores siguen el [formato común de entrega](../README.md#formato-común-de-entrega).

La entrega es **un único `README.md`** con las respuestas a las 20 preguntas, dentro de `resolucion-practica-3/` en tu repositorio personal:

```text
resolucion-practica-3/
└── README.md
```

El README debe incluir nombre, `student_id` y escala. Podés partir de la [plantilla](PLANTILLA_ENTREGA.md). No hace falta exportar los notebooks.

No incluyas datos generados, archivos del volumen, credenciales ni tokens.
