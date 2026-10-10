# Trabajo Práctico Final — Big Data y MLOps

**Fecha máxima de entrega: 31 de octubre de 2026.**

## Consigna general

Cada estudiante debe elegir **una** de las dos variantes: **1. Investigación** o **2. Ejecución**. Quien elija Ejecución debe resolver **una** de sus dos opciones. No se exige realizar las demás.

El trabajo debe comunicar el problema abordado, las decisiones tomadas, los resultados obtenidos y sus limitaciones. Las afirmaciones, los diagramas y los resultados deben ser consistentes entre sí.

## Variante 1 — Investigación: caso de negocio y arquitectura de datos

Elaborar un caso de negocio en el que Big Data aporte valor. Se puede elegir una organización real o hipotética, pero el problema y los supuestos deben quedar explícitos.

El trabajo debe incluir:

1. **Necesidad de negocio:** contexto, usuarios o áreas involucradas, problema actual, objetivo y beneficios esperados. Definir cómo se evaluaría el éxito de la propuesta.
2. **Datos:** fuentes previstas, tipo y volumen estimado, frecuencia de llegada, calidad, sensibilidad y principales restricciones. Diferenciar datos disponibles de supuestos.
3. **Arquitectura propuesta:** describir el recorrido de los datos desde la ingesta hasta su consumo. Proponer tecnologías concretas para almacenamiento, procesamiento, orquestación, calidad, gobierno y visualización, según corresponda al caso.
4. **Modelado de datos:** detallar entidades, atributos y relaciones principales; claves, granularidad y estructura de las tablas o colecciones. Explicar cómo el modelo permite responder las preguntas de negocio.
5. **Justificación:** fundamentar cada decisión relevante, incluidas las tecnologías y el diseño de datos, en función de la escala, la latencia, el costo, la seguridad, la operación y las necesidades del negocio. Explicitar compromisos y limitaciones.
6. **Gráficos y diagramas:** incorporar al menos un diagrama de arquitectura y un diagrama del modelo de datos, además de gráficos que ayuden a explicar el problema, los datos o el resultado esperado. Identificar como estimaciones los valores que no provengan de datos observados.

**Extensión opcional:** incorporar un caso de MLOps que funcione sobre la plataforma propuesta. Describir el objetivo del modelo, las variables o *features*, el flujo de entrenamiento e inferencia, el seguimiento de experimentos y el monitoreo.

Esta variante es una propuesta fundamentada; no requiere implementar la plataforma.

## Variante 2 — Ejecución

Elegir **una** de las siguientes opciones. En ambas, documentar los datos utilizados, las decisiones de implementación, los resultados y cómo reproducir la ejecución.

### Opción A — Arquitectura medallion en Databricks

Implementar una arquitectura **Bronze, Silver y Gold** sobre Databricks. Los pasos de transformación deben estar **orquestados en Pipelines de Databricks**.

La entrega debe mostrar:

1. Origen y descripción de los datos, esquema de entrada y criterio de elección.
2. Código de ingesta en Bronze, transformaciones y controles de calidad en Silver, y al menos un producto de datos o agregado útil en Gold.
3. Configuración y orden de ejecución del Pipeline, con evidencia de una ejecución completa y sus resultados.
4. Explicación del modelado de las tablas, de las reglas de transformación y de las decisiones técnicas.
5. Consultas o visualizaciones que permitan verificar el resultado final.

### Opción B — Experimentación y comparación de modelos con MLflow

Implementar un flujo de experimentación con **MLflow** para comparar modelos de aprendizaje automático sobre un conjunto de datos elegido y justificado.

La entrega debe mostrar:

1. Descripción del problema, del conjunto de datos y de la variable objetivo; análisis exploratorio y preparación de datos.
2. Entrenamiento de **al menos dos modelos o configuraciones comparables**, con una estrategia de partición y evaluación consistente.
3. Registro en MLflow de parámetros, métricas y artefactos relevantes de cada ejecución.
4. Comparación de los resultados, selección justificada de la mejor alternativa y discusión de limitaciones.
5. Código o notebooks y pasos necesarios para reproducir los experimentos.

## Entrega común a todas las variantes

Toda la documentación y todo el código desarrollado deben quedar en el **repositorio Git utilizado durante la cursada**. Crear allí una carpeta `tp-final/` que contenga un `README.md` con el nombre del estudiante, la variante elegida (y la opción, si corresponde), el objetivo, el contenido de la carpeta y las instrucciones para revisar o reproducir el trabajo.

Incluir en esa carpeta los documentos, diagramas, notebooks, scripts y archivos de configuración necesarios según la opción elegida. Cuando corresponda, agregar evidencia legible de ejecución y resultados; se aceptan capturas o exportaciones si el entorno no permite versionar las salidas. Indicar las fuentes de datos y referencias externas utilizadas. No incluir credenciales ni datos sensibles.

**Fecha máxima de entrega: 31 de octubre de 2026.**
