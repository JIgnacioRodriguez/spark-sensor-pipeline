# Sensor Data Pipeline

Proyecto de ingeniería de datos diseñado para la ingesta, limpieza y transformación de datos de sensores. Utiliza **Apache Spark (Scala)** bajo una **arquitectura Medallón** (Bronze/Silver).

## Estructura del Proyecto
* **Bronze**: Ingesta de datos crudos (JSON/Kafka).
* **Silver**: Procesamiento, filtrado y validación de datos.
* **Tests**: Suite de pruebas unitarias implementada con **ScalaTest** para garantizar la calidad de la lógica de negocio.

## Tecnologías
- **Lenguaje**: Scala 3
- **Motor**: Apache Spark 3.5.x
- **Runtime**: Java 17
- **Testing**: ScalaTest