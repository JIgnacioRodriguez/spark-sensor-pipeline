package com.sensores.main.util

import org.apache.spark.sql.SparkSession

object SparkProvider {
  def getSession(appName: String): SparkSession = {
    val master = sys.env.getOrElse("SPARK_MASTER", "local[*]")

    SparkSession.builder()
      .appName(appName)
      .master(master)
      // Activa las extensiones de Delta Lake para usar comandos SQL específicos de Delta
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      // Define el catálogo de Delta como el catálogo por defecto para las operaciones de tablas
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      // Optimizaciones de paralelismo
      // Define en cuántas particiones se dividen los datos tras un 'shuffle' (join, group by, etc.)
      .config("spark.sql.shuffle.partitions", "4") // El número de "carriles" de la carretera
      // Define el número predeterminado de particiones para operaciones RDD (sin usar DataFrames)
      .config("spark.default.parallelism", "4")
      // Memoria y GC
      // Fracción de la memoria del heap usada para ejecución y almacenamiento (0.6 es el valor por defecto)
      .config("spark.memory.fraction", "0.6") // 60% para ejecuciones (transformaciones), 40% para almacenamiento
      // Configuración de la JVM para el Driver:
      // -XX:+UseG1GC: Usa el recolector de basura G1 (optimizado para grandes heaps y baja latencia)
      // -XX:+IgnoreUnrecognizedVMOptions: Evita errores si alguna opción de la JVM no es soportada por la versión actual
      // --add-modules=jdk.unsupported: Permite acceso a APIs internas de la JDK necesarias para el funcionamiento óptimo de Spark/Netty
      .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+IgnoreUnrecognizedVMOptions --add-modules=jdk.unsupported")
      .getOrCreate()
  }
}