package com.sensores.main.transformations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

trait Transformer {
  def transform(df: DataFrame): DataFrame
}

// Implementación con parámetros inyectados en el constructor
class HighTemperatureFilter(threshold: Double) extends Transformer {
  override def transform(df: DataFrame): DataFrame = {
    // Usar 'col' es una buena práctica frente a df("nombre")
    df.filter(col("temperatura") > threshold)
  }
}

// Ejemplo de transformación de columnas
class FormatColumnTransformer(targetColumn: String) extends Transformer {
  override def transform(df: DataFrame): DataFrame = {
    df.withColumnRenamed(targetColumn, targetColumn.toLowerCase)
  }
}