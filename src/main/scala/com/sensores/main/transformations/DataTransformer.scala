package com.sensores.main.transformations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import scala.util.{Try, Success, Failure}

trait Transformer {
  def transform(df: DataFrame): Either[Throwable, DataFrame]
}

// Implementación con parámetros inyectados en el constructor
class HighTemperatureFilter(threshold: Double) extends Transformer {
  override def transform(df: DataFrame): Either[Throwable, DataFrame] = {
    // Usar 'col' es una buena práctica frente a df("nombre")
    Try(df.filter(col("temperatura") > threshold)).toEither
  }
}

// Ejemplo de transformación de columnas
class FormatColumnTransformer(targetColumn: String) extends Transformer {
  override def transform(df: DataFrame): Either[Throwable, DataFrame] = {
    Try(df.withColumnRenamed(targetColumn, targetColumn.toLowerCase)).toEither
  }
}