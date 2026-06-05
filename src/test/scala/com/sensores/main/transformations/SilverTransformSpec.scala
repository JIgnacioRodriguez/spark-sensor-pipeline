package com.sensores.main.transformations

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.functions.col
import com.sensores.main.util.SharedSparkSession

class SilverTransformSpec extends AnyFlatSpec with SharedSparkSession with Matchers {

  import spark.implicits._

  "La transformación Silver" should "aplicar correctamente las reglas de limpieza y filtrado" in {
    // 1. Datos simulados (Bronze Layer output)
    val inputData = Seq(
      (1, 22.0, "OK"),
      (2, -5.0, "SENSOR_ERROR"), // Registro mal formado
      (3, 45.0, "OK")            // Registro que debería pasar
    ).toDF("id", "temperatura", "estado")

    // 2. Ejecución de la lógica de transformación
    // (Aquí llamarías a tu método de la clase SilverTransform)
    val transformedData = inputData
      .filter(col("estado") === "OK")      // Regla: Solo estados OK
      .filter(col("temperatura") > 0)      // Regla: Solo temperaturas válidas

    // 3. Verificación de resultados
    val result = transformedData.collect()

    result.length shouldBe 2
    result.map(_.getAs[Int]("id")) should contain allOf (1, 3)
    result.map(_.getAs[Double]("temperatura")) should not contain -5.0
  }
}