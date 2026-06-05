package com.sensores.main.transformations

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.functions.col
import com.sensores.main.util.SharedSparkSession

class HighTemperatureFilterSpec extends AnyFlatSpec with SharedSparkSession with Matchers {

  import spark.implicits._

  "El filtro de temperatura" should "eliminar registros con temperatura menor a 30" in {
    // 1. Datos de entrada (Input Mock)
    val inputData = Seq((1, 20.0), (2, 35.0), (3, 25.0), (4, 40.0)).toDF("id", "temperatura")

    // 2. Aplicamos tu lógica (aquí llamarías a tu clase Transformer)
    val filteredData = inputData.filter(col("temperatura") >= 30.0)

    // 3. Verificamos el resultado
    val result = filteredData.collect()

    result.length shouldBe 2 // Solo deberían quedar los de 35 y 40
    result.map(_.getAs[Double]("temperatura")) should contain allOf (35.0, 40.0)
  }
}