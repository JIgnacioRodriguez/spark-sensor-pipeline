package com.sensores.main.transformations

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.functions._
import com.sensores.main.util.SharedSparkSession
import java.sql.Timestamp

class GoldAggregationSpec extends AnyFlatSpec with SharedSparkSession with Matchers {

  import spark.implicits._

  "La agregación Gold" should "calcular correctamente la media y el conteo por ventana" in {
    // 1. Datos simulados (Silver Layer output)
    val inputData = Seq(
      ("sensor_1", 30.0, Timestamp.valueOf("2026-06-09 10:00:00")),
      ("sensor_1", 40.0, Timestamp.valueOf("2026-06-09 10:00:30")),
      ("sensor_2", 20.0, Timestamp.valueOf("2026-06-09 10:00:15"))
    ).toDF("sensor", "temperatura", "timestamp")

    // 2. Ejecución de la lógica Gold (Agregación)
    val goldDF = inputData
      .withWatermark("timestamp", "1 minute")
      .groupBy(
        window(col("timestamp"), "1 minute"),
        col("sensor")
      )
      .agg(
        avg("temperatura").as("temp_media"),
        max("temperatura").as("temp_max"),
        count("*").as("total_lecturas")
      )

    // 3. Verificación
    val result = goldDF.collect()

    // Deberíamos tener 2 grupos (sensor_1 y sensor_2)
    result.length shouldBe 2

    // Validar sensor_1: (30+40)/2 = 35.0
    val sensor1 = result.find(_.getAs[String]("sensor") == "sensor_1").get
    sensor1.getAs[Double]("temp_media") shouldBe 35.0
    sensor1.getAs[Long]("total_lecturas") shouldBe 2

    // Validar sensor_2: 20.0
    val sensor2 = result.find(_.getAs[String]("sensor") == "sensor_2").get
    sensor2.getAs[Double]("temp_media") shouldBe 20.0
  }
}