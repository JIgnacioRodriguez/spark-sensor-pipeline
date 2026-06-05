package com.sensores.main

import com.sensores.main.util.SharedSparkSession
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BronzeIngestSpec extends AnyFlatSpec with SharedSparkSession with Matchers {

  "El proceso de ingesta Bronze" should "mapear correctamente el JSON a un esquema definido" in {
    import spark.implicits._

    // 1. Datos crudos que recibimos de Kafka
    val rawData = Seq(
      """{"id": 1, "temperatura": 22.5, "sensor_id": "A1"}""",
      """{"id": 2, "temperatura": 28.0, "sensor_id": "B2"}"""
    ).toDF("value")

    // 2. Aquí llamarías a tu función que aplica el esquema (ej. transformBronze)
    // Supongamos que tu lógica convierte el JSON a un DF con columnas estructuradas
    val schema = StructType(Array(
      StructField("id", IntegerType, true),
      StructField("temperatura", DoubleType, true),
      StructField("sensor_id", StringType, true)
    ))

    // Simulamos la conversión que hace tu App
    import org.apache.spark.sql.functions.from_json
    val result = rawData.select(from_json($"value", schema).as("data")).select("data.*")

    // 3. Verificamos que el esquema es el correcto
    result.schema.fields.map(_.name) should contain allOf ("id", "temperatura", "sensor_id")
    result.count() shouldBe 2
    result.filter($"id" === 1).select("sensor_id").as[String].first() shouldBe "A1"
  }
}