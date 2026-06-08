package com.sensores.main

import com.sensores.main.transformations.{FormatColumnTransformer, HighTemperatureFilter, Transformer}
import com.sensores.main.util.SparkProvider
import com.typesafe.config.ConfigFactory
import io.delta.tables.DeltaTable
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory


object SilverTransformApp{
  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(getClass)
    val config = ConfigFactory.load()
    val kafkaServers = config.getString("kafka.bootstrap.servers")
    val silverCheckpointDir = config.getString("spark.silver.checkpoint.location")
    val bronze_path = config.getString("spark.bronze.path")
    val silver_path = config.getString("spark.silver.path")
    val hadoopHomeDir = config.getString("hadoop.home.dir")

    // Asegúrate de que esta ruta coincida con donde pusiste el bin\winutils.exe
    System.setProperty("hadoop.home.dir", hadoopHomeDir)

    val spark = SparkProvider.getSession(config.getString("spark.app.name"))

    val bronzeDF = spark.readStream.format("delta").load(bronze_path)

    // Aquí defines tu Pipeline de forma declarativa
    val transformations: Seq[Transformer] = Seq(
      new HighTemperatureFilter(30.0),
      new FormatColumnTransformer("TEMPERATURA")
    )

    // Aplicación del pipeline profesional
    val processedDFResult: Either[Throwable, DataFrame] = transformations.foldLeft[Either[Throwable, DataFrame]](Right(bronzeDF)) { (accumulator, transformer) =>
      accumulator.flatMap(df => transformer.transform(df))
    }

    // Ahora extraemos el resultado final
    val finalDF = processedDFResult.getOrElse(throw new RuntimeException("Error en transformaciones"))

    val query = finalDF
      .writeStream
      .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("10 seconds")) // Agrupa datos cada 10s
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>

        // Si el batch está vacío, no hagas nada
        if (!batchDF.isEmpty) {
          batchDF.persist()

          val exists = io.delta.tables.DeltaTable.isDeltaTable(spark, silver_path)

          if (!exists) {
            batchDF.write.format("delta").mode("overwrite").save(silver_path)
          } else {
            val targetTable = DeltaTable.forPath(spark, silver_path)
            targetTable.as("oldData")
              .merge(batchDF.as("newData"), "oldData.id = newData.id")
              .whenMatched().updateAll()
              .whenNotMatched().insertAll()
              .execute()
          }
          batchDF.unpersist()
        }
        ()
      }
      .option("checkpointLocation", silverCheckpointDir)
      .start()

    query.awaitTermination()
  }
}